import csv
import os
import re
import time
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Optional spatial index output
try:
    import geopandas as gpd
    from shapely.geometry import shape
    HAS_GEO = True
except Exception:
    HAS_GEO = False

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------

FEATURE_QUERY_URL = (
    "https://services1.arcgis.com/wfNKYeHsOyaFyPw3/ArcGIS/rest/services/"
    "AUSTopo_MapSeriesIndex250k_FEB26/FeatureServer/2/query"
)

OUTPUT_ROOT = Path("output")
DOWNLOAD_ROOT = OUTPUT_ROOT / "pdfs"
MANIFEST_CSV = OUTPUT_ROOT / "download_manifest.csv"
INDEX_GPKG = OUTPUT_ROOT / "austopo_index.gpkg"
INDEX_SHP = OUTPUT_ROOT / "austopo_index.shp"

PAGE_SIZE = 100
TIMEOUT = 60
MAX_WORKERS = 12
SLEEP_BETWEEN_PAGES = 0.25
OVERWRITE_EXISTING = False
WRITE_SHP_TOO = False  # True if you also want shapefile
BUILD_SPATIAL_INDEX = True  # requires geopandas + shapely

CLOUDFRONT_BASE = "https://d28rz98at9flks.cloudfront.net"

OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)

manifest_lock = threading.Lock()

# ---------------------------------------------------------------------
# HTTP SESSION WITH RETRIES
# ---------------------------------------------------------------------

def build_session():
    session = requests.Session()

    retry = Retry(
        total=8,
        connect=8,
        read=8,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=MAX_WORKERS * 2,
        pool_maxsize=MAX_WORKERS * 2,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        )
    })
    return session


session = build_session()

# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------

def extract_metadata_id(source_url):
    if not source_url:
        return None

    source_url = source_url.strip()

    patterns = [
        r"/metadata/(\d+)",
        r"/dataset/ga/(\d+)",
        r"/(\d+)$",
    ]

    for pattern in patterns:
        match = re.search(pattern, source_url, re.IGNORECASE)
        if match:
            return match.group(1)

    return None


def build_pdf_url(metadata_id):
    return f"{CLOUDFRONT_BASE}/{metadata_id}/{metadata_id}_00_Feb26.pdf"


def clean_filename(text):
    if not text:
        return "UNKNOWN"

    text = str(text).strip()
    text = re.sub(r'[<>:"/\\|?*]', "", text)
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"_+", "_", text)
    text = text.strip("._")
    return text or "UNKNOWN"


def parse_tilenumber_groups(tilenumber):
    """
    Example:
      SF52-16 -> ('SF', 'SF52')
      SI55-01 -> ('SI', 'SI55')
    """
    if not tilenumber:
        return ("UNKNOWN", "UNKNOWN")

    tilenumber = str(tilenumber).strip().upper()
    major = tilenumber.split("-")[0] if "-" in tilenumber else tilenumber

    m = re.match(r"^([A-Z]{2})", major)
    state_group = m.group(1) if m else "UNKNOWN"

    return state_group, major


def build_output_path(tilenumber, tilename):
    state_group, grid_group = parse_tilenumber_groups(tilenumber)
    safe_tile = clean_filename(tilenumber)
    safe_name = clean_filename(tilename)
    folder = DOWNLOAD_ROOT / state_group / grid_group
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"{safe_tile}_{safe_name}.pdf"


def load_existing_manifest(manifest_csv):
    """
    Returns dict keyed by OBJECTID as string.
    """
    existing = {}
    if not manifest_csv.exists():
        return existing

    with open(manifest_csv, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing[str(row.get("OBJECTID", ""))] = row
    return existing


def init_manifest(manifest_csv):
    if manifest_csv.exists():
        return

    with open(manifest_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "OBJECTID",
            "TILENAME",
            "TILENUMBER",
            "SOURCE_URL",
            "METADATA_ID",
            "PDF_URL",
            "LOCAL_PATH",
            "STATUS",
            "FILE_SIZE",
        ])


def append_manifest_row(manifest_csv, row):
    with manifest_lock:
        with open(manifest_csv, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(row)


def url_exists(url):
    try:
        response = session.head(url, allow_redirects=True, timeout=TIMEOUT)
        if response.status_code == 200:
            return True
        if response.status_code in (403, 405):
            response = session.get(url, stream=True, allow_redirects=True, timeout=TIMEOUT)
            return response.status_code == 200
        return False
    except requests.RequestException:
        return False


# ---------------------------------------------------------------------
# ARCGIS FEATURE FETCH
# ---------------------------------------------------------------------

def get_page(offset, page_size, include_geometry=False):
    params = {
        "where": "1=1",
        "outFields": "OBJECTID,TILENAME,TILENUMBER,eCAT_URL",
        "returnGeometry": "true" if include_geometry else "false",
        "orderByFields": "OBJECTID ASC",
        "resultOffset": offset,
        "resultRecordCount": page_size,
        "f": "geojson" if include_geometry and HAS_GEO else "json",
        "outSR": 4326,
    }

    response = session.get(FEATURE_QUERY_URL, params=params, timeout=TIMEOUT)

    if response.status_code != 200:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text[:500]}")

    if include_geometry and HAS_GEO:
        return response.json()

    data = response.json()
    if "error" in data:
        raise RuntimeError(f"ArcGIS error: {data['error']}")
    return data


def fetch_features(include_geometry=False):
    """
    If include_geometry=False:
        yields ArcGIS JSON features
    If include_geometry=True and geopandas available:
        yields GeoJSON features
    """
    offset = 0

    while True:
        payload = get_page(offset, PAGE_SIZE, include_geometry=include_geometry)

        if include_geometry and HAS_GEO:
            features = payload.get("features", [])
        else:
            features = payload.get("features", [])

        if not features:
            break

        for feature in features:
            yield feature

        if len(features) < PAGE_SIZE:
            break

        offset += len(features)
        print(f"Fetched {offset} records...")
        time.sleep(SLEEP_BETWEEN_PAGES)


# ---------------------------------------------------------------------
# DOWNLOAD WORKER
# ---------------------------------------------------------------------

def process_feature(feature, existing_manifest):
    """
    Download one PDF and return manifest row + record dict for index.
    """
    if "properties" in feature:
        # GeoJSON feature
        props = feature.get("properties", {}) or {}
        geom = feature.get("geometry")
        objectid = props.get("OBJECTID")
        tile_name = props.get("TILENAME")
        tile_number = props.get("TILENUMBER")
        source_url = props.get("eCAT_URL")
    else:
        attrs = feature.get("attributes", {}) or {}
        geom = feature.get("geometry")
        objectid = attrs.get("OBJECTID")
        tile_name = attrs.get("TILENAME")
        tile_number = attrs.get("TILENUMBER")
        source_url = attrs.get("eCAT_URL")

    objectid_str = str(objectid)

    # Resume support via manifest
    previous = existing_manifest.get(objectid_str)
    if previous:
        previous_status = (previous.get("STATUS") or "").lower()
        previous_path = previous.get("LOCAL_PATH") or ""
        if previous_status in {"downloaded", "exists"} and previous_path and Path(previous_path).exists():
            return None, {
                "OBJECTID": objectid,
                "TILENAME": tile_name,
                "TILENUMBER": tile_number,
                "SOURCE_URL": source_url,
                "METADATA_ID": previous.get("METADATA_ID"),
                "PDF_URL": previous.get("PDF_URL"),
                "LOCAL_PATH": previous_path,
                "STATUS": "exists",
                "geometry": shape(geom) if (geom and HAS_GEO) else None,
            }

    metadata_id = extract_metadata_id(source_url)
    if not metadata_id:
        row = [
            objectid, tile_name, tile_number, source_url or "", "", "", "", "no_metadata_id", ""
        ]
        return row, {
            "OBJECTID": objectid,
            "TILENAME": tile_name,
            "TILENUMBER": tile_number,
            "SOURCE_URL": source_url,
            "METADATA_ID": None,
            "PDF_URL": None,
            "LOCAL_PATH": None,
            "STATUS": "no_metadata_id",
            "geometry": shape(geom) if (geom and HAS_GEO) else None,
        }

    pdf_url = build_pdf_url(metadata_id)
    output_path = build_output_path(tile_number, tile_name)

    if output_path.exists() and not OVERWRITE_EXISTING:
        size = output_path.stat().st_size
        row = [
            objectid, tile_name, tile_number, source_url or "", metadata_id,
            pdf_url, str(output_path), "exists", size
        ]
        return row, {
            "OBJECTID": objectid,
            "TILENAME": tile_name,
            "TILENUMBER": tile_number,
            "SOURCE_URL": source_url,
            "METADATA_ID": metadata_id,
            "PDF_URL": pdf_url,
            "LOCAL_PATH": str(output_path),
            "STATUS": "exists",
            "geometry": shape(geom) if (geom and HAS_GEO) else None,
        }

    tmp_path = output_path.with_suffix(".pdf.part")

    try:
        with session.get(pdf_url, stream=True, timeout=TIMEOUT) as r:
            if r.status_code != 200:
                row = [
                    objectid, tile_name, tile_number, source_url or "", metadata_id,
                    pdf_url, str(output_path), f"http_{r.status_code}", ""
                ]
                return row, {
                    "OBJECTID": objectid,
                    "TILENAME": tile_name,
                    "TILENUMBER": tile_number,
                    "SOURCE_URL": source_url,
                    "METADATA_ID": metadata_id,
                    "PDF_URL": pdf_url,
                    "LOCAL_PATH": str(output_path),
                    "STATUS": f"http_{r.status_code}",
                    "geometry": shape(geom) if (geom and HAS_GEO) else None,
                }

            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 128):
                    if chunk:
                        f.write(chunk)

        os.replace(tmp_path, output_path)
        size = output_path.stat().st_size

        row = [
            objectid, tile_name, tile_number, source_url or "", metadata_id,
            pdf_url, str(output_path), "downloaded", size
        ]
        return row, {
            "OBJECTID": objectid,
            "TILENAME": tile_name,
            "TILENUMBER": tile_number,
            "SOURCE_URL": source_url,
            "METADATA_ID": metadata_id,
            "PDF_URL": pdf_url,
            "LOCAL_PATH": str(output_path),
            "STATUS": "downloaded",
            "geometry": shape(geom) if (geom and HAS_GEO) else None,
        }

    except Exception as e:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass

        row = [
            objectid, tile_name, tile_number, source_url or "", metadata_id,
            pdf_url, str(output_path), f"error:{e}", ""
        ]
        return row, {
            "OBJECTID": objectid,
            "TILENAME": tile_name,
            "TILENUMBER": tile_number,
            "SOURCE_URL": source_url,
            "METADATA_ID": metadata_id,
            "PDF_URL": pdf_url,
            "LOCAL_PATH": str(output_path),
            "STATUS": f"error:{e}",
            "geometry": shape(geom) if (geom and HAS_GEO) else None,
        }


# ---------------------------------------------------------------------
# SPATIAL INDEX OUTPUT
# ---------------------------------------------------------------------

def write_spatial_index(records):
    if not BUILD_SPATIAL_INDEX:
        print("Spatial index disabled.")
        return

    if not HAS_GEO:
        print("geopandas/shapely not installed; skipping GeoPackage/shapefile index.")
        return

    valid_records = [r for r in records if r.get("geometry") is not None]
    if not valid_records:
        print("No geometries available; skipping spatial index.")
        return

    gdf = gpd.GeoDataFrame(valid_records, geometry="geometry", crs="EPSG:4326")

    # GeoPackage keeps nicer field names
    gdf.to_file(INDEX_GPKG, layer="austopo_index", driver="GPKG")
    print(f"Wrote GeoPackage: {INDEX_GPKG}")

    if WRITE_SHP_TOO:
        shp = gdf.rename(columns={
            "TILENAME": "TILENAME",
            "TILENUMBER": "TILENUM",
            "SOURCE_URL": "SRC_URL",
            "METADATA_ID": "META_ID",
            "LOCAL_PATH": "LOC_PATH",
        }).copy()

        keep_cols = ["OBJECTID", "TILENAME", "TILENUM", "META_ID", "PDF_URL", "LOC_PATH", "STATUS", "geometry"]
        shp = shp[[c for c in keep_cols if c in shp.columns]]
        shp.to_file(INDEX_SHP, driver="ESRI Shapefile")
        print(f"Wrote Shapefile: {INDEX_SHP}")


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------

def main():
    print("Loading existing manifest...")
    init_manifest(MANIFEST_CSV)
    existing_manifest = load_existing_manifest(MANIFEST_CSV)

    print("Fetching features...")
    include_geometry = BUILD_SPATIAL_INDEX and HAS_GEO
    features = list(fetch_features(include_geometry=include_geometry))
    print(f"Fetched {len(features)} features.")

    manifest_rows_written = 0
    ready_count = 0
    spatial_records = []

    print(f"Starting downloads with {MAX_WORKERS} workers...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(process_feature, feature, existing_manifest)
            for feature in features
        ]

        for i, future in enumerate(as_completed(futures), start=1):
            row, record = future.result()

            if row is not None:
                append_manifest_row(MANIFEST_CSV, row)
                manifest_rows_written += 1

                status = row[7]
                local_path = row[6]
                print(f"[{i}/{len(futures)}] {row[0]} | {Path(local_path).name if local_path else ''} | {status}")

            if record is not None:
                spatial_records.append(record)
                if record["STATUS"] in {"downloaded", "exists"}:
                    ready_count += 1

    print(f"Manifest rows added: {manifest_rows_written}")
    print(f"Files ready: {ready_count}")

    write_spatial_index(spatial_records)

    print("Done.")


if __name__ == "__main__":
    main()
