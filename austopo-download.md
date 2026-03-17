# AUSTopo 250K PDF Downloader

Downloads the **AUSTopo 1:250,000 map series PDFs (Feb 2026 edition)** from Geoscience Australia by:

1. querying the ArcGIS FeatureServer index
2. reading the source URL in `eCAT_URL`
3. extracting the metadata ID
4. constructing the final PDF URL
5. downloading the PDF using a clean filename
6. organising files into folders by grid group
7. writing a manifest CSV
8. optionally writing a **GeoPackage**, **Shapefile**, and **GeoJSON** interactive index with file paths

---

## Features

- Multi-threaded downloads for much faster performance
- Resume support using the existing manifest and downloaded files
- Retry / backoff handling for temporary ArcGIS or CloudFront failures
- Clean filenames in the format:

```text
TILENUMBER_TILENAME.pdf
```

Example:

```text
SF52-16_MOUNT_LIEBIG.pdf
```

- Folder grouping by tile family:

```text
output/pdfs/SF/SF52/SF52-16_MOUNT_LIEBIG.pdf
```

- Optional spatial index outputs:
  - GeoPackage (`.gpkg`)
  - Shapefile (`.shp`)
  - GeoJSON (`.geojson`)

---

## Data pattern used

The script does **not** scrape the PDF link from HTML.

Instead it builds the PDF URL directly from the metadata ID.

Supported source URL patterns:

```text
https://ecat.ga.gov.au/geonetwork/srv/eng/catalog.search#/metadata/148187
https://pid.geoscience.gov.au/dataset/ga/148339
```

These both resolve to a metadata ID, for example:

```text
148339
```

The final PDF URL is then constructed as:

```text
https://d28rz98at9flks.cloudfront.net/{ID}/{ID}_00_Feb26.pdf
```

Example:

```text
https://d28rz98at9flks.cloudfront.net/148339/148339_00_Feb26.pdf
```

---

## Output structure

```text
output/
  download_manifest.csv
  austopo_index.gpkg
  austopo_index.geojson
  austopo_index.shp
  pdfs/
    SF/
      SF52/
        SF52-16_MOUNT_LIEBIG.pdf
    SG/
      SG55/
        SG55-01_DARWIN.pdf
```

---

## Requirements

## Core

```bash
pip install requests
```

## Optional spatial outputs

To create the GeoPackage / Shapefile / GeoJSON index:

```bash
pip install geopandas shapely pyogrio fiona
```

Notes:

- `GeoPackage` is the best master spatial output
- `GeoJSON` is ideal for a lightweight interactive web index
- `Shapefile` is optional and mainly for compatibility with older GIS tools

---

## Configuration

Key settings in the script:

```python
OUTPUT_ROOT = Path("output")
DOWNLOAD_ROOT = OUTPUT_ROOT / "pdfs"
MANIFEST_CSV = OUTPUT_ROOT / "download_manifest.csv"
INDEX_GPKG = OUTPUT_ROOT / "austopo_index.gpkg"
INDEX_GEOJSON = OUTPUT_ROOT / "austopo_index.geojson"
INDEX_SHP = OUTPUT_ROOT / "austopo_index.shp"

PAGE_SIZE = 100
TIMEOUT = 60
MAX_WORKERS = 12
OVERWRITE_EXISTING = False
WRITE_SHP_TOO = False
WRITE_GEOJSON = True
BUILD_SPATIAL_INDEX = True
```

### Recommended values

- `MAX_WORKERS = 8 to 16` is usually a good range
- `OVERWRITE_EXISTING = False` is recommended for resume-safe runs
- `WRITE_GEOJSON = True` is recommended if you want a browser-based interactive index
- `WRITE_SHP_TOO = False` unless you specifically need shapefile output

---

## How resume works

Resume support is built into the workflow.

The script checks:

1. the manifest CSV
2. whether the target PDF already exists on disk

If a record is already marked as downloaded or the file already exists, it is skipped.

This means you can safely rerun the script after:

- a network failure
- ArcGIS service downtime
- a machine restart
- an interrupted session

### Important note

The manifest is **append-only** in the current version.

That is simple and reliable, but it means reruns can add new status lines for the same `OBJECTID`.

If you want a stricter single-record-per-tile tracking system, a SQLite manifest is the next upgrade.

---

## Filename rules

Output filename format:

```text
TILENUMBER_TILENAME.pdf
```

Illegal Windows filename characters are removed:

```text
<>:"/\|?*
```

Spaces are converted to underscores.

Examples:

```text
SF52-16_MOUNT_LIEBIG.pdf
SH54-02_ALICE_SPRINGS.pdf
```

---

## Folder grouping rules

For tile number:

```text
SF52-16
```

The script derives:

- state/group folder: `SF`
- grid folder: `SF52`

Result:

```text
output/pdfs/SF/SF52/SF52-16_MOUNT_LIEBIG.pdf
```

This keeps downloads organised and avoids placing all PDFs into one very large folder.

---

## Manifest CSV

The manifest stores download results and file paths.

Columns:

- `OBJECTID`
- `TILENAME`
- `TILENUMBER`
- `SOURCE_URL`
- `METADATA_ID`
- `PDF_URL`
- `LOCAL_PATH`
- `STATUS`
- `FILE_SIZE`

Typical statuses:

- `downloaded`
- `exists`
- `no_metadata_id`
- `http_404`
- `error:<message>`

---

## Spatial index outputs

When spatial output is enabled, the script can build a GIS index containing:

- tile attributes
- source URL
- metadata ID
- final PDF URL
- local downloaded file path
- download status
- geometry

### GeoPackage

Recommended master output:

```text
output/austopo_index.gpkg
```

Layer name:

```text
austopo_index
```

### GeoJSON

Recommended for interactive map viewers:

```text
output/austopo_index.geojson
```

This can be used directly in:

- Leaflet
- MapLibre GL JS
- OpenLayers
- QGIS
- ArcGIS Pro
- geojson.io

### Shapefile

Optional compatibility output:

```text
output/austopo_index.shp
```

Because shapefiles have short field-name limits, some fields may be abbreviated.

---

## Adding GeoJSON export

Yes — GeoJSON can be exported directly from the same GeoDataFrame used to create the GeoPackage.

Add this config variable near the top:

```python
INDEX_GEOJSON = OUTPUT_ROOT / "austopo_index.geojson"
WRITE_GEOJSON = True
```

Then update the spatial index writer like this:

```python
def write_spatial_index(records):
    if not BUILD_SPATIAL_INDEX:
        print("Spatial index disabled.")
        return

    if not HAS_GEO:
        print("geopandas/shapely not installed; skipping GeoPackage/shapefile/GeoJSON index.")
        return

    valid_records = [r for r in records if r.get("geometry") is not None]
    if not valid_records:
        print("No geometries available; skipping spatial index.")
        return

    gdf = gpd.GeoDataFrame(valid_records, geometry="geometry", crs="EPSG:4326")

    # GeoPackage
    gdf.to_file(INDEX_GPKG, layer="austopo_index", driver="GPKG")
    print(f"Wrote GeoPackage: {INDEX_GPKG}")

    # GeoJSON
    if WRITE_GEOJSON:
        gdf.to_file(INDEX_GEOJSON, driver="GeoJSON")
        print(f"Wrote GeoJSON: {INDEX_GEOJSON}")

    # Optional Shapefile
    if WRITE_SHP_TOO:
        shp = gdf.rename(columns={
            "TILENUMBER": "TILENUM",
            "SOURCE_URL": "SRC_URL",
            "METADATA_ID": "META_ID",
            "LOCAL_PATH": "LOC_PATH",
        }).copy()

        keep_cols = ["OBJECTID", "TILENAME", "TILENUM", "META_ID", "PDF_URL", "LOC_PATH", "STATUS", "geometry"]
        shp = shp[[c for c in keep_cols if c in shp.columns]]
        shp.to_file(INDEX_SHP, driver="ESRI Shapefile")
        print(f"Wrote Shapefile: {INDEX_SHP}")
```

### Why GeoJSON is useful

GeoJSON is ideal for an interactive web index because it:

- is a single portable text file
- can be loaded directly in web mapping libraries
- can include the local file path or a web URL field
- is easy to inspect and debug

### Practical tip

GeoJSON can get large. For 517 records this is completely fine.

If you later scale to much larger datasets, keep the GeoPackage as the master and generate GeoJSON only for the subset needed by the web map.

---

## Example interactive web usage

If your GeoJSON contains fields such as:

- `TILENAME`
- `TILENUMBER`
- `PDF_URL`
- `LOCAL_PATH`

you can build a simple interactive map where clicking a tile opens the PDF.

Typical workflow:

1. load `austopo_index.geojson`
2. style polygons by grid
3. show popup with tile name and number
4. include a link to the PDF or local file path

Example popup content:

```text
SF52-16
Mount Liebig
Open PDF
```

---

## Performance notes

### Multi-threading

Downloads are processed concurrently using `ThreadPoolExecutor`.

Recommended range:

- 8 workers: conservative
- 12 workers: good default
- 16 workers: faster, but slightly more load on source services

If you see throttling or repeated failures, reduce `MAX_WORKERS`.

### Page size

ArcGIS query page size is set to:

```python
PAGE_SIZE = 100
```

This is intentionally conservative to reduce failures from temporary `503` errors.

### Retries

HTTP retries are configured for:

- `429`
- `500`
- `502`
- `503`
- `504`

with exponential backoff.

---

## Running the script

```bash
python austopo.py
```

---

## Recommended workflow

1. Run the downloader
2. Confirm PDFs are downloading into `output/pdfs/...`
3. Inspect `download_manifest.csv`
4. Open `austopo_index.gpkg` in QGIS or ArcGIS Pro
5. Use `austopo_index.geojson` for any web-based interactive tile index

---

## Suggested future improvements

- switch manifest from CSV to SQLite
- add checksum validation
- optionally skip files below a minimum byte size
- optionally write an HTML interactive index
- optionally add a field for a web-friendly relative path
- optionally download cover thumbnails if available

---

## Troubleshooting

### `503 The service is unavailable`

This is usually temporary ArcGIS service instability.

Actions:

- rerun the script
- keep `PAGE_SIZE` modest
- keep retries enabled

### `no_metadata_id`

The source URL did not match any supported metadata-ID pattern.

Check the `SOURCE_URL` in the manifest and add another regex pattern if needed.

### `http_404`

The constructed CloudFront URL did not exist.

Check whether the naming convention changed for that tile.

### GeoPackage or GeoJSON not written

Install the optional GIS dependencies:

```bash
pip install geopandas shapely pyogrio fiona
```

---

## Licence / source note

This workflow depends on publicly exposed service metadata and deterministic file naming from Geoscience Australia / associated delivery infrastructure. Confirm internal project requirements before redistributing downloaded material.

