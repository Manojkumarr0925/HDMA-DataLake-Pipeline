import subprocess
import os
import logging

# ── Configuration ─────────────────────────────────────────
YEARS   = ["2023", "2024"]
STATES  = ["CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MN"]
BUCKET  = "hmda-datalake-313607939153"
VOLUME  = "dbfs:/Volumes/workspace/default/hmda_raw"
TMP_DIR = "/tmp/hmda"

# ── Logging ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("s3_to_databricks.log")
    ]
)
log = logging.getLogger(__name__)

def run(cmd: str) -> bool:
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        log.error(f"CMD FAILED: {cmd}\n{result.stderr}")
        return False
    return True

def file_exists_in_volume(volume_path: str) -> bool:
    result = subprocess.run(
        f"databricks fs ls {volume_path}",
        shell=True, capture_output=True, text=True
    )
    return result.returncode == 0

def transfer(year: str, state: str) -> bool:
    filename  = f"{state}_{year}.csv"
    s3_path   = f"s3://{BUCKET}/raw/{year}/{filename}"
    tmp_path  = f"{TMP_DIR}/{filename}"
    vol_path  = f"{VOLUME}/{year}/{filename}"

    # Resume: skip if already in volume
    if file_exists_in_volume(vol_path):
        log.info(f"SKIP  {filename} — already in volume")
        return True

    log.info(f"START {filename}")

    # Step 1: S3 → local temp
    log.info(f"  ↓ S3 → /tmp")
    if not run(f"aws s3 cp {s3_path} {tmp_path}"):
        return False

    # Step 2: local temp → Databricks Volume
    log.info(f"  ↑ /tmp → Databricks Volume")
    if not run(f"databricks fs cp {tmp_path} {vol_path}"):
        os.remove(tmp_path)
        return False

    # Step 3: clean up temp file immediately
    os.remove(tmp_path)
    log.info(f"DONE  {filename} → {vol_path}")
    return True

def main():
    os.makedirs(TMP_DIR, exist_ok=True)
    log.info(f"Starting S3 → Databricks Volume transfer — 20 files")
    results = {"success": [], "failed": []}

    for year in YEARS:
        for state in STATES:
            filename = f"{state}_{year}.csv"
            # Skip CA_2023 — already uploaded manually
            ok = transfer(year, state)
            if ok:
                results["success"].append(filename)
            else:
                results["failed"].append(filename)

    log.info("=" * 50)
    log.info(f"SUCCESS : {len(results['success'])} / 20 files")
    log.info(f"FAILED  : {len(results['failed'])} files")
    if results["failed"]:
        log.error(f"Failed: {results['failed']}")

if __name__ == "__main__":
    main()
