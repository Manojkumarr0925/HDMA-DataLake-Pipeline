import boto3
import requests
import time
import logging
from io import BytesIO

# ── Configuration ─────────────────────────────────────────
YEARS       = ["2024"]          # 2023 already done — only 2024 needed
STATES      = ["CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MN"]
BASE_URL    = "https://ffiec.cfpb.gov/v2/data-browser-api/view/csv"
BUCKET      = "hmda-datalake-313607939153"
S3_PREFIX   = "raw"
DELAY_SECS  = 3
MAX_RETRIES = 3

# S3 multipart upload requires each part to be at least 5MB
PART_SIZE   = 5 * 1024 * 1024   # 5MB chunks

# ── Logging ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("hmda_download_s3.log")
    ]
)
log = logging.getLogger(__name__)

# ── S3 client ─────────────────────────────────────────────
s3 = boto3.client("s3", region_name="us-east-1")

# ── Check if file already exists in S3 ────────────────────
def already_in_s3(key: str) -> bool:
    try:
        obj = s3.head_object(Bucket=BUCKET, Key=key)
        size = obj["ContentLength"]
        if size > 1_000_000:
            log.info(f"SKIP  {key} — already in S3 ({size:,} bytes)")
            return True
    except s3.exceptions.ClientError:
        pass
    return False

# ── Stream API → S3 multipart upload ──────────────────────
def stream_to_s3(year: str, state: str) -> bool:
    s3_key = f"{S3_PREFIX}/{year}/{state}_{year}.csv"

    if already_in_s3(s3_key):
        return True

    params = {"years": year, "states": state}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"GET   {state} {year}  (attempt {attempt}/{MAX_RETRIES})")

            # Start multipart upload
            mpu = s3.create_multipart_upload(
                Bucket=BUCKET,
                Key=s3_key,
                ContentType="text/csv"
            )
            upload_id = mpu["UploadId"]
            parts = []
            part_number = 1
            buffer = BytesIO()

            r = requests.get(BASE_URL, params=params, timeout=300, stream=True)
            r.raise_for_status()

            for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1MB API chunks
                buffer.write(chunk)

                # When buffer hits 5MB, flush it as one S3 part
                if buffer.tell() >= PART_SIZE:
                    buffer.seek(0)
                    part = s3.upload_part(
                        Bucket=BUCKET,
                        Key=s3_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=buffer.read()
                    )
                    parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                    log.info(f"  ↑ part {part_number} uploaded ({PART_SIZE/1024/1024:.0f}MB)")
                    part_number += 1
                    buffer = BytesIO()  # reset buffer

            # Upload any remaining bytes as the final part
            if buffer.tell() > 0:
                buffer.seek(0)
                part = s3.upload_part(
                    Bucket=BUCKET,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=buffer.read()
                )
                parts.append({"PartNumber": part_number, "ETag": part["ETag"]})

            # Complete the multipart upload
            s3.complete_multipart_upload(
                Bucket=BUCKET,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts}
            )

            log.info(f"DONE  {state} {year} → s3://{BUCKET}/{s3_key}")
            return True

        except Exception as e:
            log.warning(f"FAIL  {state} {year} attempt {attempt}: {e}")
            # Abort the incomplete multipart upload to avoid S3 storage charges
            try:
                s3.abort_multipart_upload(Bucket=BUCKET, Key=s3_key, UploadId=upload_id)
            except:
                pass
            if attempt < MAX_RETRIES:
                time.sleep(DELAY_SECS * attempt)

    log.error(f"GAVE UP on {state} {year} after {MAX_RETRIES} attempts")
    return False

# ── Main ──────────────────────────────────────────────────
def main():
    log.info(f"Starting HMDA S3 stream — {len(YEARS)} year(s) × {len(STATES)} states")
    results = {"success": [], "failed": []}

    for year in YEARS:
        for state in STATES:
            ok = stream_to_s3(year, state)
            key = f"{state}_{year}"
            if ok:
                results["success"].append(key)
            else:
                results["failed"].append(key)
            time.sleep(DELAY_SECS)

    log.info("=" * 50)
    log.info(f"SUCCESS : {len(results['success'])} / {len(YEARS) * len(STATES)} files")
    log.info(f"FAILED  : {len(results['failed'])} files")
    if results["failed"]:
        log.error(f"Failed: {results['failed']}")

if __name__ == "__main__":
    main()