import sys
import os
import io
import json
import boto3
import pandas as pd
import numpy as np
import base64
import time
from urllib.request import urlopen, Request
from urllib.parse import urlencode, quote

# ========================================================================
# CONFIGURATION
# ========================================================================
SERVICE_ACCOUNT_FILE = os.getenv("GDRIVE_SA_JSON", "service_account.json")
GOOGLE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")  
SILVER_BUCKET = os.getenv("SILVER_BUCKET_NAME")  
SILVER_PREFIX = os.getenv("SILVER_PREFIX", "datasets/")

s3 = boto3.client("s3")

# Expected file mappings (optional – safe to keep, no secrets here)
EXPECTED_FILES = {
    'fy_2024_snf_vbp_aggregate_performance.csv': 'fy_2024_snf_vbp_aggregate_performance_parquet',
    'fy_2024_snf_vbp_facility_performance.csv': 'fy_2024_snf_vbp_facility_performance_parquet',
    'nh_citationdescriptions_oct2024.csv': 'nh_citationdescriptions_oct2024_parquet',
    'nh_firesafetycitations_oct2024.csv': 'nh_firesafetycitations_oct2024_parquet',
    'nh_healthcitations_oct2024.csv': 'nh_healthcitations_oct2024_parquet',
    'nh_hlthinspeccutpointsstate_oct2024.csv': 'nh_hlthinspeccutpointsstate_oct2024_parquet',
    'nh_covidvaxaverages_20241027.csv': 'nh_covidvaxaverages_20241027_parquet',
    'nh_covidvaxprovider_20241027.csv': 'nh_covidvaxprovider_20241027_parquet',
    'nh_datacollectionintervals_oct2024.csv': 'nh_datacollectionintervals_oct2024_parquet',
    'nh_surveydates_oct2024.csv': 'nh_surveydates_oct2024_parquet',
    'nh_surveysummary_oct2024.csv': 'nh_surveysummary_oct2024_parquet',
    'nh_ownership_oct2024.csv': 'nh_ownership_oct2024_parquet',
    'nh_penalties_oct2024.csv': 'nh_penalties_oct2024_parquet',
    'nh_providerinfo_oct2024.csv': 'nh_providerinfo_oct2024_parquet',
    'nh_qualitymsr_claims_oct2024.csv': 'nh_qualitymsr_claims_oct2024_parquet',
    'nh_qualitymsr_mds_oct2024.csv': 'nh_qualitymsr_mds_oct2024_parquet',
    'nh_stateusaverages_oct2024.csv': 'nh_stateusaverages_oct2024_parquet',
    'skilled_nursing_facility_quality_reporting_program_national_data_oct2024.csv': 'skilled_nursing_facility_quality_reporting_program_national_data_oct2024_parquet',
    'skilled_nursing_facility_quality_reporting_program_provider_data_oct2024.csv': 'skilled_nursing_facility_quality_reporting_program_provider_data_oct2024_parquet',
    'swing_bed_snf_data_oct2024.csv': 'swing_bed_snf_data_oct2024_parquet'
}

# ========================================================================
# GOOGLE AUTHENTICATION
# ========================================================================
def sign_jwt_with_openssl(private_key_pem: str, message: str) -> str:
    """Sign JWT using openssl (works in Glue, no extra libs)."""
    import subprocess
    import tempfile
    import os
    import base64

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.pem') as key_file:
        key_file.write(private_key_pem)
        key_path = key_file.name

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as msg_file:
        msg_file.write(message)
        msg_path = msg_file.name

    try:
        result = subprocess.run(
            ['openssl', 'dgst', '-sha256', '-sign', key_path, msg_path],
            capture_output=True,
            check=True
        )
        signature = base64.urlsafe_b64encode(result.stdout).decode().rstrip('=')
        return signature
    finally:
        os.unlink(key_path)
        os.unlink(msg_path)


def get_access_token():
    """Get Google OAuth2 access token using service account credentials."""
    print("🔐 Reading service account credentials...")
    with open(SERVICE_ACCOUNT_FILE, 'r') as f:
        creds = json.load(f)

    header = {"alg": "RS256", "typ": "JWT"}
    header_b64 = base64.urlsafe_b64encode(
        json.dumps(header).encode()
    ).decode().rstrip('=')

    now = int(time.time())
    claim = {
        "iss": creds["client_email"],
        "scope": "https://www.googleapis.com/auth/drive.readonly",
        "aud": "https://oauth2.googleapis.com/token",
        "exp": now + 3600,
        "iat": now
    }
    claim_b64 = base64.urlsafe_b64encode(
        json.dumps(claim).encode()
    ).decode().rstrip('=')

    message = f"{header_b64}.{claim_b64}"
    signature = sign_jwt_with_openssl(creds["private_key"], message)
    jwt = f"{message}.{signature}"

    data = urlencode({
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": jwt
    }).encode()

    req = Request(
        "https://oauth2.googleapis.com/token",
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"}
    )

    response = urlopen(req)
    token_data = json.loads(response.read())
    print("✅ Google authentication successful")
    return token_data["access_token"]


# ========================================================================
# GOOGLE DRIVE HELPERS
# ========================================================================
def list_csv_files(folder_id, access_token):
    query = f"'{folder_id}' in parents and mimeType='text/csv' and trashed=false"
    url = f"https://www.googleapis.com/drive/v3/files?q={quote(query)}&fields=files(id,name,size)&pageSize=1000"
    req = Request(url, headers={"Authorization": f"Bearer {access_token}"})
    response = urlopen(req)
    data = json.loads(response.read())
    return data.get('files', [])


def download_csv(file_id, access_token, chunk_size=10*1024*1024):
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
    req = Request(url, headers={"Authorization": f"Bearer {access_token}"})
    response = urlopen(req)

    chunks = []
    while True:
        chunk = response.read(chunk_size)
        if not chunk:
            break
        chunks.append(chunk)

    return b"".join(chunks).decode("utf-8")


# ========================================================================
# DATA QUALITY + S3 HELPERS
# ========================================================================
def clean_dataframe(df: pd.DataFrame, file_name: str) -> pd.DataFrame:
    initial_rows = len(df)
    print(f"\n========== CLEANING: {file_name} ({initial_rows:,} rows) ==========")

    # Missing values
    missing = df.isnull().sum()
    if missing.any():
        for col in df.columns:
            if df[col].isnull().any():
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col].fillna(df[col].median(), inplace=True)
                else:
                    mode_vals = df[col].mode()
                    if len(mode_vals) > 0:
                        df[col].fillna(mode_vals[0], inplace=True)
                    else:
                        df[col].fillna("Unknown", inplace=True)

    # Duplicates
    dupes = df.duplicated().sum()
    if dupes > 0:
        df.drop_duplicates(inplace=True)

    # Outliers (IQR capping)
    num_cols = df.select_dtypes(include=[np.number]).columns
    for col in num_cols:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        df[col] = df[col].clip(lower=lower, upper=upper)

    return df


def load_existing_parquet(key: str) -> pd.DataFrame:
    try:
        obj = s3.get_object(Bucket=SILVER_BUCKET, Key=key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))
    except s3.exceptions.NoSuchKey:
        return pd.DataFrame()
    except Exception as e:
        print(f"⚠️ Could not load existing file: {e}")
        return pd.DataFrame()


def write_to_s3(df: pd.DataFrame, key: str):
    out = io.BytesIO()
    df.to_parquet(out, index=False, compression="snappy")
    s3.put_object(Bucket=SILVER_BUCKET, Key=key, Body=out.getvalue())
    print(f"✅ Written to s3://{SILVER_BUCKET}/{key} ({len(df):,} rows)")


# ========================================================================
# MAIN
# ========================================================================
def main():
    print("\n================= GDRIVE → S3 SILVER ETL =================\n")

    if not GOOGLE_FOLDER_ID or not SILVER_BUCKET:
        print("❌ Missing required env vars GDRIVE_FOLDER_ID or SILVER_BUCKET_NAME")
        sys.exit(1)

    token = get_access_token()
    files = list_csv_files(GOOGLE_FOLDER_ID, token)
    print(f"Found {len(files)} CSV files in folder")

    for idx, f in enumerate(files, 1):
        file_id = f["id"]
        file_name = f["name"]
        file_name_lower = file_name.lower()

        print(f"\n[{idx}/{len(files)}] Processing {file_name} ...")

        csv_text = download_csv(file_id, token)
        df_new = pd.read_csv(io.StringIO(csv_text), low_memory=False)
        df_new = clean_dataframe(df_new, file_name)

        if file_name_lower in EXPECTED_FILES:
            parquet_name = EXPECTED_FILES[file_name_lower]
        else:
            parquet_name = file_name_lower.replace(".csv", "_parquet")

        key = f"{SILVER_PREFIX}{parquet_name}"

        df_old = load_existing_parquet(key)
        if not df_old.empty:
            df_new = df_new[~df_new.apply(tuple, axis=1).isin(df_old.apply(tuple, axis=1))]
            df_final = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df_final = df_new

        df_final.drop_duplicates(inplace=True)
        write_to_s3(df_final, key)

    print("\n================= ETL COMPLETED =================\n")


if __name__ == "__main__":
    main()
