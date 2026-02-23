#!/usr/bin/env python3
"""
Nexudus Invoices → ETL Excel Copier → S3 Versioned Upload
---------------------------------------------------------
Opens template, adds Nexudus data to Membership Invoices sheet, saves and uploads.
Simple approach: minimal changes to preserve all Excel features.
"""

import os
import sys
from datetime import datetime, timedelta
import requests
from io import BytesIO
import json
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
import boto3
from botocore.exceptions import ClientError

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
TEMPLATE_FILE = "template/ETL_MarioDemo.xlsx"
OUTPUT_DIR = "output"
DEST_SHEET_NAME = "Membership invoices"
DATA_START_ROW = 1  # Data starts at row 1

NEXUDUS_REPORT_URL = "https://reports.nexudus.com/ReportCenter/Invoices"
NEXUDUS_TOKEN_URL = "https://spaces.nexudus.com/api/token"

# S3
S3_BUCKET = os.getenv("S3_BUCKET", "level39-etl-mario")
S3_KEY_PREFIX = "output/"

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --------------------------------------------------
# Generate timestamped filename
# --------------------------------------------------
def generate_output_filename():
    now = datetime.utcnow()
    timestamp = now.strftime("%Y-%m-%d_%H%M%S")
    filename = f"ETL_MarioDemo_{timestamp}.xlsx"
    
    return {
        "local": os.path.join(OUTPUT_DIR, filename),
        "s3": f"{S3_KEY_PREFIX}{filename}",
        "displayName": filename
    }

# --------------------------------------------------
# Calculate yesterday's date range
# --------------------------------------------------
def get_date_range():
    yesterday = datetime.utcnow() - timedelta(days=1)
    
    start_date = datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0)
    end_date = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)
    
    return {
        "start": start_date.isoformat(),
        "end": end_date.isoformat()
    }

# --------------------------------------------------
# Get Nexudus Access Token
# --------------------------------------------------
def get_nexudus_token():
    try:
        print("🔑 Requesting Nexudus authentication token...")
        
        response = requests.post(
            NEXUDUS_TOKEN_URL,
            data={
                "grant_type": "password",
                "username": os.getenv("NEXUDUS_USERNAME"),
                "password": os.getenv("NEXUDUS_PASSWORD")
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json"
            },
            timeout=10
        )
        
        response.raise_for_status()
        
        data = response.json()
        if "access_token" not in data:
            raise Exception("No access token received from Nexudus")
        
        print("✔ Nexudus token obtained successfully")
        return data["access_token"]
        
    except requests.exceptions.RequestException as err:
        if "401" in str(err):
            raise Exception("Nexudus authentication failed - invalid credentials")
        elif "timeout" in str(err).lower():
            raise Exception("Cannot reach Nexudus API - network issue or service unavailable")
        raise Exception(f"Nexudus token request failed: {str(err)}")

# --------------------------------------------------
# Download Nexudus report
# --------------------------------------------------
def download_report(token):
    try:
        date_range = get_date_range()
        start = date_range["start"].split(".")[0]
        end = date_range["end"].split(".")[0]
        
        print(f"📅 Fetching invoices from {start} to {end}")
        
        response = requests.get(
            NEXUDUS_REPORT_URL,
            params={
                "businessId": os.getenv("BUSINESS_ID"),
                "reportName": "Invoices/InvoicesAccount",
                "start": start,
                "end": end,
                "format": "Excel",
                "portrait": "false",
                "rnd": datetime.utcnow().isoformat()
            },
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            },
            timeout=30
        )
        
        response.raise_for_status()
        
        if len(response.content) == 0:
            raise Exception("Nexudus report returned empty data")
        
        print(f"✔ Report downloaded successfully ({len(response.content)} bytes)")
        return BytesIO(response.content)
        
    except requests.exceptions.RequestException as err:
        if "404" in str(err):
            raise Exception("Nexudus report endpoint not found - check API URL")
        elif "403" in str(err):
            raise Exception("Access denied to Nexudus report - check business ID and permissions")
        elif "timeout" in str(err).lower():
            raise Exception("Nexudus report request timed out")
        raise Exception(f"Failed to download Nexudus report: {str(err)}")

# --------------------------------------------------
# Update template with data
# --------------------------------------------------
def update_template_with_data(report_buffer, output_file):
    try:
        print("📝 Processing Excel data...")
        
        # Load the Nexudus report
        print("  ├─ Loading Nexudus report...")
        report_wb = load_workbook(report_buffer)
        
        if not report_wb.sheetnames:
            raise Exception("Nexudus workbook contains no sheets")
        
        print(f"  ├─ Available sheets: {', '.join(report_wb.sheetnames)}")
        
        # Find sheet with most data
        source_sheet_name = report_wb.sheetnames[0]
        max_cells = 0
        
        for sheet_name in report_wb.sheetnames:
            sheet = report_wb[sheet_name]
            cell_count = sum(1 for row in sheet.iter_rows() for cell in row if cell.value is not None)
            print(f"  ├─ '{sheet_name}': {cell_count} cells")
            
            if cell_count > max_cells:
                max_cells = cell_count
                source_sheet_name = sheet_name
        
        print(f"  ├─ Using sheet: '{source_sheet_name}' ({max_cells} cells)")
        
        source_sheet = report_wb[source_sheet_name]
        
        # Load template
        print("  ├─ Loading template...")
        if not os.path.exists(TEMPLATE_FILE):
            raise Exception(f"Template file not found: {TEMPLATE_FILE}")
        
        template_wb = load_workbook(TEMPLATE_FILE)
        print(f"  ├─ Template sheets: {', '.join(template_wb.sheetnames)}")
        
        if DEST_SHEET_NAME not in template_wb.sheetnames:
            raise Exception(f"Sheet '{DEST_SHEET_NAME}' not found in template")
        
        dest_sheet = template_wb[DEST_SHEET_NAME]
        print(f"  ├─ Found destination sheet: '{DEST_SHEET_NAME}'")
        
        # Copy data from source to destination starting at row 6
        print(f"  ├─ Copying data starting at row {DATA_START_ROW}...")
        
        copied_rows = 0
        copied_cells = 0
        
        # Iterate through source sheet
        for src_row_idx, src_row in enumerate(source_sheet.iter_rows()):
            dest_row_idx = DATA_START_ROW + src_row_idx
            
            for src_col_idx, src_cell in enumerate(src_row, start=1):
                if src_cell.value is not None:
                    dest_cell = dest_sheet.cell(row=dest_row_idx, column=src_col_idx)
                    
                    # Copy value
                    dest_cell.value = src_cell.value
                    
                    # Copy data type if available
                    if hasattr(src_cell, 'data_type'):
                        dest_cell.data_type = src_cell.data_type
                    
                    # Copy number format if available
                    if src_cell.number_format:
                        dest_cell.number_format = src_cell.number_format
                    
                    copied_cells += 1
            
            copied_rows += 1
        
        print(f"  ├─ Copied {copied_rows} rows ({copied_cells} cells)")
        print(f"  └─ Saving template...")
        
        # Save to output file
        template_wb.save(output_file)
        print(f"✔ Local ETL file saved: {output_file}")
        
    except Exception as err:
        raise Exception(f"Failed to process Excel template: {str(err)}")

# --------------------------------------------------
# Upload to S3
# --------------------------------------------------
def upload_to_s3(local_file, s3_key, display_name):
    try:
        print("☁️  Uploading to S3...")
        
        s3_client = boto3.client(
            "s3",
            region_name=os.getenv("AWS_REGION", "us-east-1"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
        
        with open(local_file, "rb") as f:
            response = s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=f.read()
            )
        
        print(f"✔ File uploaded to S3: s3://{S3_BUCKET}/{s3_key}")
        
        if "VersionId" in response:
            print(f"📦 Version ID: {response['VersionId']}")
        else:
            print("⚠️  Note: S3 versioning may not be enabled on this bucket")
            
    except ClientError as err:
        if err.response['Error']['Code'] == 'NoSuchBucket':
            raise Exception(f"S3 bucket not found: {S3_BUCKET}")
        elif err.response['Error']['Code'] == 'AccessDenied':
            raise Exception("Access denied to S3 bucket - check AWS credentials and permissions")
        raise Exception(f"Failed to upload to S3: {str(err)}")

# --------------------------------------------------
# Main ETL Job
# --------------------------------------------------
def run_etl():
    try:
        print("=" * 50)
        print("🚀 Starting Nexudus ETL Job")
        print("=" * 50)
        print(f"⏰ Run time: {datetime.utcnow().isoformat()}")
        print()
        
        file_names = generate_output_filename()
        print(f"📁 Output file: {file_names['displayName']}")
        print()
        
        # Validate environment variables
        required_vars = [
            "NEXUDUS_USERNAME",
            "NEXUDUS_PASSWORD",
            "BUSINESS_ID",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY"
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise Exception(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        # Execute ETL pipeline
        token = get_nexudus_token()
        report_buffer = download_report(token)
        update_template_with_data(report_buffer, file_names["local"])
        upload_to_s3(file_names["local"], file_names["s3"], file_names["displayName"])
        
        print()
        print("=" * 50)
        print("✅ ETL Job Completed Successfully")
        print("=" * 50)
        
    except Exception as err:
        print()
        print("=" * 50)
        print("❌ ETL Job Failed")
        print("=" * 50)
        print(f"⏰ Error time: {datetime.utcnow().isoformat()}")
        print(f"📌 Error: {str(err)}")
        print("=" * 50)
        
        if os.getenv("DEBUG") == "true":
            import traceback
            print("\nFull stack trace:")
            traceback.print_exc()
        
        sys.exit(1)

# --------------------------------------------------
# Run ETL
# --------------------------------------------------
if __name__ == "__main__":
    run_etl()
