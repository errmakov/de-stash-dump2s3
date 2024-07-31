#!/usr/bin/env python3

import os
import subprocess
import boto3
from datetime import datetime, timedelta
import re
import argparse
import sys

# Save days function
def generate_daily(input_date):
    date_format = "%Y-%m-%d"
    input_date = datetime.strptime(input_date, date_format)
    last_7_days = [(input_date - timedelta(days=i)).strftime(date_format) for i in range(7)]
    return last_7_days

def generate_weekly(input_date):
    date_format = "%Y-%m-%d"
    input_date = datetime.strptime(input_date, date_format) - timedelta(days=1)
    specific_days = [22, 15, 8, 1]
    result_days = []
    dc = 0
    while len(result_days) < 4:
        temp_date = input_date - timedelta(days=dc)
        if temp_date.day in specific_days:
            result_days.append(datetime.strftime(temp_date, date_format))
        dc += 1
    return result_days

def generate_monthly(input_date):
    date_format = "%Y-%m-%d"
    months = []
    input_date = datetime.strptime(input_date, date_format) - timedelta(days=1)
    temp_date = input_date.replace(day=1)
    months.append(temp_date.strftime("%Y-%m-%d"))

    while len(months) < 3:
        previous_month = temp_date.month - 1 if temp_date.month > 1 else 12
        previous_year = temp_date.year if temp_date.month > 1 else temp_date.year - 1
        temp_date = temp_date.replace(month=previous_month, year=previous_year)
        months.append(temp_date.strftime("%Y-%m-%d"))
    return months

def save_days(input_date):
    daily = generate_daily(input_date)
    weekly = generate_weekly(daily[-1])
    monthly = generate_monthly(weekly[-1])
    return daily + weekly + monthly

# Function to list folders in S3
def list_s3_folders(bucket_name, prefix, s3_client):
    folders = set()
    continuation_token = None

    while True:
        if continuation_token:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                ContinuationToken=continuation_token
            )
        else:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
            )

        for content in response.get('Contents', []):
            match = re.match(rf'^{prefix}/(\d{{4}}-\d{{2}}-\d{{2}})/', content['Key'])
            if match:
                folders.add(match.group(1))

        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break

    folders = sorted(folders)
    return folders

# Parse command-line arguments
parser = argparse.ArgumentParser(
    description="Backup MySQL/MariaDB databases to AWS S3 with retention policy.",
    epilog="""
Examples:
  ./backup_db_to_s3.py --bucket my-backup-bucket --profile myprofile
    Basic usage with required bucket name and profile.

  ./backup_db_to_s3.py --bucket my-backup-bucket --profile myprofile --output
    Specify output message on success.

  ./backup_db_to_s3.py --bucket my-backup-bucket --profile myprofile --user myuser
    Set a different MySQL user.

  ./backup_db_to_s3.py --bucket my-backup-bucket --profile myprofile --exclude "information_schema performance_schema mysql sys test_db"
    Exclude specific databases.
""",
    formatter_class=argparse.RawTextHelpFormatter
)

parser.add_argument("-o", "--output", action="store_true", help="Output success message if exit code 0.")
parser.add_argument("-b", "--bucket", required=True, help="Destination S3 bucket.")
parser.add_argument("-d", "--dest", default="databases", help="Destination folder in the bucket (default: databases).")
parser.add_argument("-p", "--profile", required=True, help="AWS profile to use (required).")
parser.add_argument("-u", "--user", default="root", help="MySQL/MariaDB user (default: root).")
parser.add_argument("-e", "--exclude", default="information_schema performance_schema mysql sys", help="Databases to exclude, delimited by space (default: information_schema performance_schema mysql sys).")

args = parser.parse_args()

BUCKET_NAME = args.bucket
DEST_FOLDER = args.dest
PROFILE = args.profile
MYSQL_USER = args.user
EXCLUDE_DB = args.exclude.split()
OUTPUT = args.output

DATE = datetime.utcnow().strftime("%Y-%m-%d")
TIME = datetime.utcnow().strftime("%H-%M")
TMP_FOLDER = "/tmp"
BACKUP_DIR = f"{TMP_FOLDER}/{DATE}/{TIME}"

# Ensure the backup directory exists
os.makedirs(BACKUP_DIR, exist_ok=True)

# Get the list of databases
result = subprocess.run(["mysql", "-u", MYSQL_USER, "-e", "SHOW DATABASES;"], capture_output=True, text=True)
databases = [db for db in result.stdout.split() if db not in EXCLUDE_DB and db != "Database"]

exit_status = 0

# Backup each database
for db in databases:
    filename = f"{db}.sql.gz"
    full_path = f"{BACKUP_DIR}/{filename}"

    # Dump the database and gzip it
    with open(full_path, "wb") as f:
        dump_result = subprocess.run(["mysqldump", "-u", MYSQL_USER, "--databases", db], stdout=subprocess.PIPE)
        gzip_result = subprocess.run(["gzip"], input=dump_result.stdout, stdout=f)

    # Upload to S3
    if OUTPUT:
        upload_result = subprocess.run(["aws", "s3", "cp", full_path, f"s3://{BUCKET_NAME}/{DEST_FOLDER}/{DATE}/{TIME}/{filename}", "--profile", PROFILE], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    else:
        upload_result = subprocess.run(["aws", "s3", "cp", full_path, f"s3://{BUCKET_NAME}/{DEST_FOLDER}/{DATE}/{TIME}/{filename}", "--profile", PROFILE], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    # Check for errors in the upload process
    if upload_result.returncode != 0:
        sys.stderr.write(upload_result.stderr.decode())
        exit_status = -1

    # Remove local file after upload
    os.remove(full_path)

# Clean up the temporary directory
try:
    os.removedirs(BACKUP_DIR)
except OSError:
    pass

# Retention policy: keep only necessary backups
session = boto3.Session(profile_name=PROFILE)
s3 = session.client('s3')
existing_folders = list_s3_folders(BUCKET_NAME, DEST_FOLDER, s3)

# Determine the retention dates
keep_dates = save_days(DATE)
keep_folders = set(keep_dates)

# Filter and delete old backups
for folder in existing_folders:
    if folder not in keep_folders:
        delete_response = s3.delete_objects(
            Bucket=BUCKET_NAME,
            Delete={
                'Objects': [{'Key': key['Key']} for key in s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=f"{DEST_FOLDER}/{folder}/").get('Contents', [])]
            }
        )

if OUTPUT and exit_status == 0:
    print(f"Well done for {BUCKET_NAME}/{DEST_FOLDER}")

sys.exit(exit_status)

