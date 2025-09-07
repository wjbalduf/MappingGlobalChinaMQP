#Google Drive API Setup-----------------------------------------------------------------------
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError
import os
import json
import time
import hashlib

# Path to your service account JSON
SERVICE_ACCOUNT_FILE = 'b23c9a5b080ef97e332cb82994f587c654753ea0.json'

# Scopes required for Google Drive
SCOPES = ['https://www.googleapis.com/auth/drive']

# Create credentials
creds = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=SCOPES
)

# Build the Drive API client
drive_service = build('drive', 'v3', credentials=creds)

# companies folder ID
BASE_FOLDER_ID = '1nhbclFufYn9iuG-mvaprNmv3bI7wk_ES' 

#Folder Creation Helper------------------------------------------------------------------------------------
def create_drive_folder(name, parent_id):
    """Return the folder ID for a folder with this name under parent_id."""
    query = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and '{parent_id}' in parents"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])
    
    if items:
        return items[0]['id']  # folder already exists
    
    # Create folder if missing
    file_metadata = {
        'name': name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_id]
    }
    folder = drive_service.files().create(body=file_metadata, fields='id').execute()
    return folder['id']

#Generate Paths per Issuer--------------------------------------------------------------------------------------
def get_drive_paths(ticker, form, year, base_folder_id=BASE_FOLDER_ID):
    """
    Ensure the following structure exists in Drive:
    /companies/{TICKER}/{FORM}/{YEAR}/
                                /exhibits/
                                /metadata/
    
    Returns a dictionary of folder IDs.
    """
    ticker_id = create_drive_folder(ticker, base_folder_id)
    form_id = create_drive_folder(form, ticker_id)
    year_id = create_drive_folder(year, form_id)
    exhibits_id = create_drive_folder('exhibits', year_id)
    metadata_id = create_drive_folder('metadata', year_id)
    
    return {
        'year': year_id,
        'exhibits': exhibits_id,
        'metadata': metadata_id
    }

#SHA256 helper + manifest for idempotency--------------------------------------------------------------------
# Load or create local manifest
manifest_path = 'manifest.jsonl'
if os.path.exists(manifest_path):
    with open(manifest_path, 'r') as f:
        manifest = {json.loads(line)['sha256']: line for line in f}
else:
    manifest = {}

def sha256_file(file_path):
    """Return SHA256 of a file."""
    h = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()

#Upload File----------------------------------------------------------------------------------------------------
def upload_file(local_path, parent_id, mime_type=None):
    """Upload a file to a Shared Drive with retry/backoff; skip if already uploaded by SHA."""
    file_hash = sha256_file(local_path)
    if file_hash in manifest:
        print(f"Skipping (already uploaded): {local_path}")
        return

    media = MediaFileUpload(local_path, mimetype=mime_type)
    file_metadata = {
        'name': os.path.basename(local_path),
        'parents': [parent_id]
    }

    for attempt in range(5):
        try:
            drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id',
                supportsAllDrives=True
            ).execute()

            manifest[file_hash] = json.dumps({
                'local_path': local_path,
                'sha256': file_hash
            })
            with open(manifest_path, 'a') as f:
                f.write(manifest[file_hash] + '\n')

            print(f"Uploaded: {local_path}")
            break

        except HttpError as e:
            print(f"Upload failed (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)

#Sync Function----------------------------------------------------------------------------------------------------------
def sync_filing(filing_path, exhibits=[], metadata_files=[], ticker='BABA', form='20-F', year='2023'):
    paths = get_drive_paths(ticker, form, year)
    
    # Upload main filing
    upload_file(filing_path, paths['year'], mime_type='text/html')
    
    # Upload exhibits
    for ex in exhibits:
        upload_file(ex, paths['exhibits'], mime_type='application/pdf')  # adjust MIME if needed
    
    # Upload metadata
    for meta in metadata_files:
        upload_file(meta, paths['metadata'], mime_type='application/json')

#Test--------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    # Verify that BASE_FOLDER_ID points to the right folder
    folder = drive_service.files().get(
        fileId=BASE_FOLDER_ID,
        fields="id, name, driveId",
        supportsAllDrives=True
    ).execute()
    print("BASE FOLDER CHECK:", folder)


    # Paths to local test files
    filing_path = "test_upload/filing.html"
    exhibits = ["test_upload/ex3_charter.pdf"]
    metadata_files = ["test_upload/filing.json"]

    sync_filing(
        filing_path=filing_path,
        exhibits=exhibits,
        metadata_files=metadata_files,
        ticker="TESTCO",
        form="10-K",
        year="2025"
    )
