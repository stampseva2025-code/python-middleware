import os # For file system operations like creating folders and saving files
import io # For handling file streams during download
import json # For saving the download plan and status updates
import time # For adding delays between downloads to avoid API rate limits
import threading #  To run the download process in the background without blocking the API response
from flask import request, jsonify # For handling API requests and responses
from google.oauth2 import service_account # For service account authentication
from googleapiclient.discovery import build # For Drive API
from googleapiclient.http import MediaIoBaseDownload # For downloading files in chunks

# ==========================================
# ⚙️ GOOGLE DRIVE CONFIGURATION
# ==========================================
SERVICE_ACCOUNT_FILE = 'service_account.json' # <-- UPDATE THIS with the path to your service account JSON key file
MASTER_FOLDER_ID = '1KwQhFMqBLkitbvOcuHycMhVSXS80JCpy' # <-- UPDATE THIS with the ID of the Drive folder you want to back up
SCOPES = ['https://www.googleapis.com/auth/drive.readonly'] # Read-only scope is sufficient for scanning and downloading files
LOCAL_BACKUP_DIR = '/Users/yourusername/Desktop/StampCloudBackup' # <-- UPDATE THIS

PLAN_FILE = 'drive_sync_plan.json' # This file will store the list of files to download along with their paths, so we can maintain the exact folder structure on the local machine.
STATUS_FILE = 'drive_sync_status.json'# This file will be updated in real-time with the current status of the download process, which the Electron app can read to update the UI.

def authenticate_drive():
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build('drive', 'v3', credentials=creds)

# --- RECURSIVE FOLDER SCANNER ---
def walk_drive_folder(service, folder_id, current_local_path, plan_list):
    """Recursively scans folders to map the exact structure."""
    query = f"'{folder_id}' in parents and trashed = false"
    
    page_token = None
    while True:
        response = service.files().list(
            q=query, spaces='drive', fields='nextPageToken, files(id, name, mimeType, size)', pageToken=page_token
        ).execute()

        for item in response.get('files', []):
            # If it's a folder, dive into it!
            if item['mimeType'] == 'application/vnd.google-apps.folder':
                new_folder_path = os.path.join(current_local_path, item['name'])
                walk_drive_folder(service, item['id'], new_folder_path, plan_list)
            else:
                # If it's a file, add it to our master plan
                plan_list.append({
                    "id": item['id'],
                    "name": item['name'],
                    "path": current_local_path, # The exact sub-folder it belongs in
                    "size": int(item.get('size', 0))
                })

        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break


# ==========================================
# 🚀 ROUTE 1: SCAN & PREVIEW
# ==========================================
@app.route('/local/drive-scan', methods=['GET'])
def drive_scan():
    try:
        service = authenticate_drive()
        plan_list = []
        
        print(f"🔍 Scanning Drive Folder: {MASTER_FOLDER_ID}")
        walk_drive_folder(service, MASTER_FOLDER_ID, LOCAL_BACKUP_DIR, plan_list)
        
        # Save the plan to a local JSON file
        with open(PLAN_FILE, 'w') as f:
            json.dump(plan_list, f, indent=4)
            
        total_size_mb = sum([f['size'] for f in plan_list]) / (1024 * 1024)
            
        return jsonify({
            "success": True, 
            "total_files": len(plan_list),
            "total_size_mb": round(total_size_mb, 2),
            "files": plan_list
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


# ==========================================
# BACKGROUND DOWNLOAD ENGINE
# ==========================================
def background_download_process():
    """Runs invisibly in the background so the API doesn't timeout."""
    try:
        service = authenticate_drive()
        
        with open(PLAN_FILE, 'r') as f:
            plan_list = json.load(f)
            
        total_files = len(plan_list)
        
        for index, file_data in enumerate(plan_list):
            # 1. Update the status file for Electron to read
            status = {
                "state": "downloading",
                "current_file": file_data['name'],
                "progress": f"{index + 1} / {total_files}",
                "percent": int(((index + 1) / total_files) * 100)
            }
            with open(STATUS_FILE, 'w') as f:
                json.dump(status, f)

            # 2. Create the exact folder structure on the Mac
            local_folder = file_data['path']
            os.makedirs(local_folder, exist_ok=True)
            
            local_file_path = os.path.join(local_folder, file_data['name'])

            # 3. Download the file (Skip if it already exists!)
            if not os.path.exists(local_file_path):
                print(f"📥 Downloading: {file_data['name']}")
                request = service.files().get_media(fileId=file_data['id'])
                fh = io.FileIO(local_file_path, 'wb')
                downloader = MediaIoBaseDownload(fh, request)
                
                done = False
                while done is False:
                    _, done = downloader.next_chunk()
                
                # ⏳ 4. WAIT BETWEEN DOWNLOADS (e.g., 3 seconds)
                time.sleep(3) 
            else:
                print(f"⏭️ Skipped (Already exists): {file_data['name']}")

        # Mark as finished!
        with open(STATUS_FILE, 'w') as f:
            json.dump({"state": "completed", "percent": 100, "message": "All files downloaded successfully!"}, f)

    except Exception as e:
        with open(STATUS_FILE, 'w') as f:
            json.dump({"state": "error", "error": str(e)}, f)


# ==========================================
# 🚀 ROUTE 2: TRIGGER DOWNLOAD
# ==========================================
@app.route('/local/drive-start', methods=['POST'])
def drive_start():
    # Reset status file
    with open(STATUS_FILE, 'w') as f:
        json.dump({"state": "initializing", "percent": 0}, f)
        
    # Start the background thread!
    thread = threading.Thread(target=background_download_process)
    thread.daemon = True # Allows Flask to close even if this is running
    thread.start()
    
    return jsonify({"success": True, "message": "Download engine started in background."})


# ==========================================
# 🚀 ROUTE 3: CHECK STATUS (For UI Polling)
# ==========================================
@app.route('/local/drive-status', methods=['GET'])
def drive_status():
    if not os.path.exists(STATUS_FILE):
        return jsonify({"state": "idle"})
        
    with open(STATUS_FILE, 'r') as f:
        status = json.load(f)
        
    return jsonify(status)