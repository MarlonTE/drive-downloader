import re
import os
import time
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import argparse
import logging
from logging.handlers import RotatingFileHandler
import hashlib
import csv
import json

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
from google.auth.exceptions import RefreshError

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import threading

DOWNLOAD_FOLDER = "drive_downloads"
DEFAULT_MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 1
DEFAULT_MAX_PARALLEL_DOWNLOADS = 4
DEFAULT_CHUNK_SIZE = 32768
MIN_THROUGHPUT_THRESHOLD = 10 * 1024
LOW_THROUGHPUT_TIMEOUT = 30

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
logger.addHandler(console_handler)

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

app = FastAPI()
global_download_summary_data = []
global_failed_original_links = set()
global_active_downloads = {}

def extract_file_id(url: str) -> str | None:
    """
    Extracts the Google Drive file ID from a given URL.

    Args:
        url (str): The Google Drive URL.

    Returns:
        str | None: The extracted file ID, or None if not found.
    """
    match = re.search(r'(?:id=|\/d\/)([a-zA-Z0-9_-]+)', url)
    if match:
        return match.group(1)
    return None

def is_valid_drive_link(url: str) -> bool:
    """
    Checks if a given URL is a valid Google Drive link.

    Args:
        url (str): The URL to validate.

    Returns:
        bool: True if it's a valid Google Drive link, False otherwise.
    """
    drive_patterns = [
        r'https?:\/\/drive\.google\.com\/file\/d\/[a-zA-Z0-9_-]+(?:\/view)?',
        r'https?:\/\/drive\.google\.com\/open\?id=[a-zA-Z0-9_-]+',
        r'https?:\/\/drive\.google\.com\/uc\?export=download&id=[a-zA-Z0-9_-]+'
    ]
    for pattern in drive_patterns:
        if re.match(pattern, url):
            return True
    return False

def calculate_sha256(file_path: str, chunk_size: int = DEFAULT_CHUNK_SIZE) -> str | None:
    """
    Calculates the SHA256 hash of a file.

    Args:
        file_path (str): The path to the file.
        chunk_size (int): The size of chunks to read for hashing.

    Returns:
        str | None: The SHA256 hash in hexadecimal, or None if an error occurred.
    """
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(chunk_size), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception as e:
        logger.error(f"Error calculating SHA256 for {file_path}: {e}")
        return None

def get_google_drive_api_credentials():
    """
    Retrieves Google Drive API credentials. It tries to load from token.json,
    then refreshes if expired, and finally initiates an OAuth flow if needed.

    Returns:
        Credentials | None: Google API credentials, or None if authentication fails.
    """
    SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
    creds = None
    if os.path.exists('token.json'):
        try:
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        except Exception:
            creds = None
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except RefreshError:
                creds = None
            except requests.exceptions.RequestException:
                creds = None
            except Exception:
                creds = None
        
        if not creds or not creds.valid:
            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            except FileNotFoundError:
                logger.error("credentials.json not found. API downloads require this file.")
                return None
            except requests.exceptions.RequestException as e:
                logger.error(f"Network error during OAuth flow: {e}")
                return None
            except Exception as e:
                logger.error(f"An unexpected error occurred during OAuth flow: {e}")
                return None
        
        if creds and creds.valid:
            try:
                with open('token.json', 'w') as token:
                    token.write(creds.to_json())
            except Exception as e:
                logger.warning(f"Could not save token.json: {e}")
    
    return creds

def download_file_with_api(file_id: str, output_path: str, chunk_size: int) -> tuple[bool, str, int, float, str, str | None]:
    """
    Downloads a file from Google Drive using the official Google Drive API.

    Args:
        file_id (str): The ID of the file to download.
        output_path (str): The directory to save the downloaded file.
        chunk_size (int): The size of chunks to download.

    Returns:
        tuple[bool, str, int, float, str, str | None]: A tuple containing:
            - bool: True if download was successful, False otherwise.
            - str: The final file name.
            - int: Total size of the file in bytes.
            - float: Start time of the download.
            - str: Status of the download ("Success", "Failed", "Skipped", etc.).
            - str | None: SHA256 hash of the downloaded file, or None.
    """
    start_time = time.time()
    file_name = f"{file_id}_api.bin" # Default file name
    total_size = 0
    status = "Failed"
    sha256_hash_result = None

    try:
        creds = get_google_drive_api_credentials()
        if not creds:
            status = "Failed (Auth Error)"
            logger.error("Google Drive API authentication failed. Cannot download.")
            return False, file_name, total_size, start_time, status, sha256_hash_result

        service = build('drive', 'v3', credentials=creds)

        # Get file metadata (name and size)
        file_metadata = service.files().get(fileId=file_id, fields='name,size').execute()
        file_name = file_metadata.get('name', file_name)
        total_size = int(file_metadata.get('size', 0))

        # Sanitize file name and ensure uniqueness
        sanitized_file_name = "".join([c for c in file_name if c.isalnum() or c in ('.', '_', '-')])
        if not sanitized_file_name:
            sanitized_file_name = file_name # Fallback if sanitization results in empty string

        base_name, ext = os.path.splitext(sanitized_file_name)
        unique_file_name = sanitized_file_name
        counter = 0
        while os.path.exists(os.path.join(output_path, unique_file_name)):
            if counter == 0:
                unique_file_name = f"{base_name}_{file_id}{ext}"
            else:
                unique_file_name = f"{base_name}_{file_id}_{counter}{ext}"
            counter += 1
        
        full_output_path = os.path.join(output_path, unique_file_name)
        file_name = unique_file_name # Update file_name to the unique one

        downloaded_bytes = 0
        if os.path.exists(full_output_path):
            downloaded_bytes = os.path.getsize(full_output_path)
            if downloaded_bytes == total_size and total_size > 0:
                status = "Skipped"
                sha256_hash_result = calculate_sha256(full_output_path, chunk_size)
                logger.info(f"File '{file_name}' already fully downloaded via API. Skipping.")
                return True, file_name, total_size, start_time, status, sha256_hash_result
            elif downloaded_bytes > 0:
                logger.info(f"Resuming download for '{file_name}' from {downloaded_bytes} bytes (API).")
            else:
                downloaded_bytes = 0 # File exists but is empty/corrupt, restart download

        request = service.files().get_media(fileId=file_id)
        # Use 'ab' mode for appending if resuming, 'wb' if starting fresh
        fh = io.FileIO(full_output_path, 'ab') 
        downloader = MediaIoBaseDownload(fh, request, chunksize=chunk_size)

        done = False
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=file_name, initial=downloaded_bytes) as pbar:
            while not done:
                status_api, done = downloader.next_chunk()
                if status_api:
                    # Update progress bar with actual downloaded bytes
                    pbar.update(status_api.resumable_progress - pbar.n) 
        
        fh.close() # Close the file handle after download is complete

        sha256_hash_result = calculate_sha256(full_output_path, chunk_size)
        if sha256_hash_result:
            sha256_file_path = f"{full_output_path}.sha256"
            with open(sha256_file_path, 'w') as f:
                    f.write(sha256_hash_result)
            logger.info(f"SHA256 hash calculated and saved for '{file_name}': {sha256_hash_result}")

        status = "Success"
        return True, file_name, total_size, start_time, status, sha256_hash_result

    except Exception as e:
        status = "Failed (API Error)"
        logger.error(f"Error downloading file {file_id} with Google Drive API: {e}", exc_info=True)
        return False, file_name, total_size, start_time, status, sha256_hash_result

def download_file_from_google_drive(file_id: str, output_path: str, max_retries: int, chunk_size: int) -> tuple[bool, str, int, float, str, str | None]:
    """
    Downloads a file from Google Drive using direct HTTP requests.
    Handles confirmation forms and quota exceeded errors.

    Args:
        file_id (str): The ID of the file to download.
        output_path (str): The directory to save the downloaded file.
        max_retries (int): Maximum number of retries for the download.
        chunk_size (int): The size of chunks to download.

    Returns:
        tuple[bool, str, int, float, str, str | None]: A tuple containing:
            - bool: True if download was successful, False otherwise.
            - str: The final file name.
            - int: Total size of the file in bytes.
            - float: Start time of the download.
            - str: Status of the download ("Success", "Failed", "Skipped", etc.).
            - str | None: SHA256 hash of the downloaded file, or None.
    """
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    session = requests.Session()
    
    start_time = time.time()
    file_name = f"{file_id}.bin" # Default file name
    total_file_size_expected = 0
    status = "Failed"
    sha256_hash_result = None

    for retry_count in range(max_retries + 1):
        current_downloaded_bytes = 0
        headers = {}
        full_output_path = os.path.join(output_path, file_name)

        pre_flight_url = download_url
        pre_flight_params = {}
        target_response_headers = None

        try:
            # First, make a HEAD request or a small GET request to get metadata and handle confirmation forms
            pre_flight_response = session.get(pre_flight_url, stream=True, timeout=15)
            pre_flight_response.raise_for_status()

            if 'Content-Type' in pre_flight_response.headers and 'text/html' in pre_flight_response.headers['Content-Type']:
                logger.info(f"HTML response detected from Google Drive for {file_id} during pre-flight. Checking for confirmation form or error messages.")
                
                soup = BeautifulSoup(pre_flight_response.text, 'html.parser')
                download_form = soup.find('form', {'id': 'download-form'})

                if download_form:
                    action_url = download_form.get('action')
                    if not action_url.startswith('http'):
                        action_url = f"https://drive.google.com{action_url}"
                    
                    pre_flight_params = {input_tag.get('name'): input_tag.get('value') for input_tag in download_form.find_all('input', {'type': 'hidden'}) if input_tag.get('name') and input_tag.get('value')}
                    
                    logger.info(f"Confirmation form found. Using form parameters for {file_id}.")

                    # After getting form parameters, make a HEAD request to the actual download URL
                    head_response_after_form = session.head(action_url, params=pre_flight_params, allow_redirects=True, timeout=10)
                    head_response_after_form.raise_for_status()
                    target_response_headers = head_response_after_form.headers
                    download_url = action_url # Update download_url to the action URL
                else:
                    page_text = pre_flight_response.text.lower()
                    quota_keywords = ["quota exceeded", "limit exceeded", "excess traffic", "download limit", "virus scan warning"]
                    is_quota_error = any(keyword in page_text for keyword in quota_keywords)

                    if is_quota_error:
                        status = "Failed (Quota Exceeded)"
                        error_message = (
                            f"[ERROR - QUOTA EXCEEDED] Could not download file with ID: {file_id}\n"
                            f"➤ Reason: Google Drive has temporarily blocked the download due to excessive traffic or virus scan warning.\n"
                            f"➤ Solution: Make a copy of the file in your personal Google Drive account and use its new ID for download.\n"
                            f"➤ Note: If a .bin file was created, it is NOT the original file, but an automatic error response from Google."
                        )
                        logger.error(error_message)
                        return False, file_name, total_file_size_expected, start_time, status, sha256_hash_result
                    else:
                        status = "Failed (Unrecognized HTML Page)"
                        logger.error(f"Received an unrecognized HTML page for {file_id}. This might indicate an unexpected error or change in Google Drive's response.")
                        return False, file_name, total_file_size_expected, start_time, status, sha256_hash_result
            else:
                target_response_headers = pre_flight_response.headers
                # Consume the response content to close the connection properly
                for _ in pre_flight_response.iter_content(chunk_size=chunk_size):
                    pass
                pre_flight_response.close()

            # Extract filename from Content-Disposition header
            if 'Content-Disposition' in target_response_headers:
                fname_match = re.search(r'filename\*?=UTF-8\'\'(.+)', target_response_headers['Content-Disposition'])
                if fname_match:
                    file_name = requests.utils.unquote(fname_match.group(1))
                else:
                    fname_match = re.search(r'filename="([^"]+)"', target_response_headers['Content-Disposition'])
                    if fname_match:
                        file_name = fname_match.group(1)
            
            # Sanitize file name and ensure uniqueness
            sanitized_file_name = "".join([c for c in file_name if c.isalnum() or c in ('.', '_', '-')])
            if not sanitized_file_name:
                sanitized_file_name = file_name # Fallback if sanitization results in empty string
            
            base_name, ext = os.path.splitext(sanitized_file_name)
            unique_file_name = sanitized_file_name
            counter = 0
            while os.path.exists(os.path.join(output_path, unique_file_name)):
                if counter == 0:
                    unique_file_name = f"{base_name}_{file_id}{ext}"
                else:
                    unique_file_name = f"{base_name}_{file_id}_{counter}{ext}"
                counter += 1
            
            full_output_path = os.path.join(output_path, unique_file_name)
            file_name = unique_file_name # Update file_name to the unique one

            total_file_size_expected = int(target_response_headers.get('content-length', 0))

            file_mode = 'wb' # Default to write binary mode
            if os.path.exists(full_output_path):
                current_downloaded_bytes = os.path.getsize(full_output_path)
                if current_downloaded_bytes == total_file_size_expected and total_file_size_expected > 0:
                    status = "Skipped"
                    sha256_hash_result = calculate_sha256(full_output_path, chunk_size)
                    logger.info(f"File '{file_name}' already fully downloaded. Skipping.")
                    return True, file_name, total_file_size_expected, start_time, status, sha256_hash_result
                elif current_downloaded_bytes > 0:
                    logger.info(f"Resuming download for '{file_name}' from {current_downloaded_bytes} bytes.")
                    headers['Range'] = f'bytes={current_downloaded_bytes}-'
                    file_mode = 'ab' # Append binary mode for resuming
                else:
                    current_downloaded_bytes = 0 # File exists but is empty/corrupt, restart download
                    file_mode = 'wb'
                    logger.info(f"File '{file_name}' exists but is empty/corrupt. Restarting download.")

        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            logger.error(f"Error during pre-flight or initial request for {file_id} ('{file_name}'): {e}")
            if retry_count < max_retries:
                wait_time = RETRY_BACKOFF_FACTOR * (2 ** retry_count)
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue # Go to the next retry attempt
            else:
                status = "Failed (Pre-flight/Initial Request Error)"
                logger.error(f"Final failure: Could not get metadata or handle confirmation for {file_id} ('{file_name}') after {max_retries} retries.")
                return False, file_name, total_file_size_expected, start_time, status, sha256_hash_result
        except Exception as e:
            status = "Failed (Unexpected Pre-flight Error)"
            logger.critical(f"Unexpected error during pre-flight for {file_id} ('{file_name}'): {e}", exc_info=True)
            return False, file_name, total_file_size_expected, start_time, status, sha256_hash_result

        try:
            # Main download request
            response = session.get(download_url, params=pre_flight_params, stream=True, headers=headers, timeout=60)
            response.raise_for_status()

            # Check if server honored Range header for resuming
            if current_downloaded_bytes > 0 and response.status_code == 200:
                logger.warning(f"Server did NOT honor Range header for '{file_name}'. Restarting download from beginning.")
                current_downloaded_bytes = 0
                file_mode = 'wb'

            if 'Content-Type' in response.headers and 'text/html' in response.headers['Content-Type']:
                logger.error(f"Critical error! Unexpected HTML content received during main download for file ID {file_id}. This indicates an access issue or a change in Google Drive's response not handled in pre-flight. The downloaded file might be an error page.")
                status = "Failed (Unexpected HTML Content)"
                return False, file_name, total_file_size_expected, start_time, status, sha256_hash_result

            downloaded_chunks_bytes = 0
            last_throughput_check_time = time.time()
            bytes_since_last_check = 0
            low_throughput_start_time = None

            with open(full_output_path, file_mode) as f:
                with tqdm(total=total_file_size_expected, unit='B', unit_scale=True, desc=file_name, initial=current_downloaded_bytes) as pbar:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
                            downloaded_chunks_bytes += len(chunk)
                            bytes_since_last_check += len(chunk)

                            current_time = time.time()
                            if current_time - last_throughput_check_time >= 5: # Check throughput every 5 seconds
                                elapsed_check_time = current_time - last_throughput_check_time
                                current_throughput = bytes_since_last_check / elapsed_check_time if elapsed_check_time > 0 else 0

                                if current_throughput < MIN_THROUGHPUT_THRESHOLD:
                                    if low_throughput_start_time is None:
                                        low_throughput_start_time = current_time
                                    elif current_time - low_throughput_start_time >= LOW_THROUGHPUT_TIMEOUT:
                                        logger.warning(f"Low throughput detected for '{file_name}' ({current_throughput:.2f} B/s). Download might be slow or stuck.")
                                        low_throughput_start_time = current_time # Reset timer to avoid continuous warnings
                                else:
                                    low_throughput_start_time = None # Reset low throughput timer if throughput recovers

                                last_throughput_check_time = current_time
                                bytes_since_last_check = 0
            
            sha256_hash_result = calculate_sha256(full_output_path, chunk_size)
            if sha256_hash_result:
                sha256_file_path = f"{full_output_path}.sha256"
                with open(sha256_file_path, 'w') as f:
                    f.write(sha256_hash_result)
                logger.info(f"SHA256 hash calculated and saved for '{file_name}': {sha256_hash_result}")

            status = "Success"
            return True, file_name, total_file_size_expected, start_time, status, sha256_hash_result

        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            logger.error(f"Error during main download attempt {retry_count + 1} for {file_id} ('{file_name}'): {e}")
            if retry_count < max_retries:
                wait_time = RETRY_BACKOFF_FACTOR * (2 ** retry_count)
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                status = "Failed (Max Retries for Main Download)"
                logger.error(f"Final failure: Could not download {file_id} ('{file_name}') after {max_retries} retries.")
                return False, file_name, total_file_size_expected, start_time, status, sha256_hash_result
        except Exception as e:
            status = "Failed (Unexpected Error During Main Download)"
            logger.critical(f"Unexpected error downloading {file_id} ('{file_name}'): {e}", exc_info=True)
            return False, file_name, total_file_size_expected, start_time, status, sha256_hash_result
    
    return False, file_name, total_file_size_expected, start_time, status, sha256_hash_result

def main():
    """
    Main function to parse arguments, set up logging, and orchestrate downloads.
    Can run as a standalone script or a FastAPI server.
    """
    start_total_process_time = time.time()

    parser = argparse.ArgumentParser(description="Google Drive Downloader Script.")
    parser.add_argument("--workers", type=int, default=DEFAULT_MAX_PARALLEL_DOWNLOADS,
                        help=f"Number of parallel download threads (default: {DEFAULT_MAX_PARALLEL_DOWNLOADS}).")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE,
                        help=f"Chunk size in bytes for streaming downloads (default: {DEFAULT_CHUNK_SIZE} bytes).")
    parser.add_argument("--retries", type=int, default=DEFAULT_MAX_RETRIES,
                        help=f"Maximum number of retries per download (default: {DEFAULT_MAX_RETRIES}).")
    parser.add_argument("--input-file", type=str,
                        help="Path to a text file containing Google Drive links (one link per line).")
    parser.add_argument("--dry-run", action="store_true",
                        help="If set, the script will only validate links and show a summary, without downloading any files.")
    parser.add_argument("--use-api", action="store_true",
                        help="Use the official Google Drive API for downloads (requires credentials.json and token.json).")
    parser.add_argument("--serve", action="store_true",
                        help="Run the script as a FastAPI server.")
    parser.add_argument("--port", type=int, default=8000,
                        help="Port for the FastAPI server (default: 8000).")
    args = parser.parse_args()

    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
    
    # Configure file logging
    log_file_path = os.path.join(DOWNLOAD_FOLDER, "downloads.log")
    # Remove existing file handlers to avoid duplicate logs if main is called multiple times
    for handler in logger.handlers:
        if isinstance(handler, RotatingFileHandler):
            logger.removeHandler(handler)
    file_handler = RotatingFileHandler(log_file_path, maxBytes=5*1024*1024, backupCount=5)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)

    logger.info("--- Starting Google Drive Downloader ---")
    logger.info(f"Files will be saved to: '{os.path.abspath(DOWNLOAD_FOLDER)}'")
    logger.info(f"Configured for {args.workers} parallel downloads.")
    logger.info(f"Using chunk size: {args.chunk_size} bytes.")
    logger.info(f"Maximum retries per file: {args.retries}.")

    logger.info(f"System CPU cores detected: {os.cpu_count()}")
    if PSUTIL_AVAILABLE:
        total_memory_bytes = psutil.virtual_memory().total
        logger.info(f"Total system memory: {total_memory_bytes / (1024**3):.2f} GB")
    
    logger.info("Checking connection to Google Drive...")
    try:
        requests.get("https://drive.google.com", timeout=10)
        logger.info("Connection to Google Drive successful.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Google Drive connection error: Could not connect to https://drive.google.com. Please check your internet connection. Error: {e}")
        return

    if args.serve:
        logger.info(f"Starting FastAPI server on http://0.0.0.0:{args.port}")
        # Pass arguments and global states to FastAPI app state
        app.state.download_summary_data = global_download_summary_data
        app.state.failed_original_links = global_failed_original_links
        app.state.active_downloads = global_active_downloads
        app.state.download_args = args

        class DownloadRequest(BaseModel):
            link: str

        @app.post("/download")
        async def api_download(request: DownloadRequest):
            """
            API endpoint to initiate a Google Drive download.
            """
            link = request.link
            file_id = extract_file_id(link)
            if not file_id:
                raise HTTPException(status_code=400, detail="Invalid Google Drive link or file ID not found.")
            
            request_id = str(time.time()).replace('.', '')
            global_active_downloads[request_id] = {"status": "pending", "file_id": file_id, "link": link, "progress": 0, "file_name": "N/A"}
            
            def run_download_task():
                """
                Background task to perform the actual download.
                """
                try:
                    if app.state.download_args.use_api:
                        success, file_name, total_size, download_start_time, status, sha256_hash_result = \
                            download_file_with_api(file_id, DOWNLOAD_FOLDER, app.state.download_args.chunk_size)
                    else:
                        success, file_name, total_size, download_start_time, status, sha256_hash_result = \
                            download_file_from_google_drive(file_id, DOWNLOAD_FOLDER, app.state.download_args.retries, app.state.download_args.chunk_size)
                    
                    end_time = time.time()
                    duration = end_time - download_start_time
                    avg_speed = (total_size / duration) if duration > 0 else 0

                    summary_entry = {
                        'file_id': file_id,
                        'filename': file_name,
                        'status': status,
                        'size_bytes': total_size,
                        'duration_seconds': round(duration, 2),
                        'sha256_hash': sha256_hash_result if sha256_hash_result else 'N/A'
                    }
                    global_download_summary_data.append(summary_entry)
                    if not success:
                        global_failed_original_links.add(link)
                    
                    global_active_downloads[request_id].update({
                        "status": "completed" if success else "failed",
                        "file_name": file_name,
                        "total_size": total_size,
                        "duration": duration,
                        "avg_speed": avg_speed,
                        "sha256": sha256_hash_result,
                        "progress": 100 # Assuming 100% on completion/failure
                    })
                    logger.info(f"API download completed for {file_id}: {status}")

                except Exception as e:
                    logger.error(f"Error during API download for {file_id}: {e}", exc_info=True)
                    global_active_downloads[request_id].update({"status": "error", "message": str(e), "progress": 0})
                    global_failed_original_links.add(link)

            threading.Thread(target=run_download_task).start()
            return {"message": "Download initiated", "file_id": file_id, "request_id": request_id}

        @app.get("/status")
        async def api_status():
            """
            API endpoint to get the status of active downloads.
            """
            return {"active_downloads": global_active_downloads}

        @app.get("/summary")
        async def api_summary():
            """
            API endpoint to get the download summary.
            """
            summary_json_path = os.path.join(DOWNLOAD_FOLDER, "download_summary.json")
            if os.path.exists(summary_json_path):
                try:
                    with open(summary_json_path, 'r', encoding='utf-8') as f:
                        return json.load(f)
                except Exception as e:
                    raise HTTPException(status_code=500, detail=f"Error reading summary file: {e}")
            raise HTTPException(status_code=404, detail="Download summary not found.")

        uvicorn.run(app, host="0.0.0.0", port=args.port)
        return

    drive_links = []
    if args.input_file:
        file_path = args.input_file
        if not os.path.exists(file_path):
            logger.error(f"Error: Input file '{file_path}' not found. Please provide a valid path.")
            return
        if not os.access(file_path, os.R_OK):
            logger.error(f"Error: Input file '{file_path}' is not readable. Check permissions.")
            return
        try:
            with open(file_path, 'r') as f:
                drive_links = [line.strip() for line in f if line.strip()]
            logger.info(f"Loaded {len(drive_links)} links from '{file_path}'.")
        except Exception as e:
            logger.error(f"Error reading input file '{file_path}': {e}")
            return
    else:
        print("Enter Google Drive links (one per line). Press Enter twice to finish:")
        while True:
            link = input()
            if not link:
                break
            drive_links.append(link.strip())
        logger.info(f"Entered {len(drive_links)} links.")

    if not drive_links:
        logger.error("No links provided for download. Exiting.")
        return

    download_tasks = []
    valid_links_count = 0
    
    for link in drive_links:
        if not is_valid_drive_link(link):
            logger.warning(f"Invalid Google Drive link format: {link}. It will be skipped.")
            global_failed_original_links.add(link)
            continue

        file_id = extract_file_id(link)
        if file_id:
            download_tasks.append((file_id, DOWNLOAD_FOLDER, args.retries, args.chunk_size, link))
            valid_links_count += 1
        else:
            logger.warning(f"Could not extract FILEID from URL: {link}. It will be skipped.")
            global_failed_original_links.add(link)

    if valid_links_count == 0:
        logger.error("No valid Google Drive FILEIDs found in the provided input. Exiting.")
        if args.dry_run:
            logger.info("\n--- Dry Run Mode Activated ---")
            logger.info(f"Found {valid_links_count} valid links for download.")
            if global_failed_original_links:
                logger.info(f"Found {len(global_failed_original_links)} invalid links or links with unextractable IDs:")
                for link in sorted(list(global_failed_original_links)):
                    logger.info(f"  - {link}")
            logger.info("The script will exit without performing any downloads.")
        return

    if args.dry_run:
        logger.info("\n--- Dry Run Mode Activated ---")
        logger.info(f"Found {valid_links_count} valid links for download.")
        if global_failed_original_links:
            logger.info(f"Found {len(global_failed_original_links)} invalid links or links with unextractable IDs:")
            for link in sorted(list(global_failed_original_links)):
                    logger.info(f"  - {link}")
        logger.info("The script will exit without performing any downloads.")
        return

    completed_downloads_count = 0
    failed_downloads_count = 0

    if args.workers > 1:
        logger.info(f"\nStarting parallel downloads ({args.workers} threads)...")
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_task = {}
            for task in download_tasks:
                file_id, output_folder, retries, chunk_size, original_link = task
                if args.use_api:
                    future = executor.submit(download_file_with_api, file_id, output_folder, chunk_size)
                else:
                    future = executor.submit(download_file_from_google_drive, file_id, output_folder, retries, chunk_size)
                future_to_task[future] = task

            try:
                for future in as_completed(future_to_task):
                    original_link = future_to_task[future][4]
                    file_id_for_log = future_to_task[future][0]
                    try:
                        success, file_name, total_size, download_start_time, status, sha256_hash_result = future.result()
                        end_time = time.time()
                        duration = end_time - download_start_time
                        avg_speed = (total_size / duration) if duration > 0 else 0

                        log_message = (
                            f"Download Finished: ID={file_id_for_log} | Name='{file_name}' | Status: {status} | "
                            f"Duration: {duration:.2f}s | Size: {total_size} bytes | "
                            f"Average Speed: {avg_speed / 1024:.2f} KB/s"
                        )
                        if success:
                            logger.info(log_message)
                            completed_downloads_count += 1
                        else:
                            logger.error(log_message)
                            failed_downloads_count += 1
                            global_failed_original_links.add(original_link)
                        
                        global_download_summary_data.append({
                            'file_id': file_id_for_log,
                            'filename': file_name,
                            'status': status,
                            'size_bytes': total_size,
                            'duration_seconds': round(duration, 2),
                            'sha256_hash': sha256_hash_result if sha256_hash_result else 'N/A'
                        })
                    except Exception as exc:
                        logger.critical(f"Task for {file_id_for_log} (Link: {original_link}) generated an exception: {exc}", exc_info=True)
                        failed_downloads_count += 1
                        global_failed_original_links.add(original_link)
                        global_download_summary_data.append({
                            'file_id': file_id_for_log,
                            'filename': 'N/A',
                            'status': 'Failed (Exception)',
                            'size_bytes': 0,
                            'duration_seconds': round(time.time() - start_total_process_time, 2),
                            'sha256_hash': 'N/A'
                        })
            except KeyboardInterrupt:
                logger.warning("KeyboardInterrupt detected. Shutting down executor gracefully...")
                executor.shutdown(wait=False, cancel_futures=True)
                for future in future_to_task:
                    original_link = future_to_task[future][4]
                    file_id_for_log = future_to_task[future][0]
                    if future.done() and not future.cancelled():
                        try:
                            success, file_name, total_size, download_start_time, status, sha256_hash_result = future.result()
                            end_time = time.time()
                            duration = end_time - download_start_time
                            avg_speed = (total_size / duration) if duration > 0 else 0

                            log_message = (
                                f"Download Finished (Interrupted): ID={file_id_for_log} | Name='{file_name}' | Status: {status} | "
                                f"Duration: {duration:.2f}s | Size: {total_size} bytes | "
                                f"Average Speed: {avg_speed / 1024:.2f} KB/s"
                            )
                            if success:
                                logger.info(log_message)
                                completed_downloads_count += 1
                            else:
                                logger.error(log_message)
                                failed_downloads_count += 1
                                global_failed_original_links.add(original_link)
                            global_download_summary_data.append({
                                'file_id': file_id_for_log,
                                'filename': file_name,
                                'status': status,
                                'size_bytes': total_size,
                                'duration_seconds': round(duration, 2),
                                'sha256_hash': sha256_hash_result if sha256_hash_result else 'N/A'
                            })
                        except Exception as exc:
                            logger.critical(f"Interrupted task for {file_id_for_log} (Link: {original_link}) generated an exception: {exc}", exc_info=True)
                            failed_downloads_count += 1
                            global_failed_original_links.add(original_link)
                            global_download_summary_data.append({
                                'file_id': file_id_for_log,
                                'filename': 'N/A',
                                'status': 'Failed (Interrupted Exception)',
                                'size_bytes': 0,
                                'duration_seconds': round(time.time() - start_total_process_time, 2),
                                'sha256_hash': 'N/A'
                            })
                    elif future.cancelled():
                        logger.info(f"Download for {file_id_for_log} (Link: {original_link}) was cancelled due to interruption.")
                        failed_downloads_count += 1
                        global_failed_original_links.add(original_link)
                        global_download_summary_data.append({
                            'file_id': file_id_for_log,
                            'filename': 'N/A',
                            'status': 'Cancelled',
                            'size_bytes': 0,
                            'duration_seconds': 0,
                            'sha256_hash': 'N/A'
                        })
                    elif not future.done():
                        logger.info(f"Download for {file_id_for_log} (Link: {original_link}) was still in progress and did not complete.")
                        failed_downloads_count += 1
                        global_failed_original_links.add(original_link)
                        global_download_summary_data.append({
                            'file_id': file_id_for_log,
                            'filename': 'N/A',
                            'status': 'Incomplete (Interrupted)',
                            'size_bytes': 0,
                            'duration_seconds': 0,
                            'sha256_hash': 'N/A'
                        })
                logger.info("Executor shutdown complete.")
    else:
        logger.info("\nStarting sequential downloads...")
        for task in download_tasks:
            original_link = task[4]
            file_id_for_log = task[0]
            try:
                if args.use_api:
                    success, file_name, total_size, download_start_time, status, sha256_hash_result = \
                        download_file_with_api(file_id_for_log, DOWNLOAD_FOLDER, args.chunk_size)
                else:
                    success, file_name, total_size, download_start_time, status, sha256_hash_result = \
                        download_file_from_google_drive(file_id_for_log, DOWNLOAD_FOLDER, args.retries, args.chunk_size)
                
                end_time = time.time()
                duration = end_time - download_start_time
                avg_speed = (total_size / duration) if duration > 0 else 0

                log_message = (
                    f"Download Finished: ID={file_id_for_log} | Name='{file_name}' | Status: {status} | "
                    f"Duration: {duration:.2f}s | Size: {total_size} bytes | "
                    f"Average Speed: {avg_speed / 1024:.2f} KB/s"
                )
                if success:
                    logger.info(log_message)
                    completed_downloads_count += 1
                else:
                    logger.error(log_message)
                    failed_downloads_count += 1
                    global_failed_original_links.add(original_link)
                
                global_download_summary_data.append({
                    'file_id': file_id_for_log,
                    'filename': file_name,
                    'status': status,
                    'size_bytes': total_size,
                    'duration_seconds': round(duration, 2),
                    'sha256_hash': sha256_hash_result if sha256_hash_result else 'N/A'
                })
            except KeyboardInterrupt:
                logger.warning("KeyboardInterrupt detected. Stopping sequential downloads.")
                global_failed_original_links.add(original_link)
                break
            except Exception as exc:
                logger.critical(f"Sequential download task for {file_id_for_log} (Link: {original_link}) generated an exception: {exc}", exc_info=True)
                failed_downloads_count += 1
                global_failed_original_links.add(original_link)
                global_download_summary_data.append({
                    'file_id': file_id_for_log,
                    'filename': 'N/A',
                    'status': 'Failed (Exception)',
                    'size_bytes': 0,
                    'duration_seconds': round(time.time() - start_total_process_time, 2),
                    'sha256_hash': 'N/A'
                })

    logger.info(f"\n--- Download Summary ---")
    logger.info(f"Total downloads attempted: {len(download_tasks)}")
    logger.info(f"Completed downloads: {completed_downloads_count}")
    logger.info(f"Failed downloads: {failed_downloads_count}")

    if global_download_summary_data:
        summary_csv_path = os.path.join(DOWNLOAD_FOLDER, "download_summary.csv")
        try:
            with open(summary_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['file_id', 'filename', 'status', 'size_bytes', 'duration_seconds', 'sha256_hash']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(global_download_summary_data)
            logger.info(f"Download summary saved to: '{os.path.abspath(summary_csv_path)}'")
        except Exception as e:
            logger.error(f"Error saving download summary to CSV: {e}")

    if global_download_summary_data:
        summary_json_path = os.path.join(DOWNLOAD_FOLDER, "download_summary.json")
        try:
            with open(summary_json_path, 'w', encoding='utf-8') as jsonfile:
                json.dump(global_download_summary_data, jsonfile, indent=4, ensure_ascii=False)
            logger.info(f"Download summary saved to JSON: '{os.path.abspath(summary_json_path)}'")
        except Exception as e:
            logger.error(f"Error saving download summary to JSON: {e}")

    if global_failed_original_links:
        failed_links_path = os.path.join(DOWNLOAD_FOLDER, "failed_downloads.txt")
        try:
            with open(failed_links_path, 'w', encoding='utf-8') as f:
                for link in sorted(list(global_failed_original_links)):
                    f.write(link + "\n")
            logger.info(f"Failed download links saved to: '{os.path.abspath(failed_links_path)}'")
        except Exception as e:
            logger.error(f"Error saving failed download links: {e}")

    if PSUTIL_AVAILABLE:
        process = psutil.Process(os.getpid())
        peak_memory_usage_bytes = process.memory_info().rss
        logger.info(f"Peak memory usage: {peak_memory_usage_bytes / (1024**2):.2f} MB")

    end_total_process_time = time.time()
    total_duration_seconds = end_total_process_time - start_total_process_time
    logger.info(f"\nTotal process time: {total_duration_seconds:.2f} seconds.")
    logger.info(f"\nDownload process finished. Check the log at: '{os.path.abspath(log_file_path)}'")

if __name__ == "__main__":
    main()
