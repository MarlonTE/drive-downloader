import re
import os
import time
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup # Import BeautifulSoup for HTML parsing
import argparse
import logging
from logging.handlers import RotatingFileHandler
import hashlib # For SHA256 hash calculation
import csv # For CSV summary output
import json # For JSON summary output (alternative)

# --- Configuration ---
DOWNLOAD_FOLDER = "drive_downloads"  # Folder where files will be saved
DEFAULT_MAX_RETRIES = 3             # Default maximum number of retries per download
RETRY_BACKOFF_FACTOR = 1            # Factor for exponential backoff (1s, 2s, 4s, etc.)
DEFAULT_MAX_PARALLEL_DOWNLOADS = 4  # Default maximum number of concurrent downloads
DEFAULT_CHUNK_SIZE = 32768          # Default chunk size for streaming downloads (bytes)
MIN_THROUGHPUT_THRESHOLD = 10 * 1024 # Minimum throughput threshold for warning (10 KB/s)
LOW_THROUGHPUT_TIMEOUT = 30         # Time in seconds before logging low throughput warning

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
logger.addHandler(console_handler)

# --- Check for psutil availability and define PSUTIL_AVAILABLE globally ---
# This block ensures PSUTIL_AVAILABLE is always defined, preventing NameError.
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logger.warning("psutil not installed. Resource usage hints (memory) will be limited.")


# --- Helper Functions ---

def extract_file_id(url: str) -> str | None:
    """
    Extracts the FILEID from a Google Drive URL.

    Args:
        url (str): The Google Drive URL.

    Returns:
        str | None: The extracted FILEID if found, otherwise None.
    """
    match = re.search(r'(?:id=|\/d\/)([a-zA-Z0-9_-]+)', url)
    if match:
        return match.group(1)
    return None

def is_valid_drive_link(url: str) -> bool:
    """
    Performs a basic validation to check if the URL looks like a Google Drive link.

    Args:
        url (str): The URL to validate.

    Returns:
        bool: True if the URL matches a known Google Drive pattern, False otherwise.
    """
    # Patterns for common Google Drive share/download links
    drive_patterns = [
        r'https?:\/\/drive\.google\.com\/file\/d\/[a-zA-Z0-9_-]+\/view',
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
        chunk_size (int): The size of chunks to read the file in.

    Returns:
        str | None: The SHA256 hash of the file in hexadecimal format, or None if an error occurs.
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

def download_file_from_google_drive(file_id: str, output_path: str, max_retries: int, chunk_size: int) -> tuple[bool, str, int, float, str, str | None]:
    """
    Downloads a file from Google Drive, handling large file confirmation,
    exponential retries, and logging download timing and speed.

    Args:
        file_id (str): The ID of the Google Drive file.
        output_path (str): The directory where the file will be saved.
        max_retries (int): Maximum number of retry attempts.
        chunk_size (int): Size of chunks for streaming download.

    Returns:
        tuple[bool, str, int, float, str, str | None]: A tuple containing:
            - bool: True if download was successful or skipped, False otherwise.
            - str: The determined file name.
            - int: Total size of the file in bytes.
            - float: Start time of the download process (time.time()).
            - str: Status of the download ("Success", "Skipped", "Failed", "Failed (Quota Exceeded)", etc.).
            - str | None: SHA256 hash of the downloaded file, or None if not calculated/failed.
    """
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    session = requests.Session()
    
    start_time = time.time()
    file_name = f"{file_id}.bin" # Default name, will be updated if Content-Disposition is found
    total_size = 0
    status = "Failed" # Default status for early exit scenarios
    sha256_hash_result = None

    for retry_count in range(max_retries + 1):
        try:
            # First attempt to get the file.
            # This could be the direct download or the warning page.
            # Added timeout to prevent hanging connections.
            response = session.get(download_url, stream=True, timeout=30)
            response.raise_for_status()  # Raises an exception for bad HTTP status codes

            # --- Detect and handle Google Drive HTML responses (warning page or error) ---
            if 'Content-Type' in response.headers and 'text/html' in response.headers['Content-Type']:
                logger.info(f"Detected Google Drive HTML response for {file_id}. Checking for confirmation form or error messages.")
                
                soup = BeautifulSoup(response.text, 'html.parser')
                download_form = soup.find('form', {'id': 'download-form'})

                if download_form:
                    # If a download form is found, it's the large file confirmation page.
                    # Extract the action URL from the form, which will be the real download endpoint
                    action_url = download_form.get('action')
                    if not action_url.startswith('http'): # Ensure it's an absolute URL
                        action_url = f"https://drive.google.com{action_url}"

                    # Collect all hidden input fields from the form
                    params = {}
                    hidden_inputs = download_form.find_all('input', {'type': 'hidden'})
                    for input_tag in hidden_inputs:
                        name = input_tag.get('name')
                        value = input_tag.get('value')
                        if name and value:
                            params[name] = value

                    logger.info(f"Confirmation form found. Retrying download with form parameters for {file_id}.")
                    
                    # Make the second GET request to the action endpoint with all hidden parameters.
                    # This simulates submitting the form and should initiate the binary download.
                    response = session.get(action_url, params=params, stream=True, timeout=30)
                    response.raise_for_status()
                else:
                    # If HTML is detected but the form is not found, check for specific error messages.
                    # This implies it's likely a direct error page, not a confirmation page.
                    page_text = response.text.lower()
                    quota_keywords = ["quota exceeded", "limit exceeded", "excess traffic", "download limit", "virus scan warning"]
                    is_quota_error = any(keyword in page_text for keyword in quota_keywords)

                    if is_quota_error:
                        status = "Failed (Quota Exceeded)"
                        error_message = (
                            f"[ERROR - QUOTA EXCEEDED] Could not download file with ID: {file_id}\n"
                            f"➤ Reason: Google Drive has temporarily blocked the download due to excessive traffic or virus scan warning.\n"
                            f"➤ Solution: Make a copy of the file to your personal Google Drive account and use its new ID for download.\n"
                            f"➤ Note: If a .bin file was created, it is NOT the original file, but an automatic error response from Google."
                        )
                        logger.error(error_message)
                        # Do not proceed with saving the .bin file in this case
                        return False, file_name, total_size, start_time, status, sha256_hash_result
                    else:
                        # Generic HTML error page, not specifically quota or virus warning
                        logger.warning(f"HTML page detected for {file_id}, but no download form or specific quota/virus warning text was found. This might indicate a change in Google Drive's warning page or an access issue.")
                        raise requests.exceptions.RequestException("Download form not found or unrecognized HTML warning page.")

            # --- Determine file name before checking for existence ---
            # Attempt to extract the file name from Content-Disposition,
            # which is the preferred method for getting the original name.
            if 'Content-Disposition' in response.headers:
                fname_match = re.search(r'filename\*?=UTF-8\'\'(.+)', response.headers['Content-Disposition'])
                if fname_match:
                    file_name = requests.utils.unquote(fname_match.group(1))
                else:
                    fname_match = re.search(r'filename="([^"]+)"', response.headers['Content-Disposition'])
                    if fname_match:
                        file_name = fname_match.group(1)
            
            # Use a sanitized file name for path construction
            # Remove characters that are not alphanumeric, '.', '_', or '-'
            sanitized_file_name = "".join([c for c in file_name if c.isalnum() or c in ('.', '_', '-')])
            if not sanitized_file_name: # Fallback if sanitization makes it empty
                sanitized_file_name = file_name # Use original if sanitized is empty, might cause issues but better than no name
            
            full_output_path = os.path.join(output_path, sanitized_file_name)

            # --- Check if file already exists and skip if so ---
            if os.path.exists(full_output_path) and os.path.getsize(full_output_path) > 0:
                status = "Skipped"
                # If skipped, try to calculate SHA256 from existing file
                sha256_hash_result = calculate_sha256(full_output_path, chunk_size)
                logger.info(f"File '{sanitized_file_name}' already exists in '{output_path}'. Skipping download.")
                return True, sanitized_file_name, os.path.getsize(full_output_path), start_time, status, sha256_hash_result

            # --- Continue with normal download flow ---
            total_size = int(response.headers.get('content-length', 0))

            downloaded_bytes = 0
            last_throughput_check_time = time.time()
            bytes_since_last_check = 0
            low_throughput_start_time = None

            with open(full_output_path, 'wb') as f:
                # Use tqdm to show a download progress bar
                with tqdm(total=total_size, unit='B', unit_scale=True, desc=sanitized_file_name) as pbar:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
                            downloaded_bytes += len(chunk)
                            bytes_since_last_check += len(chunk)

                            current_time = time.time()
                            # Check throughput every few seconds
                            if current_time - last_throughput_check_time >= 5: # Check every 5 seconds
                                elapsed_check_time = current_time - last_throughput_check_time
                                current_throughput = bytes_since_last_check / elapsed_check_time if elapsed_check_time > 0 else 0

                                if current_throughput < MIN_THROUGHPUT_THRESHOLD:
                                    if low_throughput_start_time is None:
                                        low_throughput_start_time = current_time
                                    elif current_time - low_throughput_start_time >= LOW_THROUGHPUT_TIMEOUT:
                                        logger.warning(f"Low throughput detected for '{sanitized_file_name}' ({current_throughput:.2f} B/s). Download might be slow or stuck.")
                                        # Reset low_throughput_start_time to avoid repeated warnings for the same continuous low throughput
                                        low_throughput_start_time = current_time 
                                else:
                                    low_throughput_start_time = None # Reset if throughput recovers

                                last_throughput_check_time = current_time
                                bytes_since_last_check = 0
            
            # --- Calculate SHA256 hash after successful download ---
            sha256_hash_result = calculate_sha256(full_output_path, chunk_size)
            if sha256_hash_result:
                sha256_file_path = f"{full_output_path}.sha256"
                with open(sha256_file_path, 'w') as f:
                    f.write(sha256_hash_result)
                logger.info(f"SHA256 hash calculated and saved for '{sanitized_file_name}': {sha256_hash_result}")

            status = "Success"
            return True, sanitized_file_name, total_size, start_time, status, sha256_hash_result

        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            # Handle network errors, HTTP errors, and timeouts with exponential retries
            logger.error(f"Error on attempt {retry_count + 1} for {file_id} ('{file_name}'): {e}")
            if retry_count < max_retries:
                wait_time = RETRY_BACKOFF_FACTOR * (2 ** retry_count)
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                status = "Failed (Max Retries)"
                logger.error(f"Final failure: Could not download {file_id} ('{file_name}') after {max_retries} retries.")
                return False, file_name, total_size, start_time, status, sha256_hash_result
        except Exception as e:
            # Catch any other unexpected exceptions
            status = "Failed (Unexpected Error)"
            logger.critical(f"Unexpected error while downloading {file_id} ('{file_name}'): {e}", exc_info=True)
            return False, file_name, total_size, start_time, status, sha256_hash_result
    
    # This part is reached if all retries fail
    return False, file_name, total_size, start_time, status, sha256_hash_result

# --- Main Function ---

def main():
    """
    Main function to parse arguments, read links, and manage the download process.
    Handles parallel downloads, logging, and graceful shutdown.
    """
    start_total_process_time = time.time() # Start total process timer

    parser = argparse.ArgumentParser(description="Google Drive Downloader Script.")
    parser.add_argument("--workers", type=int, default=DEFAULT_MAX_PARALLEL_DOWNLOADS,
                        help=f"Number of parallel download threads (default: {DEFAULT_MAX_PARALLEL_DOWNLOADS}).")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE,
                        help=f"Chunk size in bytes for streaming downloads (default: {DEFAULT_CHUNK_SIZE} bytes).")
    parser.add_argument("--retries", type=int, default=DEFAULT_MAX_RETRIES,
                        help=f"Maximum number of retries per download (default: {DEFAULT_MAX_RETRIES}).")
    parser.add_argument("--input-file", type=str,
                        help="Path to a text file containing Google Drive links (one link per line).")
    args = parser.parse_args()

    # --- Ensure the download folder exists before configuring the logger ---
    # This prevents FileNotFoundError when setting up RotatingFileHandler if the folder doesn't exist.
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
    
    # Reconfigure file handler with the correct path after folder creation
    log_file_path = os.path.join(DOWNLOAD_FOLDER, "downloads.log")
    for handler in logger.handlers:
        if isinstance(handler, RotatingFileHandler):
            logger.removeHandler(handler)
    file_handler = RotatingFileHandler(log_file_path, maxBytes=5*1024*1024, backupCount=5)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)

    logger.info("--- Starting Google Drive Downloader ---")
    logger.info(f"Files will be saved in: '{os.path.abspath(DOWNLOAD_FOLDER)}'")
    logger.info(f"Configured for {args.workers} parallel downloads.")
    logger.info(f"Using chunk size: {args.chunk_size} bytes.")
    logger.info(f"Max retries per file: {args.retries}.")

    # Log resource usage hints
    logger.info(f"System CPU cores detected: {os.cpu_count()}")
    if PSUTIL_AVAILABLE:
        total_memory_bytes = psutil.virtual_memory().total
        logger.info(f"Total system memory: {total_memory_bytes / (1024**3):.2f} GB")
    
    # Read links from a text file or in-memory list
    drive_links = []
    if args.input_file:
        file_path = args.input_file
        # Validate input file existence and readability
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

    # Process links and validate FILEIDs
    download_tasks = []
    valid_links_found = False
    failed_original_links = set() # Set to store unique failed original links

    for link in drive_links:
        if not is_valid_drive_link(link):
            logger.warning(f"Invalid Google Drive link format: {link}. It will be skipped.")
            failed_original_links.add(link) # Add invalid links to failed list
            continue

        file_id = extract_file_id(link)
        if file_id:
            download_tasks.append((file_id, DOWNLOAD_FOLDER, args.retries, args.chunk_size, link)) # Pass original link
            valid_links_found = True
        else:
            logger.warning(f"Could not extract FILEID from URL: {link}. It will be skipped.")
            failed_original_links.add(link) # Add links with unextractable IDs to failed list

    if not valid_links_found:
        logger.error("No valid Google Drive FILEIDs found in the provided input. Exiting.")
        return

    # Download in parallel if configured
    completed_downloads = []
    failed_downloads = []
    download_summary_data = [] # List to store data for CSV/JSON summary

    if args.workers > 1:
        logger.info(f"\nStarting parallel downloads ({args.workers} threads)...")
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            # Map file_id to original link for better logging on exceptions
            future_to_task = {executor.submit(download_file_from_google_drive, task[0], task[1], task[2], task[3]): task for task in download_tasks}
            try:
                for future in as_completed(future_to_task):
                    original_link = future_to_task[future][4] # Retrieve original link
                    file_id_for_log = future_to_task[future][0]
                    try:
                        success, file_name, total_size, download_start_time, status, sha256_hash_result = future.result()
                        end_time = time.time()
                        duration = end_time - download_start_time
                        avg_speed = (total_size / duration) if duration > 0 else 0 # bytes/sec

                        log_message = (
                            f"Download Finished: ID={file_id_for_log} | Name='{file_name}' | Status: {status} | "
                            f"Duration: {duration:.2f}s | Size: {total_size} bytes | "
                            f"Avg Speed: {avg_speed / 1024:.2f} KB/s"
                        )
                        if success:
                            logger.info(log_message)
                            completed_downloads.append(file_name)
                        else:
                            logger.error(log_message)
                            failed_downloads.append(file_name)
                            failed_original_links.add(original_link) # Add to failed links set
                        
                        download_summary_data.append({
                            'file_id': file_id_for_log,
                            'filename': file_name,
                            'status': status,
                            'size_bytes': total_size,
                            'duration_seconds': round(duration, 2),
                            'sha256_hash': sha256_hash_result if sha256_hash_result else 'N/A'
                        })
                    except Exception as exc:
                        logger.critical(f"Task for {file_id_for_log} (Link: {original_link}) generated an exception: {exc}", exc_info=True)
                        failed_downloads.append(file_id_for_log) # Log as failed if exception during result retrieval
                        failed_original_links.add(original_link) # Add to failed links set
                        download_summary_data.append({
                            'file_id': file_id_for_log,
                            'filename': 'N/A', # File name might not be determined on exception
                            'status': 'Failed (Exception)',
                            'size_bytes': 0,
                            'duration_seconds': round(time.time() - download_start_time, 2),
                            'sha256_hash': 'N/A'
                        })
            except KeyboardInterrupt:
                logger.warning("KeyboardInterrupt detected. Shutting down executor gracefully...")
                executor.shutdown(wait=False, cancel_futures=True) # Cancel remaining futures
                # Collect results for futures that completed before shutdown
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
                                f"Avg Speed: {avg_speed / 1024:.2f} KB/s"
                            )
                            if success:
                                logger.info(log_message)
                                completed_downloads.append(file_name)
                            else:
                                logger.error(log_message)
                                failed_downloads.append(file_name)
                                failed_original_links.add(original_link)
                            download_summary_data.append({
                                'file_id': file_id_for_log,
                                'filename': file_name,
                                'status': status,
                                'size_bytes': total_size,
                                'duration_seconds': round(duration, 2),
                                'sha256_hash': sha256_hash_result if sha256_hash_result else 'N/A'
                            })
                        except Exception as exc:
                            logger.critical(f"Interrupted task for {file_id_for_log} (Link: {original_link}) generated an exception: {exc}", exc_info=True)
                            failed_downloads.append(file_id_for_log)
                            failed_original_links.add(original_link)
                            download_summary_data.append({
                                'file_id': file_id_for_log,
                                'filename': 'N/A',
                                'status': 'Failed (Interrupted Exception)',
                                'size_bytes': 0,
                                'duration_seconds': round(time.time() - download_start_time, 2),
                                'sha256_hash': 'N/A'
                            })
                    elif future.cancelled():
                        logger.info(f"Download for {file_id_for_log} (Link: {original_link}) was cancelled due to interruption.")
                        failed_downloads.append(file_id_for_log) # Consider cancelled as failed for summary
                        failed_original_links.add(original_link)
                        download_summary_data.append({
                            'file_id': file_id_for_log,
                            'filename': 'N/A',
                            'status': 'Cancelled',
                            'size_bytes': 0,
                            'duration_seconds': 0,
                            'sha256_hash': 'N/A'
                        })
                    elif not future.done():
                        logger.info(f"Download for {file_id_for_log} (Link: {original_link}) was still running and not completed.")
                        failed_downloads.append(file_id_for_log) # Consider incomplete as failed for summary
                        failed_original_links.add(original_link)
                        download_summary_data.append({
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
                success, file_name, total_size, download_start_time, status, sha256_hash_result = download_file_from_google_drive(*task)
                end_time = time.time()
                duration = end_time - download_start_time
                avg_speed = (total_size / duration) if duration > 0 else 0

                log_message = (
                    f"Download Finished: ID={file_id_for_log} | Name='{file_name}' | Status: {status} | "
                    f"Duration: {duration:.2f}s | Size: {total_size} bytes | "
                    f"Avg Speed: {avg_speed / 1024:.2f} KB/s"
                )
                if success:
                    logger.info(log_message)
                    completed_downloads.append(file_name)
                else:
                    logger.error(log_message)
                    failed_downloads.append(file_name)
                    failed_original_links.add(original_link)
                
                download_summary_data.append({
                    'file_id': file_id_for_log,
                    'filename': file_name,
                    'status': status,
                    'size_bytes': total_size,
                    'duration_seconds': round(duration, 2),
                    'sha256_hash': sha256_hash_result if sha256_hash_result else 'N/A'
                })
            except KeyboardInterrupt:
                logger.warning("KeyboardInterrupt detected. Stopping sequential downloads.")
                failed_original_links.add(original_link) # Add current link to failed
                break # Exit the loop
            except Exception as exc:
                logger.critical(f"Sequential download task for {file_id_for_log} (Link: {original_link}) generated an exception: {exc}", exc_info=True)
                failed_downloads.append(file_id_for_log) # Use file_id as file_name might not be set yet
                failed_original_links.add(original_link)
                download_summary_data.append({
                    'file_id': file_id_for_log,
                    'filename': 'N/A',
                    'status': 'Failed (Exception)',
                    'size_bytes': 0,
                    'duration_seconds': round(time.time() - download_start_time, 2),
                    'sha256_hash': 'N/A'
                })

    logger.info(f"\n--- Download Summary ---")
    logger.info(f"Total downloads attempted: {len(download_tasks)}")
    logger.info(f"Completed downloads: {len(completed_downloads)}")
    for f_name in completed_downloads:
        logger.info(f"  - {f_name}")
    logger.info(f"Failed downloads: {len(failed_downloads)}")
    for f_name in failed_downloads:
        logger.error(f"  - {f_name}")

    # --- Generate Download Summary File (CSV) ---
    if download_summary_data:
        summary_csv_path = os.path.join(DOWNLOAD_FOLDER, "download_summary.csv")
        try:
            with open(summary_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['file_id', 'filename', 'status', 'size_bytes', 'duration_seconds', 'sha256_hash']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(download_summary_data)
            logger.info(f"Download summary saved to: '{os.path.abspath(summary_csv_path)}'")
        except Exception as e:
            logger.error(f"Error saving download summary to CSV: {e}")

    # --- Save Failed Links to a Separate File ---
    if failed_original_links:
        failed_links_path = os.path.join(DOWNLOAD_FOLDER, "failed_downloads.txt")
        try:
            with open(failed_links_path, 'w', encoding='utf-8') as f:
                for link in sorted(list(failed_original_links)): # Sort for consistent output
                    f.write(link + "\n")
            logger.info(f"Failed download links saved to: '{os.path.abspath(failed_links_path)}'")
        except Exception as e:
            logger.error(f"Error saving failed download links: {e}")

    if PSUTIL_AVAILABLE:
        process = psutil.Process(os.getpid())
        peak_memory_usage_bytes = process.memory_info().rss
        logger.info(f"Peak memory usage: {peak_memory_usage_bytes / (1024**2):.2f} MB")

    end_total_process_time = time.time() # End total process timer
    total_duration_seconds = end_total_process_time - start_total_process_time
    logger.info(f"\nTotal process time: {total_duration_seconds:.2f} seconds.")
    logger.info(f"\nDownload process finished. Check the log at: '{os.path.abspath(log_file_path)}'")

if __name__ == "__main__":
    main()
