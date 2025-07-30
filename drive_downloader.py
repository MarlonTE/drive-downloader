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

# File handler (rotating log file)
# This path will be updated in main() after DOWNLOAD_FOLDER is ensured to exist
log_file_path = os.path.join(DOWNLOAD_FOLDER, "downloads.log")
file_handler = RotatingFileHandler(log_file_path, maxBytes=5*1024*1024, backupCount=5) # 5MB per file, 5 backups
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logger.warning("psutil not installed. Resource usage hints (memory) will be limited.")


# --- Helper Functions ---

def extract_file_id(url):
    """
    Extracts the FILEID from a Google Drive URL.
    Supports formats like:
    - https://drive.google.com/file/d/FILEID/view
    - https://drive.google.com/open?id=FILEID
    - https://drive.google.com/uc?export=download&id=FILEID
    """
    match = re.search(r'(?:id=|\/d\/)([a-zA-Z0-9_-]+)', url)
    if match:
        return match.group(1)
    return None

def download_file_from_google_drive(file_id, output_path, max_retries, chunk_size):
    """
    Downloads a file from Google Drive, handling large file confirmation,
    exponential retries, and logging download timing and speed.
    """
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    session = requests.Session()
    
    start_time = time.time()
    file_name = f"{file_id}.bin" # Default name, will be updated if Content-Disposition is found
    total_size = 0
    status = "Failed" # Default status

    for retry_count in range(max_retries + 1):
        try:
            # First attempt to get the file.
            # This could be the direct download or the warning page.
            # Added timeout to prevent hanging connections.
            response = session.get(download_url, stream=True, timeout=30)
            response.raise_for_status()  # Raises an exception for bad HTTP status codes

            # --- Detect and handle Google Drive virus scan warning page ---
            # Google returns an HTML warning page for files > 100MB
            # that have not been scanned for viruses, or other error pages.
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
                        return False, file_name, total_size, start_time, status
                    else:
                        # Generic HTML error page, not specifically quota or virus warning
                        logger.warning(f"HTML page detected for {file_id}, but no download form or specific quota/virus warning text was found. This might indicate a change in Google Drive's warning page or an access issue.")
                        raise requests.exceptions.RequestException("Download form not found or unrecognized HTML warning page.")

            # --- Continue with normal download flow ---
            # Get the file name from headers or use the file_id
            if 'Content-Disposition' in response.headers:
                # Attempt to extract the file name from Content-Disposition,
                # which is the preferred method for getting the original name.
                fname_match = re.search(r'filename\*?=UTF-8\'\'(.+)', response.headers['Content-Disposition'])
                if fname_match:
                    file_name = requests.utils.unquote(fname_match.group(1))
                else:
                    fname_match = re.search(r'filename="([^"]+)"', response.headers['Content-Disposition'])
                    if fname_match:
                        file_name = fname_match.group(1)

            full_output_path = os.path.join(output_path, file_name)
            total_size = int(response.headers.get('content-length', 0))

            downloaded_bytes = 0
            last_throughput_check_time = time.time()
            bytes_since_last_check = 0
            low_throughput_start_time = None

            with open(full_output_path, 'wb') as f:
                # Use tqdm to show a download progress bar
                with tqdm(total=total_size, unit='B', unit_scale=True, desc=file_name) as pbar:
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
                                        logger.warning(f"Low throughput detected for '{file_name}' ({current_throughput:.2f} B/s). Download might be slow or stuck.")
                                        # Reset low_throughput_start_time to avoid repeated warnings for the same continuous low throughput
                                        low_throughput_start_time = current_time 
                                else:
                                    low_throughput_start_time = None # Reset if throughput recovers

                                last_throughput_check_time = current_time
                                bytes_since_last_check = 0

            status = "Success"
            return True, file_name, total_size, start_time, status

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
                return False, file_name, total_size, start_time, status
        except Exception as e:
            # Catch any other unexpected exceptions
            status = "Failed (Unexpected Error)"
            logger.critical(f"Unexpected error while downloading {file_id} ('{file_name}'): {e}", exc_info=True)
            return False, file_name, total_size, start_time, status
    
    # This part is reached if all retries fail
    return False, file_name, total_size, start_time, status

# --- Main Function ---

def main():
    parser = argparse.ArgumentParser(description="Google Drive Downloader Script.")
    parser.add_argument("--workers", type=int, default=DEFAULT_MAX_PARALLEL_DOWNLOADS,
                        help=f"Number of parallel download threads (default: {DEFAULT_MAX_PARALLEL_DOWNLOADS}).")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE,
                        help=f"Chunk size in bytes for streaming downloads (default: {DEFAULT_CHUNK_SIZE} bytes).")
    parser.add_argument("--retries", type=int, default=DEFAULT_MAX_RETRIES,
                        help=f"Maximum number of retries per download (default: {DEFAULT_MAX_RETRIES}).")
    args = parser.parse_args()

    # Ensure the download folder exists
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
    
    # Set up logging to the download folder
    log_file_path = os.path.join(DOWNLOAD_FOLDER, "downloads.log")
    for handler in logger.handlers: # Remove existing file handler if any, to re-add with correct path
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
    input_choice = input("Do you want to enter links from a text file (f) or directly here (m)? [f/m]: ").lower()

    if input_choice == 'f':
        file_path = input("Enter the path to the text file with links (one link per line): ")
        try:
            with open(file_path, 'r') as f:
                drive_links = [line.strip() for line in f if line.strip()]
            logger.info(f"Loaded {len(drive_links)} links from '{file_path}'.")
        except FileNotFoundError:
            logger.error(f"Error: The file '{file_path}' was not found.")
            return
    elif input_choice == 'm':
        print("Enter Google Drive links (one per line). Press Enter twice to finish:")
        while True:
            link = input()
            if not link:
                break
            drive_links.append(link.strip())
        logger.info(f"Entered {len(drive_links)} links.")
    else:
        logger.error("Invalid option. Exiting.")
        return

    if not drive_links:
        logger.info("No links provided for download. Exiting.")
        return

    # Process links
    download_tasks = []
    for link in drive_links:
        file_id = extract_file_id(link)
        if file_id:
            download_tasks.append((file_id, DOWNLOAD_FOLDER, args.retries, args.chunk_size))
        else:
            logger.warning(f"Could not extract FILEID from URL: {link}. It will be skipped.")

    if not download_tasks:
        logger.info("No valid FILEIDs found for download. Exiting.")
        return

    # Download in parallel if configured
    completed_downloads = []
    failed_downloads = []

    if args.workers > 1:
        logger.info(f"\nStarting parallel downloads ({args.workers} threads)...")
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_file_id = {executor.submit(download_file_from_google_drive, *task): task[0] for task in download_tasks}
            try:
                for future in as_completed(future_to_file_id):
                    file_id = future_to_file_id[future]
                    try:
                        success, file_name, total_size, download_start_time, status = future.result()
                        end_time = time.time()
                        duration = end_time - download_start_time
                        avg_speed = (total_size / duration) if duration > 0 else 0 # bytes/sec

                        log_message = (
                            f"Download Finished: '{file_name}' | Status: {status} | "
                            f"Duration: {duration:.2f}s | Size: {total_size} bytes | "
                            f"Avg Speed: {avg_speed / 1024:.2f} KB/s"
                        )
                        if success:
                            logger.info(log_message)
                            completed_downloads.append(file_name)
                        else:
                            logger.error(log_message)
                            failed_downloads.append(file_name)
                    except Exception as exc:
                        logger.critical(f"Task for {file_id} generated an exception: {exc}", exc_info=True)
                        failed_downloads.append(file_id) # Log as failed if exception during result retrieval
            except KeyboardInterrupt:
                logger.warning("KeyboardInterrupt detected. Shutting down executor gracefully...")
                executor.shutdown(wait=False, cancel_futures=True) # Cancel remaining futures
                # Collect results for futures that completed before shutdown
                for future in future_to_file_id:
                    if future.done() and not future.cancelled():
                        try:
                            success, file_name, total_size, download_start_time, status = future.result()
                            end_time = time.time()
                            duration = end_time - download_start_time
                            avg_speed = (total_size / duration) if duration > 0 else 0

                            log_message = (
                                f"Download Finished (Interrupted): '{file_name}' | Status: {status} | "
                                f"Duration: {duration:.2f}s | Size: {total_size} bytes | "
                                f"Avg Speed: {avg_speed / 1024:.2f} KB/s"
                            )
                            if success:
                                logger.info(log_message)
                                completed_downloads.append(file_name)
                            else:
                                logger.error(log_message)
                                failed_downloads.append(file_name)
                        except Exception as exc:
                            logger.critical(f"Interrupted task for {file_id} generated an exception: {exc}", exc_info=True)
                            failed_downloads.append(file_id)
                    elif future.cancelled():
                        logger.info(f"Download for {future_to_file_id[future]} was cancelled due to interruption.")
                        failed_downloads.append(future_to_file_id[future]) # Consider cancelled as failed for summary
                    elif not future.done():
                        logger.info(f"Download for {future_to_file_id[future]} was still running and not completed.")
                        failed_downloads.append(future_to_file_id[future]) # Consider incomplete as failed for summary
                logger.info("Executor shutdown complete.")
    else:
        logger.info("\nStarting sequential downloads...")
        for task in download_tasks:
            try:
                success, file_name, total_size, download_start_time, status = download_file_from_google_drive(*task)
                end_time = time.time()
                duration = end_time - download_start_time
                avg_speed = (total_size / duration) if duration > 0 else 0

                log_message = (
                    f"Download Finished: '{file_name}' | Status: {status} | "
                    f"Duration: {duration:.2f}s | Size: {total_size} bytes | "
                    f"Avg Speed: {avg_speed / 1024:.2f} KB/s"
                )
                if success:
                    logger.info(log_message)
                    completed_downloads.append(file_name)
                else:
                    logger.error(log_message)
                    failed_downloads.append(file_name)
            except KeyboardInterrupt:
                logger.warning("KeyboardInterrupt detected. Stopping sequential downloads.")
                break # Exit the loop
            except Exception as exc:
                logger.critical(f"Sequential download task generated an exception: {exc}", exc_info=True)
                failed_downloads.append(task[0]) # Use file_id as file_name might not be set yet

    logger.info(f"\n--- Download Summary ---")
    logger.info(f"Total downloads attempted: {len(download_tasks)}")
    logger.info(f"Completed downloads: {len(completed_downloads)}")
    for f_name in completed_downloads:
        logger.info(f"  - {f_name}")
    logger.info(f"Failed downloads: {len(failed_downloads)}")
    for f_name in failed_downloads:
        logger.error(f"  - {f_name}")

    if PSUTIL_AVAILABLE:
        process = psutil.Process(os.getpid())
        peak_memory_usage_bytes = process.memory_info().rss
        logger.info(f"Peak memory usage: {peak_memory_usage_bytes / (1024**2):.2f} MB")

    logger.info(f"\nDownload process finished. Check the log at: '{os.path.abspath(log_file_path)}'")

if __name__ == "__main__":
    main()
