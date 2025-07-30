import re
import os
import time
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup # Import BeautifulSoup for HTML parsing

# --- Configuration ---
DOWNLOAD_FOLDER = "drive_downloads"  # Folder where files will be saved
MAX_RETRIES = 3                     # Maximum number of retries per download
RETRY_BACKOFF_FACTOR = 1            # Factor for exponential backoff (1s, 2s, 4s, etc.)
MAX_PARALLEL_DOWNLOADS = 4          # Maximum number of concurrent downloads (optional)

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

def download_file_from_google_drive(file_id, output_path):
    """
    Downloads a file from Google Drive, handling large file confirmation
    and exponential retries.
    """
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    session = requests.Session()
    log_messages = []

    for retry_count in range(MAX_RETRIES + 1):
        try:
            # First attempt to get the file.
            # This could be the direct download or the warning page.
            response = session.get(download_url, stream=True)
            response.raise_for_status()  # Raises an exception for bad HTTP status codes

            # --- Detect and handle Google Drive virus scan warning page ---
            # Google returns an HTML warning page for files > 100MB
            # that have not been scanned for viruses.
            if 'Content-Type' in response.headers and 'text/html' in response.headers['Content-Type']:
                log_messages.append(f"Detected Google Drive warning page for {file_id}. Attempting to extract confirmation form.")
                
                # Parse the HTML content to find the confirmation form
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Look for the form with id="download-form"
                download_form = soup.find('form', {'id': 'download-form'})

                if download_form:
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

                    log_messages.append(f"Confirmation form found. Retrying download with form parameters for {file_id}.")
                    
                    # Make the second GET request to the action endpoint with all hidden parameters.
                    # This simulates submitting the form and should initiate the binary download.
                    response = session.get(action_url, params=params, stream=True)
                    response.raise_for_status()
                else:
                    # If HTML is detected but the form is not found, it's an unexpected failure
                    log_messages.append(f"Warning: HTML page detected for {file_id}, but the download form was not found. This might indicate a change in Google Drive's warning page or an access issue.")
                    raise requests.exceptions.RequestException("Download form not found on the warning page.")

            # --- Continue with normal download flow ---
            # Get the file name from headers or use the file_id
            file_name = None
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

            if not file_name:
                file_name = f"{file_id}.bin" # Default name if extraction fails

            full_output_path = os.path.join(output_path, file_name)
            total_size = int(response.headers.get('content-length', 0))

            with open(full_output_path, 'wb') as f:
                # Use tqdm to show a download progress bar
                with tqdm(total=total_size, unit='B', unit_scale=True, desc=file_name) as pbar:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))

            log_messages.append(f"Success: '{file_name}' downloaded to '{full_output_path}'")
            return True, log_messages

        except requests.exceptions.RequestException as e:
            # Handle network or HTTP errors and exponential retries
            log_messages.append(f"Error on attempt {retry_count + 1} for {file_id}: {e}")
            if retry_count < MAX_RETRIES:
                wait_time = RETRY_BACKOFF_FACTOR * (2 ** retry_count)
                log_messages.append(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                log_messages.append(f"Final failure: Could not download {file_id} after {MAX_RETRIES} retries.")
                return False, log_messages
        except Exception as e:
            # Catch any other unexpected exceptions
            log_messages.append(f"Unexpected error while downloading {file_id}: {e}")
            return False, log_messages

# --- Main Function ---

def main():
    print("--- Starting Google Drive Downloader ---")

    # Ensure the download folder exists
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
    print(f"Files will be saved in: '{os.path.abspath(DOWNLOAD_FOLDER)}'")

    # Read links from a text file or in-memory list
    drive_links = []
    input_choice = input("Do you want to enter links from a text file (f) or directly here (m)? [f/m]: ").lower()

    if input_choice == 'f':
        file_path = input("Enter the path to the text file with links (one link per line): ")
        try:
            with open(file_path, 'r') as f:
                drive_links = [line.strip() for line in f if line.strip()]
            print(f"Loaded {len(drive_links)} links from '{file_path}'.")
        except FileNotFoundError:
            print(f"Error: The file '{file_path}' was not found.")
            return
    elif input_choice == 'm':
        print("Enter Google Drive links (one per line). Press Enter twice to finish:")
        while True:
            link = input()
            if not link:
                break
            drive_links.append(link.strip())
        print(f"Entered {len(drive_links)} links.")
    else:
        print("Invalid option. Exiting.")
        return

    if not drive_links:
        print("No links provided for download. Exiting.")
        return

    # Process links
    download_tasks = []
    for link in drive_links:
        file_id = extract_file_id(link)
        if file_id:
            download_tasks.append((file_id, DOWNLOAD_FOLDER))
        else:
            print(f"Warning: Could not extract FILEID from URL: {link}. It will be skipped.")

    if not download_tasks:
        print("No valid FILEIDs found for download. Exiting.")
        return

    # Download in parallel if configured
    log_results = []
    if MAX_PARALLEL_DOWNLOADS > 1:
        print(f"\nStarting parallel downloads ({MAX_PARALLEL_DOWNLOADS} threads/processes)...")
        with ThreadPoolExecutor(max_workers=MAX_PARALLEL_DOWNLOADS) as executor:
            future_to_file_id = {executor.submit(download_file_from_google_drive, file_id, output_path): file_id for file_id, output_path in download_tasks}
            for future in as_completed(future_to_file_id):
                file_id = future_to_file_id[future]
                success, messages = future.result()
                log_results.extend(messages)
    else:
        print("\nStarting sequential downloads...")
        for file_id, output_path in download_tasks:
            success, messages = download_file_from_google_drive(file_id, output_path)
            log_results.extend(messages)

    # --- Log Results ---
    log_file_path = os.path.join(DOWNLOAD_FOLDER, "downloads_log.txt")
    with open(log_file_path, "w") as f:
        for msg in log_results:
            f.write(msg + "\n")
            print(msg) # Also prints to console

    print(f"\nDownload process finished. Check the log at: '{os.path.abspath(log_file_path)}'")

if __name__ == "__main__":
    main()
