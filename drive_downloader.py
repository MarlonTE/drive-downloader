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
    match = re.search(r'(?:id=|\/d\/)([a-zA-Z0-9_-]+)', url)
    if match:
        return match.group(1)
    return None

def is_valid_drive_link(url: str) -> bool:
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
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(chunk_size), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception as e:
        logger.error(f"Error al calcular SHA256 para {file_path}: {e}")
        return None

def get_google_drive_api_credentials():
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
                return None
            except requests.exceptions.RequestException:
                return None
            except Exception:
                return None
        
        if creds and creds.valid:
            try:
                with open('token.json', 'w') as token:
                    token.write(creds.to_json())
            except Exception:
                pass
    
    return creds

def download_file_with_api(file_id: str, output_path: str, chunk_size: int) -> tuple[bool, str, int, float, str, str | None]:
    start_time = time.time()
    file_name = f"{file_id}_api.bin"
    total_size = 0
    status = "Failed"
    sha256_hash_result = None

    try:
        creds = get_google_drive_api_credentials()
        if not creds:
            status = "Failed (Auth Error)"
            return False, file_name, total_size, start_time, status, sha256_hash_result

        service = build('drive', 'v3', credentials=creds)

        file_metadata = service.files().get(fileId=file_id, fields='name,size').execute()
        file_name = file_metadata.get('name', file_name)
        total_size = int(file_metadata.get('size', 0))

        sanitized_file_name = "".join([c for c in file_name if c.isalnum() or c in ('.', '_', '-')])
        if not sanitized_file_name:
            sanitized_file_name = file_name

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
        file_name = unique_file_name

        downloaded_bytes = 0
        if os.path.exists(full_output_path):
            downloaded_bytes = os.path.getsize(full_output_path)
            if downloaded_bytes == total_size and total_size > 0:
                status = "Skipped"
                sha256_hash_result = calculate_sha256(full_output_path, chunk_size)
                logger.info(f"Archivo '{file_name}' ya descargado completamente a través de la API. Omitiendo.")
                return True, file_name, total_size, start_time, status, sha256_hash_result
            elif downloaded_bytes > 0:
                logger.info(f"Reanudando descarga para '{file_name}' desde {downloaded_bytes} bytes (API).")
            else:
                downloaded_bytes = 0

        request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(full_output_path, 'ab') 
        downloader = MediaIoBaseDownload(fh, request, chunksize=chunk_size)

        done = False
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=file_name, initial=downloaded_bytes) as pbar:
            while not done:
                status_api, done = downloader.next_chunk()
                if status_api:
                    pbar.update(status_api.resumable_progress - pbar.n) 
        
        fh.close()

        sha256_hash_result = calculate_sha256(full_output_path, chunk_size)
        if sha256_hash_result:
            sha256_file_path = f"{full_output_path}.sha256"
            with open(sha256_file_path, 'w') as f:
                    f.write(sha256_hash_result)
            logger.info(f"SHA256 hash calculado y guardado para '{file_name}': {sha256_hash_result}")

        status = "Success"
        return True, file_name, total_size, start_time, status, sha256_hash_result

    except Exception as e:
        status = "Failed (API Error)"
        logger.error(f"Error al descargar el archivo {file_id} con la API de Google Drive: {e}", exc_info=True)
        return False, file_name, total_size, start_time, status, sha256_hash_result

def download_file_from_google_drive(file_id: str, output_path: str, max_retries: int, chunk_size: int) -> tuple[bool, str, int, float, str, str | None]:
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    session = requests.Session()
    
    start_time = time.time()
    file_name = f"{file_id}.bin"
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
            pre_flight_response = session.get(pre_flight_url, stream=True, timeout=15)
            pre_flight_response.raise_for_status()

            if 'Content-Type' in pre_flight_response.headers and 'text/html' in pre_flight_response.headers['Content-Type']:
                logger.info(f"Se detectó respuesta HTML de Google Drive para {file_id} durante el pre-vuelo. Verificando formulario de confirmación o mensajes de error.")
                
                soup = BeautifulSoup(pre_flight_response.text, 'html.parser')
                download_form = soup.find('form', {'id': 'download-form'})

                if download_form:
                    action_url = download_form.get('action')
                    if not action_url.startswith('http'):
                        action_url = f"https://drive.google.com{action_url}"
                    
                    pre_flight_params = {input_tag.get('name'): input_tag.get('value') for input_tag in download_form.find_all('input', {'type': 'hidden'}) if input_tag.get('name') and input_tag.get('value')}
                    
                    logger.info(f"Formulario de confirmación encontrado. Se usarán los parámetros del formulario para {file_id}.")

                    head_response_after_form = session.head(action_url, params=pre_flight_params, allow_redirects=True, timeout=10)
                    head_response_after_form.raise_for_status()
                    target_response_headers = head_response_after_form.headers
                    download_url = action_url
                else:
                    page_text = pre_flight_response.text.lower()
                    quota_keywords = ["quota exceeded", "limit exceeded", "excess traffic", "download limit", "virus scan warning"]
                    is_quota_error = any(keyword in page_text for keyword in quota_keywords)

                    if is_quota_error:
                        status = "Failed (Quota Exceeded)"
                        error_message = (
                            f"[ERROR - CUOTA EXCEDIDA] No se pudo descargar el archivo con ID: {file_id}\n"
                            f"➤ Razón: Google Drive ha bloqueado temporalmente la descarga debido a tráfico excesivo o advertencia de escaneo de virus.\n"
                            f"➤ Solución: Haz una copia del archivo en tu cuenta personal de Google Drive y usa su nuevo ID para la descarga.\n"
                            f"➤ Nota: Si se creó un archivo .bin, NO es el archivo original, sino una respuesta de error automática de Google."
                        )
                        logger.error(error_message)
                        return False, file_name, total_file_size_expected, start_time, status, sha256_hash_result
                    else:
                        status = "Failed (Unrecognized HTML Page)"
                        return False, file_name, total_file_size_expected, start_time, status, sha256_hash_result
            else:
                target_response_headers = pre_flight_response.headers
                for _ in pre_flight_response.iter_content(chunk_size=chunk_size):
                    pass
                pre_flight_response.close()

            if 'Content-Disposition' in target_response_headers:
                fname_match = re.search(r'filename\*?=UTF-8\'\'(.+)', target_response_headers['Content-Disposition'])
                if fname_match:
                    file_name = requests.utils.unquote(fname_match.group(1))
                else:
                    fname_match = re.search(r'filename="([^"]+)"', target_response_headers['Content-Disposition'])
                    if fname_match:
                        file_name = fname_match.group(1)
            
            sanitized_file_name = "".join([c for c in file_name if c.isalnum() or c in ('.', '_', '-')])
            if not sanitized_file_name:
                sanitized_file_name = file_name
            
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
            file_name = unique_file_name

            total_file_size_expected = int(target_response_headers.get('content-length', 0))

            file_mode = 'wb'
            if os.path.exists(full_output_path):
                current_downloaded_bytes = os.path.getsize(full_output_path)
                if current_downloaded_bytes == total_file_size_expected and total_file_size_expected > 0:
                    status = "Skipped"
                    sha256_hash_result = calculate_sha256(full_output_path, chunk_size)
                    logger.info(f"Archivo '{file_name}' ya descargado completamente. Omitiendo.")
                    return True, file_name, total_file_size_expected, start_time, status, sha256_hash_result
                elif current_downloaded_bytes > 0:
                    logger.info(f"Reanudando descarga para '{file_name}' desde {current_downloaded_bytes} bytes.")
                    headers['Range'] = f'bytes={current_downloaded_bytes}-'
                    file_mode = 'ab'
                else:
                    current_downloaded_bytes = 0
                    file_mode = 'wb'
                    logger.info(f"Archivo '{file_name}' existe pero está vacío/corrupto. Reiniciando descarga.")

        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            logger.error(f"Error durante el pre-vuelo o la solicitud inicial para {file_id} ('{file_name}'): {e}")
            if retry_count < max_retries:
                wait_time = RETRY_BACKOFF_FACTOR * (2 ** retry_count)
                logger.info(f"Reintentando en {wait_time} segundos...")
                time.sleep(wait_time)
                continue
            else:
                status = "Failed (Pre-vuelo/Error de solicitud inicial)"
                logger.error(f"Fallo final: No se pudo obtener metadatos o manejar la confirmación para {file_id} ('{file_name}') después de {max_retries} reintentos.")
                return False, file_name, total_file_size_expected, start_time, status, sha256_hash_result
        except Exception as e:
            status = "Failed (Error inesperado durante el pre-vuelo)"
            logger.critical(f"Error inesperado durante el pre-vuelo para {file_id} ('{file_name}'): {e}", exc_info=True)
            return False, file_name, total_file_size_expected, start_time, status, sha256_hash_result

        try:
            response = session.get(download_url, params=pre_flight_params, stream=True, headers=headers, timeout=60)
            response.raise_for_status()

            if current_downloaded_bytes > 0 and response.status_code == 200:
                logger.warning(f"El servidor NO honró el encabezado Range para '{file_name}'. Reiniciando descarga desde el principio.")
                current_downloaded_bytes = 0
                file_mode = 'wb'

            if 'Content-Type' in response.headers and 'text/html' in response.headers['Content-Type']:
                logger.error(f"¡Error crítico! Se recibió contenido HTML inesperado durante la descarga principal para el archivo con ID {file_id}. Esto indica un problema de acceso o un cambio en la respuesta de Google Drive que no fue manejado en el pre-vuelo. El archivo descargado podría ser una página de error.")
                status = "Failed (Contenido HTML inesperado)"
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
                            if current_time - last_throughput_check_time >= 5:
                                elapsed_check_time = current_time - last_throughput_check_time
                                current_throughput = bytes_since_last_check / elapsed_check_time if elapsed_check_time > 0 else 0

                                if current_throughput < MIN_THROUGHPUT_THRESHOLD:
                                    if low_throughput_start_time is None:
                                        low_throughput_start_time = current_time
                                    elif current_time - low_throughput_start_time >= LOW_THROUGHPUT_TIMEOUT:
                                        logger.warning(f"Rendimiento bajo detectado para '{file_name}' ({current_throughput:.2f} B/s). La descarga podría ser lenta o estar atascada.")
                                        low_throughput_start_time = current_time 
                                else:
                                    low_throughput_start_time = None 

                                last_throughput_check_time = current_time
                                bytes_since_last_check = 0
            
            sha256_hash_result = calculate_sha256(full_output_path, chunk_size)
            if sha256_hash_result:
                sha256_file_path = f"{full_output_path}.sha256"
                with open(sha256_file_path, 'w') as f:
                    f.write(sha256_hash_result)
                logger.info(f"SHA256 hash calculado y guardado para '{file_name}': {sha256_hash_result}")

            status = "Success"
            return True, file_name, total_file_size_expected, start_time, status, sha256_hash_result

        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            logger.error(f"Error durante el intento de descarga principal {retry_count + 1} para {file_id} ('{file_name}'): {e}")
            if retry_count < max_retries:
                wait_time = RETRY_BACKOFF_FACTOR * (2 ** retry_count)
                logger.info(f"Reintentando en {wait_time} segundos...")
                time.sleep(wait_time)
            else:
                status = "Failed (Máx. reintentos para descarga principal)"
                logger.error(f"Fallo final: No se pudo descargar {file_id} ('{file_name}') después de {max_retries} reintentos.")
                return False, file_name, total_file_size_expected, start_time, status, sha256_hash_result
        except Exception as e:
            status = "Failed (Error inesperado durante la descarga principal)"
            logger.critical(f"Error inesperado al descargar {file_id} ('{file_name}'): {e}", exc_info=True)
            return False, file_name, total_file_size_expected, start_time, status, sha256_hash_result
    
    return False, file_name, total_file_size_expected, start_time, status, sha256_hash_result

def main():
    start_total_process_time = time.time()

    parser = argparse.ArgumentParser(description="Script de descarga de Google Drive.")
    parser.add_argument("--workers", type=int, default=DEFAULT_MAX_PARALLEL_DOWNLOADS,
                        help=f"Número de hilos de descarga paralelos (por defecto: {DEFAULT_MAX_PARALLEL_DOWNLOADS}).")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE,
                        help=f"Tamaño del chunk en bytes para descargas por streaming (por defecto: {DEFAULT_CHUNK_SIZE} bytes).")
    parser.add_argument("--retries", type=int, default=DEFAULT_MAX_RETRIES,
                        help=f"Número máximo de reintentos por descarga (por defecto: {DEFAULT_MAX_RETRIES}).")
    parser.add_argument("--input-file", type=str,
                        help="Ruta a un archivo de texto que contiene enlaces de Google Drive (un enlace por línea).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Si se establece, el script solo validará los enlaces y mostrará un resumen, sin descargar ningún archivo.")
    parser.add_argument("--use-api", action="store_true",
                        help="Usa la API oficial de Google Drive para las descargas (requiere credentials.json y token.json).")
    parser.add_argument("--serve", action="store_true",
                        help="Ejecuta el script como un servidor FastAPI.")
    parser.add_argument("--port", type=int, default=8000,
                        help="Puerto para el servidor FastAPI (por defecto: 8000).")
    args = parser.parse_args()

    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
    
    log_file_path = os.path.join(DOWNLOAD_FOLDER, "downloads.log")
    for handler in logger.handlers:
        if isinstance(handler, RotatingFileHandler):
            logger.removeHandler(handler)
    file_handler = RotatingFileHandler(log_file_path, maxBytes=5*1024*1024, backupCount=5)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)

    logger.info("--- Iniciando Google Drive Downloader ---")
    logger.info(f"Los archivos se guardarán en: '{os.path.abspath(DOWNLOAD_FOLDER)}'")
    logger.info(f"Configurado para {args.workers} descargas paralelas.")
    logger.info(f"Usando tamaño de chunk: {args.chunk_size} bytes.")
    logger.info(f"Máximo de reintentos por archivo: {args.retries}.")

    logger.info(f"Núcleos de CPU del sistema detectados: {os.cpu_count()}")
    if PSUTIL_AVAILABLE:
        total_memory_bytes = psutil.virtual_memory().total
        logger.info(f"Memoria total del sistema: {total_memory_bytes / (1024**3):.2f} GB")
    
    logger.info("Verificando conexión a Google Drive...")
    try:
        requests.get("https://drive.google.com", timeout=10)
        logger.info("Conexión a Google Drive exitosa.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error de conexión a Google Drive: No se pudo conectar a https://drive.google.com. Por favor, verifica tu conexión a internet. Error: {e}")
        return

    if args.serve:
        logger.info(f"Iniciando servidor FastAPI en http://0.0.0.0:{args.port}")
        app.state.download_summary_data = global_download_summary_data
        app.state.failed_original_links = global_failed_original_links
        app.state.active_downloads = global_active_downloads
        app.state.download_args = args

        class DownloadRequest(BaseModel):
            link: str

        @app.post("/download")
        async def api_download(request: DownloadRequest):
            link = request.link
            file_id = extract_file_id(link)
            if not file_id:
                raise HTTPException(status_code=400, detail="Enlace de Google Drive inválido o ID de archivo no encontrado.")
            
            request_id = str(time.time()).replace('.', '')
            global_active_downloads[request_id] = {"status": "pending", "file_id": file_id, "link": link, "progress": 0, "file_name": "N/A"}
            
            def run_download_task():
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
                        "progress": 100
                    })
                    logger.info(f"Descarga API completada para {file_id}: {status}")

                except Exception as e:
                    logger.error(f"Error durante la descarga API para {file_id}: {e}", exc_info=True)
                    global_active_downloads[request_id].update({"status": "error", "message": str(e), "progress": 0})
                    global_failed_original_links.add(link)

            threading.Thread(target=run_download_task).start()
            return {"message": "Descarga iniciada", "file_id": file_id, "request_id": request_id}

        @app.get("/status")
        async def api_status():
            return {"active_downloads": global_active_downloads}

        @app.get("/summary")
        async def api_summary():
            summary_json_path = os.path.join(DOWNLOAD_FOLDER, "download_summary.json")
            if os.path.exists(summary_json_path):
                try:
                    with open(summary_json_path, 'r', encoding='utf-8') as f:
                        return json.load(f)
                except Exception as e:
                    raise HTTPException(status_code=500, detail=f"Error al leer el archivo de resumen: {e}")
            raise HTTPException(status_code=404, detail="Resumen de descargas no encontrado.")

        uvicorn.run(app, host="0.0.0.0", port=args.port)
        return

    drive_links = []
    if args.input_file:
        file_path = args.input_file
        if not os.path.exists(file_path):
            logger.error(f"Error: Archivo de entrada '{file_path}' no encontrado. Por favor, proporciona una ruta válida.")
            return
        if not os.access(file_path, os.R_OK):
            logger.error(f"Error: Archivo de entrada '{file_path}' no se puede leer. Verifica los permisos.")
            return
        try:
            with open(file_path, 'r') as f:
                drive_links = [line.strip() for line in f if line.strip()]
            logger.info(f"Cargados {len(drive_links)} enlaces desde '{file_path}'.")
        except Exception as e:
            logger.error(f"Error al leer el archivo de entrada '{file_path}': {e}")
            return
    else:
        print("Ingresa los enlaces de Google Drive (uno por línea). Presiona Enter dos veces para finalizar:")
        while True:
            link = input()
            if not link:
                break
            drive_links.append(link.strip())
        logger.info(f"Ingresados {len(drive_links)} enlaces.")

    if not drive_links:
        logger.error("No se proporcionaron enlaces para descargar. Saliendo.")
        return

    download_tasks = []
    valid_links_count = 0
    
    for link in drive_links:
        if not is_valid_drive_link(link):
            logger.warning(f"Formato de enlace de Google Drive inválido: {link}. Será omitido.")
            global_failed_original_links.add(link)
            continue

        file_id = extract_file_id(link)
        if file_id:
            download_tasks.append((file_id, DOWNLOAD_FOLDER, args.retries, args.chunk_size, link))
            valid_links_count += 1
        else:
            logger.warning(f"No se pudo extraer el FILEID de la URL: {link}. Será omitido.")
            global_failed_original_links.add(link)

    if valid_links_count == 0:
        logger.error("No se encontraron FILEIDs válidos de Google Drive en la entrada proporcionada. Saliendo.")
        if args.dry_run:
            logger.info("\n--- Modo Dry Run Activado ---")
            logger.info(f"Se encontraron {valid_links_count} enlaces válidos para descargar.")
            if global_failed_original_links:
                logger.info(f"Se encontraron {len(global_failed_original_links)} enlaces inválidos o con ID no extraíble:")
                for link in sorted(list(global_failed_original_links)):
                    logger.info(f"  - {link}")
            logger.info("El script finalizará sin realizar descargas.")
        return

    if args.dry_run:
        logger.info("\n--- Modo Dry Run Activado ---")
        logger.info(f"Se encontraron {valid_links_count} enlaces válidos para descargar.")
        if global_failed_original_links:
            logger.info(f"Se encontraron {len(global_failed_original_links)} enlaces inválidos o con ID no extraíble:")
            for link in sorted(list(global_failed_original_links)):
                    logger.info(f"  - {link}")
        logger.info("El script finalizará sin realizar descargas.")
        return

    completed_downloads_count = 0
    failed_downloads_count = 0

    if args.workers > 1:
        logger.info(f"\nIniciando descargas paralelas ({args.workers} hilos)...")
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
                            f"Descarga Finalizada: ID={file_id_for_log} | Nombre='{file_name}' | Estado: {status} | "
                            f"Duración: {duration:.2f}s | Tamaño: {total_size} bytes | "
                            f"Velocidad Promedio: {avg_speed / 1024:.2f} KB/s"
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
                        logger.critical(f"La tarea para {file_id_for_log} (Enlace: {original_link}) generó una excepción: {exc}", exc_info=True)
                        failed_downloads_count += 1
                        global_failed_original_links.add(original_link)
                        global_download_summary_data.append({
                            'file_id': file_id_for_log,
                            'filename': 'N/A',
                            'status': 'Fallo (Excepción)',
                            'size_bytes': 0,
                            'duration_seconds': round(time.time() - start_total_process_time, 2),
                            'sha256_hash': 'N/A'
                        })
            except KeyboardInterrupt:
                logger.warning("KeyboardInterrupt detectado. Cerrando el ejecutor de forma elegante...")
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
                                f"Descarga Finalizada (Interrumpida): ID={file_id_for_log} | Nombre='{file_name}' | Estado: {status} | "
                                f"Duración: {duration:.2f}s | Tamaño: {total_size} bytes | "
                                f"Velocidad Promedio: {avg_speed / 1024:.2f} KB/s"
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
                            logger.critical(f"Tarea interrumpida para {file_id_for_log} (Enlace: {original_link}) generó una excepción: {exc}", exc_info=True)
                            failed_downloads_count += 1
                            global_failed_original_links.add(original_link)
                            global_download_summary_data.append({
                                'file_id': file_id_for_log,
                                'filename': 'N/A',
                                'status': 'Fallo (Excepción Interrumpida)',
                                'size_bytes': 0,
                                'duration_seconds': round(time.time() - start_total_process_time, 2),
                                'sha256_hash': 'N/A'
                            })
                    elif future.cancelled():
                        logger.info(f"Descarga para {file_id_for_log} (Enlace: {original_link}) fue cancelada debido a interrupción.")
                        failed_downloads_count += 1
                        global_failed_original_links.add(original_link)
                        global_download_summary_data.append({
                            'file_id': file_id_for_log,
                            'filename': 'N/A',
                            'status': 'Cancelada',
                            'size_bytes': 0,
                            'duration_seconds': 0,
                            'sha256_hash': 'N/A'
                        })
                    elif not future.done():
                        logger.info(f"Descarga para {file_id_for_log} (Enlace: {original_link}) aún estaba en curso y no se completó.")
                        failed_downloads_count += 1
                        global_failed_original_links.add(original_link)
                        global_download_summary_data.append({
                            'file_id': file_id_for_log,
                            'filename': 'N/A',
                            'status': 'Incompleta (Interrumpida)',
                            'size_bytes': 0,
                            'duration_seconds': 0,
                            'sha256_hash': 'N/A'
                        })
                logger.info("Cierre del ejecutor completado.")
    else:
        logger.info("\nIniciando descargas secuenciales...")
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
                    f"Descarga Finalizada: ID={file_id_for_log} | Nombre='{file_name}' | Estado: {status} | "
                    f"Duración: {duration:.2f}s | Tamaño: {total_size} bytes | "
                    f"Velocidad Promedio: {avg_speed / 1024:.2f} KB/s"
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
                logger.warning("KeyboardInterrupt detectado. Deteniendo descargas secuenciales.")
                global_failed_original_links.add(original_link)
                break
            except Exception as exc:
                logger.critical(f"La tarea de descarga secuencial para {file_id_for_log} (Enlace: {original_link}) generó una excepción: {exc}", exc_info=True)
                failed_downloads_count += 1
                global_failed_original_links.add(original_link)
                global_download_summary_data.append({
                    'file_id': file_id_for_log,
                    'filename': 'N/A',
                    'status': 'Fallo (Excepción)',
                    'size_bytes': 0,
                    'duration_seconds': round(time.time() - start_total_process_time, 2),
                    'sha256_hash': 'N/A'
                })

    logger.info(f"\n--- Resumen de Descargas ---")
    logger.info(f"Total de descargas intentadas: {len(download_tasks)}")
    logger.info(f"Descargas completadas: {completed_downloads_count}")
    logger.info(f"Descargas fallidas: {failed_downloads_count}")

    if global_download_summary_data:
        summary_csv_path = os.path.join(DOWNLOAD_FOLDER, "download_summary.csv")
        try:
            with open(summary_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['file_id', 'filename', 'status', 'size_bytes', 'duration_seconds', 'sha256_hash']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(global_download_summary_data)
            logger.info(f"Resumen de descargas guardado en: '{os.path.abspath(summary_csv_path)}'")
        except Exception as e:
            logger.error(f"Error al guardar el resumen de descargas en CSV: {e}")

    if global_download_summary_data:
        summary_json_path = os.path.join(DOWNLOAD_FOLDER, "download_summary.json")
        try:
            with open(summary_json_path, 'w', encoding='utf-8') as jsonfile:
                json.dump(global_download_summary_data, jsonfile, indent=4, ensure_ascii=False)
            logger.info(f"Resumen de descargas guardado en JSON: '{os.path.abspath(summary_json_path)}'")
        except Exception as e:
            logger.error(f"Error al guardar el resumen de descargas en JSON: {e}")

    if global_failed_original_links:
        failed_links_path = os.path.join(DOWNLOAD_FOLDER, "failed_downloads.txt")
        try:
            with open(failed_links_path, 'w', encoding='utf-8') as f:
                for link in sorted(list(global_failed_original_links)):
                    f.write(link + "\n")
            logger.info(f"Enlaces de descarga fallidos guardados en: '{os.path.abspath(failed_links_path)}'")
        except Exception as e:
            logger.error(f"Error al guardar los enlaces de descarga fallidos: {e}")

    if PSUTIL_AVAILABLE:
        process = psutil.Process(os.getpid())
        peak_memory_usage_bytes = process.memory_info().rss
        logger.info(f"Uso máximo de memoria: {peak_memory_usage_bytes / (1024**2):.2f} MB")

    end_total_process_time = time.time()
    total_duration_seconds = end_total_process_time - start_total_process_time
    logger.info(f"\nTiempo total del proceso: {total_duration_seconds:.2f} segundos.")
    logger.info(f"\nProceso de descarga finalizado. Revisa el registro en: '{os.path.abspath(log_file_path)}'")

if __name__ == "__main__":
    main()
