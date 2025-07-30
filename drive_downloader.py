import re
import os
import time
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup # Importación de BeautifulSoup para parsear HTML

# --- Configuración ---
DOWNLOAD_FOLDER = "descargas_drive"  # Carpeta donde se guardarán los archivos
MAX_RETRIES = 3                     # Número máximo de reintentos por descarga
RETRY_BACKOFF_FACTOR = 1            # Factor para el backoff exponencial (1s, 2s, 4s, etc.)
MAX_PARALLEL_DOWNLOADS = 4          # Número máximo de descargas concurrentes (opcional)

# --- Funciones Auxiliares ---

def extract_file_id(url):
    """
    Extrae el FILEID de una URL de Google Drive.
    Soporta formatos como:
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
    Descarga un archivo de Google Drive, manejando la confirmación de archivos grandes
    y reintentos exponenciales.
    """
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    session = requests.Session()
    log_messages = []

    for retry_count in range(MAX_RETRIES + 1):
        try:
            # Primer intento para obtener el archivo.
            # Podría ser la descarga directa o la página de advertencia.
            response = session.get(download_url, stream=True)
            response.raise_for_status()  # Lanza una excepción para códigos de estado HTTP erróneos

            # --- Detección y manejo de la página de advertencia de Google Drive ---
            # Google devuelve una página HTML de advertencia para archivos > 100MB
            # que no han sido escaneados en busca de virus.
            if 'Content-Type' in response.headers and 'text/html' in response.headers['Content-Type']:
                log_messages.append(f"Detectada página de advertencia de Google Drive para {file_id}. Intentando extraer formulario de confirmación.")
                
                # Parsear el contenido HTML para encontrar el formulario de confirmación
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Buscar el formulario con id="download-form"
                download_form = soup.find('form', {'id': 'download-form'})

                if download_form:
                    # Extraer la URL de acción del formulario, que será el endpoint real de descarga
                    action_url = download_form.get('action')
                    if not action_url.startswith('http'): # Asegurarse de que sea una URL absoluta
                        action_url = f"https://drive.google.com{action_url}"

                    # Recoger todos los campos ocultos (input type="hidden") del formulario
                    params = {}
                    hidden_inputs = download_form.find_all('input', {'type': 'hidden'})
                    for input_tag in hidden_inputs:
                        name = input_tag.get('name')
                        value = input_tag.get('value')
                        if name and value:
                            params[name] = value

                    log_messages.append(f"Formulario de confirmación encontrado. Reintentando descarga con parámetros del formulario para {file_id}.")
                    
                    # Realizar la segunda solicitud GET al endpoint de acción con todos los parámetros ocultos.
                    # Esto simula el envío del formulario y debería iniciar la descarga binaria.
                    response = session.get(action_url, params=params, stream=True)
                    response.raise_for_status()
                else:
                    # Si se detecta HTML pero no se encuentra el formulario, es un fallo inesperado
                    log_messages.append(f"Advertencia: Página HTML detectada para {file_id}, pero no se encontró el formulario de descarga. Esto podría indicar un cambio en la página de advertencia de Google Drive o un problema de acceso.")
                    raise requests.exceptions.RequestException("No se encontró el formulario de descarga en la página de advertencia.")

            # --- Continuación del flujo de descarga normal ---
            # Obtener el nombre del archivo desde los encabezados o usar el file_id
            file_name = None
            if 'Content-Disposition' in response.headers:
                # Intenta extraer el nombre de archivo del Content-Disposition,
                # que es el método preferido para obtener el nombre original.
                fname_match = re.search(r'filename\*?=UTF-8\'\'(.+)', response.headers['Content-Disposition'])
                if fname_match:
                    file_name = requests.utils.unquote(fname_match.group(1))
                else:
                    fname_match = re.search(r'filename="([^"]+)"', response.headers['Content-Disposition'])
                    if fname_match:
                        file_name = fname_match.group(1)

            if not file_name:
                file_name = f"{file_id}.bin" # Nombre por defecto si no se puede extraer

            full_output_path = os.path.join(output_path, file_name)
            total_size = int(response.headers.get('content-length', 0))

            with open(full_output_path, 'wb') as f:
                # Usar tqdm para mostrar una barra de progreso de la descarga
                with tqdm(total=total_size, unit='B', unit_scale=True, desc=file_name) as pbar:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))

            log_messages.append(f"Éxito: '{file_name}' descargado en '{full_output_path}'")
            return True, log_messages

        except requests.exceptions.RequestException as e:
            # Manejo de errores de red o HTTP y reintentos exponenciales
            log_messages.append(f"Error en el intento {retry_count + 1} para {file_id}: {e}")
            if retry_count < MAX_RETRIES:
                wait_time = RETRY_BACKOFF_FACTOR * (2 ** retry_count)
                log_messages.append(f"Reintentando en {wait_time} segundos...")
                time.sleep(wait_time)
            else:
                log_messages.append(f"Fallo definitivo: No se pudo descargar {file_id} después de {MAX_RETRIES} reintentos.")
                return False, log_messages
        except Exception as e:
            # Captura cualquier otra excepción inesperada
            log_messages.append(f"Error inesperado al descargar {file_id}: {e}")
            return False, log_messages

# --- Función Principal ---

def main():
    print("--- Iniciando el Descargador de Google Drive ---")

    # Asegurarse de que la carpeta de descarga exista
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
    print(f"Los archivos se guardarán en: '{os.path.abspath(DOWNLOAD_FOLDER)}'")

    # Leer enlaces desde un archivo o lista en memoria
    drive_links = []
    input_choice = input("¿Deseas introducir los enlaces desde un archivo de texto (f) o directamente aquí (m)? [f/m]: ").lower()

    if input_choice == 'f':
        file_path = input("Introduce la ruta del archivo de texto con los enlaces (un enlace por línea): ")
        try:
            with open(file_path, 'r') as f:
                drive_links = [line.strip() for line in f if line.strip()]
            print(f"Se han cargado {len(drive_links)} enlaces desde '{file_path}'.")
        except FileNotFoundError:
            print(f"Error: El archivo '{file_path}' no fue encontrado.")
            return
    elif input_choice == 'm':
        print("Introduce los enlaces de Google Drive (uno por línea). Presiona Enter dos veces para finalizar:")
        while True:
            link = input()
            if not link:
                break
            drive_links.append(link.strip())
        print(f"Se han introducido {len(drive_links)} enlaces.")
    else:
        print("Opción no válida. Saliendo.")
        return

    if not drive_links:
        print("No se proporcionaron enlaces para descargar. Saliendo.")
        return

    # Procesar enlaces
    download_tasks = []
    for link in drive_links:
        file_id = extract_file_id(link)
        if file_id:
            download_tasks.append((file_id, DOWNLOAD_FOLDER))
        else:
            print(f"Advertencia: No se pudo extraer el FILEID de la URL: {link}. Se omitirá.")

    if not download_tasks:
        print("No se encontraron FILEIDs válidos para descargar. Saliendo.")
        return

    # Descargar en paralelo si está configurado
    log_results = []
    if MAX_PARALLEL_DOWNLOADS > 1:
        print(f"\nIniciando descargas en paralelo ({MAX_PARALLEL_DOWNLOADS} hilos/procesos)...")
        with ThreadPoolExecutor(max_workers=MAX_PARALLEL_DOWNLOADS) as executor:
            future_to_file_id = {executor.submit(download_file_from_google_drive, file_id, output_path): file_id for file_id, output_path in download_tasks}
            for future in as_completed(future_to_file_id):
                file_id = future_to_file_id[future]
                success, messages = future.result()
                log_results.extend(messages)
    else:
        print("\nIniciando descargas secuenciales...")
        for file_id, output_path in download_tasks:
            success, messages = download_file_from_google_drive(file_id, output_path)
            log_results.extend(messages)

    # --- Registro de Logs ---
    log_file_path = os.path.join(DOWNLOAD_FOLDER, "descargas_log.txt")
    with open(log_file_path, "w") as f:
        for msg in log_results:
            f.write(msg + "\n")
            print(msg) # También imprime en consola

    print(f"\nProceso de descarga finalizado. Revisa el log en: '{os.path.abspath(log_file_path)}'")

if __name__ == "__main__":
    main()
