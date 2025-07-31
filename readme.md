Google Drive DownloaderEste script de Python está diseñado para automatizar y simplificar el proceso de descarga de múltiples archivos de Google Drive, eliminando la necesidad de interacción manual para cada enlace. Ofrece una solución eficiente para usuarios que gestionan un gran volumen de descargas, permitiendo la recuperación de archivos a través de métodos directos (HTTP) o mediante la API oficial de Google Drive. Incluye funcionalidades robustas para descargas paralelas, reanudación, verificación de integridad y puede operarse como un script de línea de comandos o como un servidor FastAPI.CaracterísticasAutomatización de Descargas Masivas: Facilita la descarga de múltiples enlaces de Google Drive sin intervención individual, ideal para colecciones extensas de archivos.Múltiples métodos de descarga: Descarga directa (HTTP) o a través de la API oficial de Google Drive.Descargas paralelas: Configura el número de hilos para descargar múltiples archivos simultáneamente, optimizando el tiempo.Reanudación de descargas: Continúa las descargas incompletas, ahorrando ancho de banda y tiempo.Verificación de integridad: Calcula y guarda el hash SHA256 de los archivos descargados para asegurar su integridad.Manejo de errores: Detecta y notifica errores comunes como "cuota excedida" de Google Drive, proporcionando soluciones.Modo Dry Run: Valida los enlaces y muestra un resumen sin iniciar descargas, útil para la planificación.Modo Servidor FastAPI: Ejecuta el descargador como un servicio web con endpoints RESTful para iniciar descargas, verificar el estado y obtener resúmenes, permitiendo la integración con otras aplicaciones.Registro detallado: Genera un archivo de registro (downloads.log) con información detallada de cada operación para seguimiento y depuración.Resumen de descargas: Genera archivos CSV y JSON con un resumen completo de todas las descargas realizadas.RequisitosPara ejecutar este script, necesitas tener instalados los siguientes paquetes de Python:requeststqdmbeautifulsoup4google-api-python-clientgoogle-auth-httplib2google-auth-oauthlibFastAPIuvicornpydanticpsutil (opcional, para monitoreo de memoria)Puedes instalarlos usando pip:pip install requests tqdm beautifulsoup4 google-api-python-client google-auth-httplib2 google-auth-oauthlib fastapi uvicorn pydantic psutil
Para descargas con la API de Google Drive (--use-api)Si planeas usar la opción --use-api, necesitarás configurar las credenciales de la API de Google Drive:Ve a la Consola de Desarrolladores de Google Cloud.Crea un nuevo proyecto o selecciona uno existente.Habilita la Google Drive API para tu proyecto.Ve a "Credenciales" y crea credenciales de "ID de cliente de OAuth".Selecciona "Aplicación de escritorio" como tipo de aplicación.Descarga el archivo credentials.json y colócalo en el mismo directorio que tu script drive_downloader.py.La primera vez que uses la opción --use-api, se abrirá una ventana del navegador para que autorices la aplicación. Una vez autorizado, se generará un archivo token.json que almacenará tus credenciales para futuras sesiones.InstalaciónClona este repositorio o descarga el archivo drive_downloader.py.Instala las dependencias como se mencionó en la sección Requisitos.UsoEl script se puede ejecutar de dos maneras principales: como un script de línea de comandos o como un servidor FastAPI.1. Como script de línea de comandospython drive_downloader.py [OPCIONES]
Opciones:--workers <int>: Número de hilos de descarga paralelos (por defecto: 4).--chunk-size <int>: Tamaño del chunk en bytes para descargas por streaming (por defecto: 32768).--retries <int>: Número máximo de reintentos por descarga (por defecto: 3).--input-file <path>: Ruta a un archivo de texto que contiene enlaces de Google Drive (un enlace por línea).--dry-run: Si se establece, el script solo validará los enlaces y mostrará un resumen, sin descargar ningún archivo.--use-api: Usa la API oficial de Google Drive para las descargas (requiere credentials.json y token.json).Ejemplos:Descargar enlaces introducidos manualmente (secuencial):python drive_downloader.py
(Luego, introduce los enlaces uno por uno y presiona Enter dos veces para finalizar).Descargar enlaces desde un archivo (paralelo, 8 hilos):python drive_downloader.py --input-file links.txt --workers 8
Descargar usando la API de Google Drive:python drive_downloader.py --input-file links.txt --use-api
Realizar un "dry run" para validar enlaces:python drive_downloader.py --input-file links.txt --dry-run
2. Como servidor FastAPIPuedes ejecutar el script como un servidor web para interactuar con él a través de una API REST.python drive_downloader.py --serve [--port <int>]
--serve: Habilita el modo servidor FastAPI.--port <int>: Puerto para el servidor FastAPI (por defecto: 8000).Una vez que el servidor esté en funcionamiento (por ejemplo, en http://localhost:8000), puedes interactuar con los siguientes endpoints:POST /download: Inicia una descarga.Método: POSTCuerpo de la solicitud (JSON):{
    "link": "https://drive.google.com/file/d/FILE_ID/view"
}
Respuesta (JSON):{
    "message": "Download initiated",
    "file_id": "FILE_ID",
    "request_id": "UNIQUE_REQUEST_ID"
}
GET /status: Obtiene el estado de las descargas activas.Método: GETRespuesta (JSON):{
    "active_downloads": {
        "UNIQUE_REQUEST_ID_1": {
            "status": "completed",
            "file_id": "FILE_ID_1",
            "link": "...",
            "progress": 100,
            "file_name": "downloaded_file.zip",
            "total_size": 1234567,
            "duration": 10.5,
            "avg_speed": 123456,
            "sha256": "..."
        },
        "UNIQUE_REQUEST_ID_2": {
            "status": "pending",
            "file_id": "FILE_ID_2",
            "link": "...",
            "progress": 0,
            "file_name": "N/A"
        }
    }
}
GET /summary: Obtiene el resumen completo de todas las descargas (del archivo download_summary.json).Método: GETRespuesta (JSON): Una lista de objetos de resumen de descarga.[
    {
        "file_id": "FILE_ID_1",
        "filename": "downloaded_file.zip",
        "status": "Success",
        "size_bytes": 1234567,
        "duration_seconds": 10.5,
        "sha256_hash": "abcdef123456..."
    },
    {
        "file_id": "FILE_ID_2",
        "filename": "another_file.pdf",
        "status": "Failed (Quota Exceeded)",
        "size_bytes": 0,
        "duration_seconds": 5.2,
        "sha256_hash": "N/A"
    }
]
Archivos generadosEl script creará una carpeta drive_downloads en el directorio de ejecución. Dentro de esta carpeta, encontrarás:Archivos descargados: Los archivos de Google Drive.Archivos .sha256: Archivos de texto que contienen el hash SHA256 de cada descarga exitosa.downloads.log: Un archivo de registro detallado de todas las operaciones del script.download_summary.csv: Un resumen de todas las descargas en formato CSV.download_summary.json: Un resumen de todas las descargas en formato JSON.failed_downloads.txt: Una lista de los enlaces originales que fallaron en la descarga.Solución de Problemascredentials.json not found (Error de autenticación API): Asegúrate de haber seguido los pasos en la sección Para descargas con la API de Google Drive para obtener y colocar el archivo credentials.json correctamente.[ERROR - QUOTA EXCEEDED]: Google Drive ha bloqueado temporalmente la descarga debido a tráfico excesivo. La solución más común es hacer una copia del archivo en tu propia cuenta de Google Drive y usar el ID de esa nueva copia para la descarga.Connection to Google Drive failed: Verifica tu conexión a internet. El script necesita acceso a drive.google.com.Descargas lentas o atascadas: El script tiene un mecanismo de detección de bajo rendimiento. Si ves advertencias de "Low throughput detected", la descarga podría ser muy lenta o estar estancada. Esto puede deberse a problemas de red o limitaciones del servidor de Google Drive. Los reintentos automáticos pueden ayudar.Unrecognized HTML Page o Unexpected HTML Content: Esto indica que Google Drive devolvió una página HTML inesperada en lugar del archivo. Podría ser un error no manejado, un cambio en la forma en que Google Drive sirve los archivos, o una página de error diferente a la de "cuota excedida".Contribución¡Las contribuciones son bienvenidas! Si encuentras un error o tienes una sugerencia para mejorar, por favor, abre un issue o envía un pull request.LicenciaEste proyecto está bajo la licencia MIT.