from fastapi import FastAPI
from databricks import sql
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import os

# Cargar variables de entorno desde el archivo .env
load_dotenv()

app = FastAPI()

# Cargar variables de entorno desde Azure o .env
server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")

# Validar que las variables no sean None
if not server_hostname or not http_path or not access_token:
    raise ValueError("Faltan variables de entorno: Verifica DATABRICKS_SERVER_HOSTNAME, HTTP_PATH y ACCESS_TOKEN")

@app.get("/")
def read_root():
    return {"message": "¡Bienvenido a la API de Databricks! Usa /api/objetos para obtener datos."}

@app.get("/api/objetos")
def get_objetos():
    try:
        # Conexión al clúster de Databricks
        with sql.connect(server_hostname=server_hostname,
                         http_path=http_path,
                         access_token=access_token) as connection:

            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM prd_medallion.ds_bdanntp2_spplus_adm.spl_tb_objetos LIMIT 10")
                result = cursor.fetchall()

                # Obtener nombres de columnas
                columns = [column[0] for column in cursor.description]
                
                # Convertir resultados a una lista de diccionarios
                objetos = [dict(zip(columns, row)) for row in result]

        return JSONResponse(content=objetos)  # Devolver los resultados en formato JSON

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
