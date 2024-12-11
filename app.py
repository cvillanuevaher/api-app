from fastapi import FastAPI
from databricks import sql
from fastapi.responses import JSONResponse
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

app = FastAPI()

# Configuración Databricks
server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME", "adb-2719666371304333.13.azuredatabricks.net")
http_path = os.getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/88f4da3677adc403")
access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")

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

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
