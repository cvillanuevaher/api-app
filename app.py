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

from datetime import datetime

@app.get("/api/stock")
def get_stock(
    fecha: str = Query(..., description="Fecha del movimiento (YYYY-MM-DD)"),
    codigos_centros: list[str] = Query(..., description="Lista de códigos de centros"),
    codigos_canchas: list[str] = Query(..., description="Lista de códigos de canchas")
):
    try:
        # Validar parámetros requeridos
        if not fecha or not codigos_centros or not codigos_canchas:
            return JSONResponse(content={"error": "Parámetros requeridos: 'fecha', 'codigos_centros' y 'codigos_canchas'"}, status_code=400)

        # Convertir listas en cadenas separadas por comas para SQL
        codigos_centros_str = ", ".join([f"'{codigo}'" for codigo in codigos_centros])
        codigos_canchas_str = ", ".join([f"'{codigo}'" for codigo in codigos_canchas])

        # Consulta SQL parametrizada
        query = f"""
        SELECT 
            B.FECHA_MOVIMIENTO AS FECHA,
            C.NOMBRE AS centro,
            C.CODIGO AS cod_cancha,
            S.ID_SECTOR AS cod_sector,
            S.DESCRIPCION AS sector,
            PC.SIGLA AS producto,
            L.ESTADO_CALIDAD AS calidad,
            CONCAT(ENV.DESCRIPCION_CORTA, 
                   CASE WHEN ENV.COD_ENVASE = '16' THEN '' 
                   ELSE CONCAT(ENV.CAPACIDAD, ' ', ENV.UNIDAD_ENV) END) AS formato,
            SUM(CASE WHEN L.ALM_CODIGO = 4 THEN B.STOCK_FINAL / 1000 ELSE B.STOCK_FINAL END) AS stock
        FROM 
            `prd_medallion`.ds_bdanntp2_cancha_adm.sdp_tb_balance_dia_productos B
        JOIN 
            `prd_medallion`.ds_bdanntp2_cancha_adm.sdp_tb_lotes_inventario L ON B.ID_LOTE = L.ID_LOTE
        JOIN 
            `prd_medallion`.ds_bdanntp2_cancha_adm.sdp_tb_canchas C ON L.ALM_CODIGO = C.CODIGO
        JOIN 
            `prd_medallion`.ds_bdanntp2_cancha_adm.sdp_tb_ubicaciones U ON L.ALM_CODIGO = U.ALM_CODIGO AND L.ID_UBICACION = U.ID_UBICACION
        JOIN 
            `prd_medallion`.ds_bdanntp2_cancha_adm.sdp_tb_sectores S ON L.ALM_CODIGO = S.UBI_ALM_CODIGO AND L.ID_UBICACION = S.UBI_ID_UBICACION AND L.ID_SECTOR = S.ID_SECTOR
        JOIN 
            `prd_medallion`.ds_bdanntp2_cancha_adm.sdp_va_productos_canchas PC ON PC.COD_PRODUCTO = CAST(L.COD_PRODUCTO AS STRING)
        JOIN 
            `prd_medallion`.ds_bdanntp2_usr_dblink.sdp_tb_envases ENV ON ENV.COD_ENVASE = L.COD_ENVASE
        LEFT JOIN 
            `prd_medallion`.ds_bdanntp2_cancha_adm.sdp_tb_zonas_despacho ZD ON L.ALM_CODIGO = ZD.ALM_CODIGO AND L.ID_UBICACION = ZD.ID_UBICACION
        LEFT JOIN 
            `prd_medallion`.ds_bdanntp2_cancha_adm.sdp_no_sector_stock NSS ON CAST(L.ALM_CODIGO AS STRING) = NSS.COD_CANCHA AND L.ID_UBICACION = NSS.COD_UBI AND L.ID_SECTOR = NSS.COD_SEC
        WHERE 
            B.FECHA_MOVIMIENTO = DATE('{fecha}')
            AND L.ALM_CODIGO IN ({codigos_centros_str})
            AND L.ID_UBICACION IN ({codigos_canchas_str})
            AND L.COD_PRODUCTO NOT IN (2220, 2308)
            AND UPPER(S.DESCRIPCION) NOT LIKE '%VIRTUAL%'
            AND ZD.ALM_CODIGO IS NULL
            AND NSS.COD_CANCHA IS NULL
        GROUP BY 
            B.FECHA_MOVIMIENTO,
            C.NOMBRE,
            C.CODIGO,
            S.ID_SECTOR,
            S.DESCRIPCION,
            PC.SIGLA,
            L.ESTADO_CALIDAD,
            CONCAT(ENV.DESCRIPCION_CORTA, 
                   CASE WHEN ENV.COD_ENVASE = '16' THEN '' 
                   ELSE CONCAT(ENV.CAPACIDAD, ' ', ENV.UNIDAD_ENV) END)
        HAVING 
            SUM(CASE WHEN L.ALM_CODIGO = 4 THEN B.STOCK_FINAL / 1000 ELSE B.STOCK_FINAL END) >= 0
        """

        # Ejecutar la consulta SQL en Databricks
        with sql.connect(server_hostname=server_hostname,
                         http_path=http_path,
                         access_token=access_token) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()

                # Obtener nombres de columnas
                columns = [column[0] for column in cursor.description]

                # Convertir resultados a una lista de diccionarios
                data = [dict(zip(columns, row)) for row in result]

        return JSONResponse(content=data)

    except Exception as e:
        print(f"Error: {str(e)}")
        return JSONResponse(content={"error": str(e)}, status_code=500)

