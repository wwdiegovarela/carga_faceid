from fastapi import FastAPI, HTTPException
import requests
from google.cloud import bigquery
import json
from datetime import datetime
import pandas as pd
import os


# Configuración
API_LOCAL_URL = os.getenv("API_LOCAL_URL")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID_24 = os.getenv("TABLE_ID_24")
TABLE_ID_HIST = os.getenv("TABLE_ID_HIST")
TOKEN_24 = os.getenv("TOKEN_CR_24")
TOKEN_HIST = os.getenv("TOKEN_HIST")

def consulta_cr(token):
    """Consulta la API de ControlRoll y retorna un DataFrame normalizado"""
    headers = {
        "method": "report",
        "token": token
    }
    response = requests.get(API_LOCAL_URL, headers=headers, timeout=3600)
    response.raise_for_status()
    
    data_text = response.text
    
    try:
        data_json = json.loads(data_text)
    except json.JSONDecodeError as e:
        print(f"Error parseando JSON. Status code: {response.status_code}")
        print(f"Primeros 500 caracteres: {data_text[:500]}")
        print(f"Content-Type: {response.headers.get('Content-Type', 'N/A')}")
        raise ValueError(f"La API no devolvió JSON válido. Status: {response.status_code}. Primeros 200 chars: {data_text[:200]}") from e
    
    if not isinstance(data_json, list):
        if isinstance(data_json, dict):
            if 'error' in data_json or 'message' in data_json:
                raise ValueError(f"La API devolvió un error: {data_json}")
            data_json = [data_json]
        else:
            raise ValueError(f"La API devolvió un tipo inesperado: {type(data_json)}")
    
    if len(data_json) == 0:
        return pd.DataFrame()
    
    data = pd.DataFrame(data_json)
    
    data.columns = data.columns.str.lower()
    data.columns = data.columns.str.replace(' ', '_')
    data.columns = data.columns.str.replace('.', '')
    data.columns = data.columns.str.replace('%', '')
    data.columns = data.columns.str.replace('-', '_')
    data.columns = data.columns.str.replace('(', '')
    data.columns = data.columns.str.replace(')', '')
    data.columns = data.columns.str.replace('á', 'a')
    data.columns = data.columns.str.replace('é', 'e')
    data.columns = data.columns.str.replace('í', 'i')
    data.columns = data.columns.str.replace('ó', 'o')
    data.columns = data.columns.str.replace('ú', 'u')
    data.columns = data.columns.str.replace('ñ', 'n')
    data.columns = data.columns.str.replace('°', '')
   


    date_columns = ['fecha']
    print("Transformando columnas a formato datetime")
    for col in date_columns:
        if col in data.columns:
            data[col] = (
                pd.to_datetime(data[col], format='%d-%m-%Y %H:%M:%S', errors='coerce')
                  .dt.date
            )
    
    time_columns = ['hora']
    print("Transformando columnas a formato hora")
        for col in time_columns:
            if col in data.columns:
            data[col] = (
                pd.to_datetime(data[col], format='%H:%M:%S', errors='coerce')
                .dt.time
            )


    # Procesar datos de rotación

    print(f"✅ Datos procesados exitosamente: {len(data)} registros")
    return data

def load_to_bigquery(df_bridge, table_id, write_disposition="WRITE_TRUNCATE"):
    """Función para cargar datos procesados a BigQuery"""
    if df_bridge is None:
        return {
            "success": True,
            "message": "No hay datos para cargar",
            "records_processed": 0
        }
    
    print("=== CARGANDO DATOS A BIGQUERY ===")
    
    try:
        if not table_id:
            raise ValueError("No se proporcionó TABLE_ID válido para la carga.")

        client = bigquery.Client(project=PROJECT_ID)
        full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
        
        job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)

        schema_fields = []
        if "fecha" in df_bridge.columns:
            schema_fields.append(bigquery.SchemaField("fecha", "DATE"))
        if "hora" in df_bridge.columns:
            schema_fields.append(bigquery.SchemaField("hora", "TIME"))
        if schema_fields:
            job_config.schema = schema_fields
        
        print(f"🔄 Cargando {len(df_bridge)} registros a BigQuery: {full_table_id}")
        job = client.load_table_from_dataframe(df_bridge, full_table_id, job_config=job_config)
        job.result()
        
        print(f"✅ Data cargada exitosamente. {len(df_bridge)} registros cargados a BigQuery.")
        
        return {
            "success": True,
            "message": "Data procesada y cargada exitosamente",
            "records_processed": len(df_bridge)
        }
    except Exception as e:
        error_msg = f"Error al cargar datos en BigQuery: {type(e).__name__}: {str(e)}"
        print(f"❌ {error_msg}")
        import traceback
        print(f"❌ Stack trace: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=error_msg)

def sync_to_bigquery(token, table_id, write_disposition="WRITE_TRUNCATE"):
    """Función principal para sincronizar datos con BigQuery"""
    print("=== INICIANDO SINCRONIZACIÓN COMPLETA ===")

    if not token:
        raise ValueError("No se proporcionó token válido para la sincronización.")
    if not table_id:
        raise ValueError("No se proporcionó TABLE_ID válido para la sincronización.")
    
    # Paso 1: Obtener y procesar datos
    df_bridge = consulta_cr(token)
    
    # Paso 2: Cargar a BigQuery
    result = load_to_bigquery(df_bridge, table_id, write_disposition=write_disposition)
    
    return result

# Crear la aplicación FastAPI
app = FastAPI()

@app.get("/")
def root():
    return {"message": "Servicio de sincronización de rotación activo"}

@app.get("/health")
def health_check():
    """Endpoint de salud para verificar el estado del servicio"""
    return {
        "status": "healthy",
        "message": "Servicio funcionando correctamente",
        "timestamp": datetime.now().isoformat()
    }

@app.post("/fetch_data")
def fetch_data():
    """
    Endpoint para obtener y procesar datos de la API externa (sin cargar a BigQuery)
    """
    try:
        df_bridge = consulta_cr(TOKEN_24)
        if df_bridge is None:
            return {
                "success": True,
                "message": "No hay datos para procesar",
                "records_processed": 0
            }
        
        return {
            "success": True,
            "message": "Datos obtenidos y procesados exitosamente",
            "records_processed": len(df_bridge),
            "columns": list(df_bridge.columns),
            "sample_data": df_bridge.head(3).to_dict('records') if len(df_bridge) > 0 else []
        }
    except Exception as e:
        error_response = {
            "success": False,
            "error": str(e),
            "message": "Error al obtener y procesar datos"
        }
        raise HTTPException(status_code=500, detail=error_response)

@app.post("/load_data")
def load_data():
    """
    Endpoint para cargar datos procesados a BigQuery
    """
    try:
        return sync_to_bigquery(TOKEN_24, TABLE_ID_24)
    except Exception as e:
        error_response = {
            "success": False,
            "error": str(e),
            "message": "Error al cargar datos a BigQuery"
        }
        raise HTTPException(status_code=500, detail=error_response)


@app.post("/carga_bigquery/cr_24")
def carga_bigquery():
    """
    Endpoint para sincronizar datos de rotación (proceso completo)
    """
    try:
        result = sync_to_bigquery(TOKEN_24, TABLE_ID_24, write_disposition="WRITE_TRUNCATE")
        return result
    except Exception as e:
        error_response = {
            "success": False,
            "error": str(e),
            "message": "Error al procesar la sincronización"
        }
        raise HTTPException(status_code=500, detail=error_response)

@app.post("/carga_bigquery/cr_hist")
def carga_bigquery2():
    """
    Endpoint para sincronizar datos de rotación utilizando el segundo token
    """
    try:
        result = sync_to_bigquery(TOKEN_HIST, TABLE_ID_HIST, write_disposition="WRITE_APPEND")
        return result
    except Exception as e:
        error_response = {
            "success": False,
            "error": str(e),
            "message": "Error al procesar la sincronización con el segundo token"
        }
        raise HTTPException(status_code=500, detail=error_response)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))