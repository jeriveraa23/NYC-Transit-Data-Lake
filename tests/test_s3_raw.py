import pandas as pd
import requests
import io

def test_nyc_http_direct():
    # URL pública directa de Amazon S3 para el dataset
    # Nota: AWS permite acceso HTTPS directo a objetos en buckets públicos
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    
    print(f"--- Intentando descarga directa desde: {url} ---")
    
    try:
        # 1. Hacer una petición GET normal (como un navegador)
        response = requests.get(url, stream=True)
        response.raise_for_status() # Lanza error si no puede conectar
        
        # 2. Leer los bytes en memoria
        with io.BytesIO(response.content) as f:
            df = pd.read_parquet(f, engine='pyarrow')
        
        print("\n✅ ¡CONEXIÓN EXITOSA POR HTTP! MOSTRANDO 10 REGISTROS:\n")
        cols = ['tpep_pickup_datetime', 'trip_distance', 'total_amount', 'tip_amount']
        print(df[cols].head(10).to_string(index=False))
        print(f"\nFilas totales cargadas: {len(df):,}")

    except Exception as e:
        print(f"❌ Error incluso por HTTP: {e}")
        print("\nPosibles causas:")
        print("1. El archivo cambió de nombre (NYC usa CloudFront ahora).")
        print("2. Tu firewall o antivirus está bloqueando la conexión saliente.")

if __name__ == "__main__":
    # Necesitas instalar 'requests' si no lo tienes: pip install requests
    test_nyc_http_direct()