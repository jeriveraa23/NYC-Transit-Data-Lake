import pandas as pd
import io 
from datetime import datetime

class NYCTaxiTransformer:
    def __init__(self):
        self.name = "NYC Taxi Silver Transformer"

    def transform_to_silver(self, raw_data_bytes):
        """Clean logic: Bronze -> Silver"""
        df = pd.read_parquet(io.BytesIO(raw_data_bytes))
        
        df_clean = df[(df['trip_distance'] > 0) & (df['total_amount'] > 0)].copy()

        df_clean['tpep_pickup_datetime']  = pd.to_datetime(df_clean['tpep_pickup_datetime'])
        df_clean['tpep_dropoff_datetime'] = pd.to_datetime(df_clean['tpep_dropoff_datetime'])

        df_clean['processed_at'] = datetime.now()

        output = io.BytesIO()
        df_clean.to_parquet(output, index=False)

        print(f"Silver transformation completed. Finals rows: {len(df_clean)}")
        return output.getvalue()
    
    def transform_to_gold(self, silver_data_bytes):
        """Business agregation: Silver -> Gold"""
        df = pd.read_parquet(io.BytesIO(silver_data_bytes))

        gold_df = df.groupby(['PULocationID', df['tpep_pickup_datetime'].dt.date]).agg({
            'total_amount': 'sum',
            'trip_distance': 'mean',
            'VendorID': 'count' # Contamos registros como total de viajes
        }).reset_index()

        gold_df.rename(columns={
            'tpep_pickup_datetime': 'pickup_date',
            'total_amount': 'daily_revenue',
            'trip_distance': 'avg_distance',
            'VendorID': 'total_trips'
        }, inplace=True)

        output = io.BytesIO()
        gold_df.to_parquet(output, index=False)

        print(f"Gold transformation completed. Finals rows: {len(gold_df)}")
        return output.getvalue()