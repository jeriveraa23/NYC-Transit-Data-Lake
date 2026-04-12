from prophet import Prophet
import pandas as pd

class NYCForecaster:

    def __init__(self, forecast_days: int = 30):
        self.forecast_days = forecast_days
        self.model = None
        self.forecast = None

    def train(self, pandas_df: pd.DataFrame) -> "NYCForecaster":

        if pandas_df.empty:
            raise ValueError("The training DataFrame is empty.")
        
        pandas_df = pandas_df.copy()
        pandas_df["ds"] = pd.to_datetime(pandas_df["ds"])
        pandas_df["y"]  = pandas_df["y"].astype(float)

        self.model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=True,
            daily_seasonality=False,
            interval_width=0.95
        )

        self.model.fit(pandas_df)
        print(f"[Forecaster] The model has trained with {len(pandas_df)} history days")

        return self
    
    def predict(self) -> pd.DataFrame:
        if self.model is None:
            raise ValueError("The model hasn't been trained. First call train()")
        
        future = self.model.make_future_dataframe(periods=self.forecast_days, freq="D")
        self.forecast = self.model.predict(future)

        result = self.forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
        result["yhat"] = result["yhat"].clip(lower=0).round(0).astype(int)
        result["yhat_lower"] = result["yhat_lower"].clip(lower=0).round(0).astype(int)
        result["yhat_upper"] = result["yhat_upper"].clip(lower=0).round(0).astype(int)

        print(f"[Forecaster] Forecast generated for {self.forecast_days} days.")
        return result
    
    def save_forecast(self, forecast_df:pd.DataFrame, spark, bucket_name: str):
        output_path =f"s3a://{bucket_name}/ml/forecasts/"

        spark_df = spark.createDataFrame(forecast_df)

        spark_df.write \
            .mode("overwrite") \
            .parquet(output_path)
        
        print(f"[Forecaster] Forecast saved in {output_path}")