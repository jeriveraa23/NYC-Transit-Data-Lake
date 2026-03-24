import requests

class NYCTaxiExtractor:
    def __init__(self):
        self.base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    def download_parquet(self, year, month):
        """Download file and return like binary content"""
        file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
        url = f"{self.base_url}/{file_name}"

        print(f"DEBUG: Request to {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()

        return response.content, file_name