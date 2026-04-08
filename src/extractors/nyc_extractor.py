import requests

class NYCTaxiExtractor:
    def __init__(self):
        self.base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    def download_parquet(self, year, month):
        """Download parquet file as stream (memory efficient)"""

        file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
        url = f"{self.base_url}/{file_name}"

        print(f"Downloading from {url}")

        try:
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()

            return response.raw, file_name  # stream, not full content

        except requests.exceptions.RequestException as e:
            print(f"ERROR downloading file: {e}")
            raise
