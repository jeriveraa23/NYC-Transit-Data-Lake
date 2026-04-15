import pandas as pd
import s3f3
from typing import Optional, Dict, List

class S3Service:
    def __init__(self, bucket_name: str):
        self.bucket = bucket_name
        self.fs = s3fs.S3FileSystem()

    def _build_path(self, prefix: str, partitions: Optional[Dict[str, int]] = None) -> str:
        path = f"s3://{self.bucket}/{prefix}"

        if partitions:
            for key, value in partitions.items():
                if isinstance(value, int) and key == "month":
                    value = f"{value:02d}"

                    path += f"{key}={value}/"
        
        return path
    
    def read_parquet(self, prefix: str, partitions: Optional[Dict[str, int]] = None, columns: Optional[List[str]] = None) -> pd.DataFrame:
        path = self._build_path(prefix, partitions)

        print(f"[SeService] Reading from: {path}")

        try:
            df = pd.read_parquet(
                path,
                filesystem=self.fs,
                columns=columns
            )

            if df.empty:
                print(f"[S3Service] Warning, Dataframe empty in {path}")
            
            return df
        
        except Exception as e:
            print(f"[S3Service] Error reading {path}: {e}")
            raise

    def list_files(self, prefix:str) -> List[str]:
        path = f"{self.bucket}/{prefix}"

        try:
            files = self.fs.ls(path)
            print(f"[S3Service] {len(files)} files founded in {path}")
            return files
        except Exception as e:
            print(f"[S3Service] Error listing files in {path}:{e}")
            raise