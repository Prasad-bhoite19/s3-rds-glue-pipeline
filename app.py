import os
import boto3
import pandas as pd
from sqlalchemy import create_engine

# Environment Variables
S3_BUCKET = os.getenv("S3_BUCKET")
S3_KEY = os.getenv("S3_KEY")

RDS_ENDPOINT = os.getenv("RDS_ENDPOINT")
RDS_USER = os.getenv("RDS_USER")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")
RDS_DB = os.getenv("RDS_DB")
RDS_TABLE = os.getenv("RDS_TABLE")

GLUE_DB = os.getenv("GLUE_DB")
GLUE_TABLE = os.getenv("GLUE_TABLE")
GLUE_S3_PATH = os.getenv("GLUE_S3_PATH")

# AWS Clients
s3 = boto3.client("s3")
glue = boto3.client("glue")

def read_s3_csv():
    print("Reading CSV from S3...")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    df = pd.read_csv(obj["Body"])
    print(df.head())
    return df

def upload_to_rds(df):
    try:
        print("Connecting to RDS...")
        engine = create_engine(
            f"mysql+pymysql://{RDS_USER}:{RDS_PASSWORD}@{RDS_ENDPOINT}/{RDS_DB}"
        )

        print("Uploading data to RDS...")
        df.to_sql(RDS_TABLE, engine, if_exists="replace", index=False)

        print("Data successfully inserted into RDS")

    except Exception as e:
        print("RDS upload failed:", e)
        fallback_to_glue()

def fallback_to_glue():
    print("Falling back to AWS Glue")

    try:
        glue.create_table(
            DatabaseName=GLUE_DB,
            TableInput={
                "Name": GLUE_TABLE,
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "id", "Type": "int"},
                        {"Name": "name", "Type": "string"},
                        {"Name": "department", "Type": "string"},
                        {"Name": "salary", "Type": "int"}
                    ],
                    "Location": GLUE_S3_PATH,
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                        "Parameters": {
                            "field.delim": ","
                        }
                    }
                },
                "TableType": "EXTERNAL_TABLE"
            }
        )

        print("Glue table created successfully")

    except Exception as e:
        print("Glue fallback failed:", e)

def main():
    df = read_s3_csv()
    upload_to_rds(df)

if __name__ == "__main__":
    main()
