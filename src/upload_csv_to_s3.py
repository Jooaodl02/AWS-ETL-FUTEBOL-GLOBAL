import os
import boto3
from botocore.exceptions import BotoCoreError, ClientError

CSV_FILES = [
    "former_names.csv",
    "goalscorers.csv",
    "results.csv",
    "shootouts.csv",
]

BUCKET_NAME = "bucket-futebol"
BUCKET_PREFIX = "raw"
PROFILE_NAME = "glue-etl-user"


def upload_file_to_s3(local_path: str, bucket: str, key: str, s3_client):
    try:
        s3_client.upload_file(local_path, bucket, key)
        print(f"OK: {local_path} -> s3://{bucket}/{key}")
    except FileNotFoundError:
        print(f"ERRO: Arquivo local não encontrado: {local_path}")
    except (BotoCoreError, ClientError) as err:
        print(
            f"ERRO: Falha ao fazer upload de {local_path} -> s3://{bucket}/{key}: {err}")


def main():
    session = boto3.Session(profile_name=PROFILE_NAME)
    s3_client = session.client("s3")

    for csv_file in CSV_FILES:
        if not os.path.isfile(csv_file):
            print(f"Pulando (não encontrado): {csv_file}")
            continue

        # Cada CSV em sua própria pasta raw/<nome_tabela>/<arquivo>
        table_name = os.path.splitext(csv_file)[0]
        object_key = f"{BUCKET_PREFIX}/{table_name}/{csv_file}"
        upload_file_to_s3(csv_file, BUCKET_NAME, object_key, s3_client)


if __name__ == "__main__":
    main()
