import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


def test_s3_connection():
    try:
        # Criar sessão com o perfil glue-etl-user
        session = boto3.Session(profile_name='glue-etl-user')

        # Criar cliente S3 usando a sessão
        s3_client = session.client('s3')

        # Listar buckets
        response = s3_client.list_buckets()

        print("Conexão com S3 bem-sucedida!")
        print("Buckets encontrados:")
        for bucket in response['Buckets']:
            print(f"- {bucket['Name']}")

    except NoCredentialsError:
        print("Erro: Credenciais AWS não encontradas. Configure suas credenciais AWS.")
    except PartialCredentialsError:
        print("Erro: Credenciais AWS incompletas.")
    except Exception as e:
        print(f"Erro ao conectar com S3: {e}")


if __name__ == "__main__":
    test_s3_connection()
