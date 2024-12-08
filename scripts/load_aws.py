import boto3
from botocore.exceptions import NoCredentialsError
import os

def handle_s3(file_name, bucket, access_key, secret_key, session_token, prefix):
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token
    )
    s3_client = session.client('s3')

    try:
        # Obtém apenas o nome do arquivo do caminho completo
        file_name_base = os.path.basename(file_name)
        
        # Construindo o nome completo do arquivo dentro do prefixo especificado
        object_name = f"raw/{prefix}/{file_name_base}"
        
        # Realizando o upload para o S3
        s3_client.upload_file(file_name, bucket, object_name)

        os.remove(file_name)

        return True
    except NoCredentialsError:
        print("As credenciais não estão disponíveis")
        return False
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        return False

