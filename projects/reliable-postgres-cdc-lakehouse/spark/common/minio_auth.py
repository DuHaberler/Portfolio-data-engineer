from common.secrets_file import read_secrets

SECRETS_FILE_ENV = "MINIO_SECRETS_FILE"
ACCESS_KEY = "MINIO_SPARK_ACCESS_KEY"
SECRET_KEY = "MINIO_SPARK_SECRET_KEY"

#Retorna '(access_key, secret_key)' do usuário MinIO do Spark.
def get_minio_credential(secrets_file=None) -> tuple[str, str]:

    values = read_secrets(secrets_file, SECRETS_FILE_ENV, [ACCESS_KEY, SECRET_KEY], "minio_init")
    return {values[ACCESS_KEY]}, {values[SECRET_KEY]}