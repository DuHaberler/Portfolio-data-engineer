from common.secrets_file import read_secrets

SECRETS_FILE_ENV = "POLARIS_SECRETS_FILE"
CLIENT_ID_KEY = "POLARIS_SPARK_CLIENT_ID"
CLIENT_SECRET_KEY = "POLARIS_SPARK_CLIENT_SECRET"

#Retorna 'clientId:clientSecret' do principal do Spark.
def get_polaris_credential(secrets_file=None) -> str:

    values = read_secrets(secrets_file, SECRETS_FILE_ENV, [CLIENT_ID_KEY, CLIENT_SECRET_KEY], "polaris_init")
    return f"{values[CLIENT_ID_KEY]}:{values[CLIENT_SECRET_KEY]}"