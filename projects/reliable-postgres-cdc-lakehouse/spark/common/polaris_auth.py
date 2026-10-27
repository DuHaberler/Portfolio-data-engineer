import os
from pathlib import Path

SECRETS_FILE_ENV = "POLARIS_SECRETS_FILE"
CLIENT_ID_KEY = "POLARIS_SPARK_CLIENT_ID"
CLIENT_SECRET_KEY = "POLARIS_SPARK_CLIENT_SECRET"

#Retorna 'clientId:clientSecret' do principal do Spark.
def get_polaris_credential(secrets_file=None) -> str:

    if secrets_file is None:
        secrets_file = os.environ.get(SECRETS_FILE_ENV)
        if not secrets_file:
            raise RuntimeError(f"Variável de ambiente '{SECRETS_FILE_ENV}' não definida.")

    secrets_file = Path(secrets_file)

    if not secrets_file.is_file():
        raise FileNotFoundError(f"Arquivo de credenciais '{secrets_file}' não encontrado. Execute o polaris-init.")

    values = {}

    for line in secrets_file.read_text(encoding="utf-8").splitlines():

        line = line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        else:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()

    for key in (CLIENT_ID_KEY, CLIENT_SECRET_KEY):
        if not values.get(key):
            raise RuntimeError(f"Chave '{key}' ausente ou vazia em '{secrets_file}'. Execute o polaris-init.")
    return f"{values[CLIENT_ID_KEY]}:{values[CLIENT_SECRET_KEY]}"
    
    return 