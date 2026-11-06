import os
from pathlib import Path

def read_secrets(secrets_file, env_var, required_keys, init_service) -> dict:

    if secrets_file is None:
        secrets_file = os.environ.get(env_var)
        if not secrets_file:
            raise RuntimeError(f"Variável de ambiente '{env_var}' não definida.")

    secrets_file = Path(secrets_file)

    if not secrets_file.is_file():
        raise FileNotFoundError(f"Arquivo de credenciais '{secrets_file}' não encontrado. Execute o '{init_service}'.")

    values = {}

    for line in secrets_file.read_text(encoding="utf-8").splitlines():

        line = line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()

    for key in required_keys:
        if not values.get(key):
            raise RuntimeError(f"Chave '{key}' ausente ou vazia em '{secrets_file}'. Execute o '{init_service}'.")

    return values

