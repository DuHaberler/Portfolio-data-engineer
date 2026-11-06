import os
import json
import sys
import time
import secrets
import tempfile

from minio import Minio
from minio.minioadmin import MinioAdmin
from minio.credentials import StaticProvider
from minio.error import MinioAdminException, S3Error

VALIDATION_BUCKET = "lakehouse"
VALIDATION_PREFIX = "analytics/"
INVALID_CREDENTIAL_CODES = {"InvalidAccessKeyId", "SignatureDoesNotMatch"}

def get_env_vars():
    return {
        "endpoint": os.environ["MINIO_ENDPOINT"],
        "root_user": os.environ["MINIO_ROOT_USER"],
        "root_password": os.environ["MINIO_ROOT_PASSWORD"],
        "buckets": [b.strip() for b in os.environ["MINIO_BUCKETS"].split(",") if b.strip()],
        "spark_user": os.environ["MINIO_SPARK_USER"],
        "policy_name": os.environ["MINIO_SPARK_POLICY"],
        "policy_file": os.environ["MINIO_POLICY_FILE"],
        "secrets_file": os.environ["MINIO_SECRETS_FILE"],
        "region": os.environ["MINIO_REGION"]
    }

def wait_for_minio(client, max_attempts=30, interval=2):
    for attempt in range(1, max_attempts+1):
        try:
            client.list_buckets()
            print(f"MinIO disponível (tentativa {attempt}).")
            return
        except Exception as e:
            print(f"MinIO indisponível (tentativa {attempt}/{max_attempts}): '{type(e).__name__}'")
            time.sleep(interval)
    raise RuntimeError(f"MinIO não respondeu após {max_attempts} tentativas.")

def ensure_buckets(client, buckets):

    for bucket in buckets:
            if client.bucket_exists(bucket):
                print(f"Bucket '{bucket}' já existe.")
                continue
    
            else:
                client.make_bucket(bucket)

            if not client.bucket_exists(bucket):
                raise RuntimeError(f"Falha na criação do bucket '{bucket}'.")
            else:
                print(f"Bucket '{bucket}' criado com sucesso.")

def load_policy(policy_file):
    with open(policy_file, encoding="utf-8") as file:
        policy = json.load(file)

    return policy

#explora as policies e ordena as listas para comparar os resultados.
def normalize(value):
    if isinstance(value, dict):
        return {k: normalize(v) for k, v in value.items()}
    if isinstance(value, list):
        items = [normalize(v) for v in value]
        return sorted(items, key=lambda v: json.dumps(v, sort_keys=True))
    return value

def ensure_policy(admin, policy_name, policy_file):
    desired = load_policy(policy_file)

    try:
        current = json.loads(admin.policy_info(policy_name))
        current = current.get("Policy", current)
    except Exception:
        current = None
    
    if normalize(current) == normalize(desired):
        print(f"Política '{policy_name}' já existente e atualizada")
        return

    else:
        admin.policy_add(policy_name, policy_file)

        #Valida a criação
        confirmed = json.loads(admin.policy_info(policy_name))
        confirmed = confirmed.get("Policy", confirmed)

        if normalize(confirmed) != normalize(desired):
            print(f"desired: '{normalize(desired)}'")
            print(f"confirmed: '{normalize(confirmed)}'")
            raise RuntimeError(f"Política '{policy_name}' divergente após a gravação.")

        action = "criada" if current is None else "atualizada"
        print(f"Política '{policy_name}' {action}")

def get_user(admin, user):
    try:
        return json.loads(admin.user_info(user))
    except MinioAdminException as e:
        if "XMinioAdminNoSuchUser" in str(e):
            return None
        raise

def generate_secret():
    return secrets.token_urlsafe(32)

def ensure_user(admin, spark_user):
    
    if get_user(admin, spark_user) is not None:
        print(f"Usuário '{spark_user}' já existe.")
        return None

    new_secret = generate_secret()
    admin.user_add(spark_user, new_secret)

    if get_user(admin, spark_user) is None:
        raise RuntimeError(f"Erro na criação do usuário '{spark_user}'.")

    return new_secret

def get_attached_policies(admin, spark_user):
    info = get_user(admin, spark_user) or {}
    return {p.strip() for p in info.get("policyName","").split(",") if p.strip()}


def ensure_policy_attached(admin, policy_name, spark_user):
    if policy_name in get_attached_policies(admin, spark_user):
        print(f"Política '{policy_name}' já associada ao usuário '{spark_user}'.")
        return

    admin.attach_policy([policy_name], user=spark_user)

    if policy_name not in get_attached_policies(admin, spark_user):
        raise RuntimeError(f"Falha ao associar a política '{policy_name}' ao usuário '{spark_user}'.")
    else:
        print(f"política '{policy_name}' associada ao usuário '{spark_user}'.")

def write_secrets(secrets_file, user, new_secret):
    directory = os.path.dirname(secrets_file)
    content = (
        f"MINIO_SPARK_ACCESS_KEY={user}\n"
        f"MINIO_SPARK_SECRET_KEY={new_secret}\n"
    )

    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".minio-credentials-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        os.replace(tmp_path, secrets_file)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise

    print(f"Credenciais gravadas em '{secrets_file}'")

def read_secrets(secrets_file):
    if not os.path.exists(secrets_file):
        return None

    values = {}
    with open(secrets_file, encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip()

    access_key = values.get("MINIO_SPARK_ACCESS_KEY")
    secret_key = values.get("MINIO_SPARK_SECRET_KEY")

    if not access_key or not secret_key:
        return None

    return access_key, secret_key

def reset_credentials(admin, vars):
    secret = generate_secret()

    admin.user_add(vars["spark_user"], secret)
    write_secrets(vars["secrets_file"], vars["spark_user"], secret)
    print(f"Credenciais do usuário '{vars["spark_user"]}' redefinidas.")

def validate_credentials(endpoint, region, access_key, secret_key):
    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False, region=region)
    try:
        #list_objects é lazy, next() força a requisição.
        next(iter(client.list_objects(VALIDATION_BUCKET, prefix=VALIDATION_PREFIX)), None)
        return True
    except S3Error as e:
        if e.code in INVALID_CREDENTIAL_CODES: #Access Denied é problema de política, por isso não está aqui.
            return False
        raise

def ensure_credentials(admin, vars, new_secret):
    user = vars["spark_user"]

    #Cenário 1: Usuário recém criado em ensure_user, arquivo é sobrescrito.
    if new_secret is not None:
        write_secrets(vars["secrets_file"], user, new_secret)
        return

    stored = read_secrets(vars["secrets_file"])

    #Cenário 2: usuário existe, mas arquivo incompleto ou ausente.
    if stored is None:
        reset_credentials(admin, vars)
        return

    access_key, secret_key = stored

    #Cenário 3:credenciais não batem
    if access_key != user:
        reset_credentials(admin, vars)
        return

    #Cenário 4: credencial válida
    if validate_credentials(vars["endpoint"], vars["region"], access_key, secret_key):
        print(f"Credenciais do usuário {user} validadas.")
        return

    #Cenário 5:credencial inválida
    reset_credentials(admin, vars)

def main():

    vars = get_env_vars()

    client = Minio(vars["endpoint"], access_key=vars["root_user"], secret_key=vars["root_password"], secure=False, region=vars["region"])
    admin = MinioAdmin(vars["endpoint"], credentials=StaticProvider(vars["root_user"], vars["root_password"]), secure=False, region=vars["region"])

    wait_for_minio(client)
    ensure_buckets(client, vars["buckets"])
    ensure_policy(admin, vars["policy_name"], vars["policy_file"])

    new_secret = ensure_user(admin, vars["spark_user"])
    ensure_policy_attached(admin, vars["policy_name"], vars["spark_user"])
    ensure_credentials(admin, vars, new_secret)

    return 0


if __name__ == "__main__":
    sys.exit(main())