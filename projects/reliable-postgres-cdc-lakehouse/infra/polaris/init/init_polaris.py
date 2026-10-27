import os
import json
import base64
import tempfile
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError

privileges_list = {"NAMESPACE_READ_PROPERTIES", "TABLE_LIST", "TABLE_CREATE", "TABLE_READ_DATA", "TABLE_WRITE_DATA"}

def get_vars():
    api_url = os.environ["POLARIS_API_URL"]
    realm = os.environ["POLARIS_REALM"]
    user = os.environ["POLARIS_USER"]
    root_client_secret = os.environ["POLARIS_ROOT_CLIENT_SECRET"]
    catalog = os.environ["POLARIS_CATALOG"]
    namespace = os.environ["POLARIS_NAMESPACE"]
    spark_principal = os.environ["SPARK_PRINCIPAL"]
    spark_principal_role = os.environ["SPARK_PRINCIPAL_ROLE"]
    catalog_role_name = os.environ["CATALOG_ROLE_NAME"]

    return (api_url, realm, user, root_client_secret, catalog, namespace,
            spark_principal, spark_principal_role, catalog_role_name)

def get_catalog_config(catalog):
    return {
        "name": catalog,
        "type": "INTERNAL",
        "properties": {
            "default-base-location": "s3://lakehouse/analytics/"
        },
        "storageConfigInfo": {
            "storageType": "S3",
            "allowedLocations": [
                "s3://lakehouse/analytics/"
            ],
            "endpoint": "http://minio:9000",
            "endpointInternal": "http://minio:9000",
            "region": "sa-east-1",
            "pathStyleAccess": True,
            "stsUnavailable": True
        }
    }

#Não utiliza polaris_request_json pois o contet-type não é application/json.
def get_token(api_url, realm, user, root_client_secret):

    credentials = f"{user}:{root_client_secret}"
    encoded_credentials = base64.b64encode(
        credentials.encode("utf-8")
    ).decode("ascii")

    payload = urlencode({
        "grant_type": "client_credentials",
        "scope": "PRINCIPAL_ROLE:ALL"
    }).encode("utf-8")

    request = Request(
        url=f"{api_url.rstrip('/')}/api/catalog/v1/oauth/tokens",
        data=payload,
        headers={
            "Authorization": f"Basic {encoded_credentials}",
            "Polaris-Realm": realm,
            "Content-Type": "application/x-www-form-urlencoded"
        },
        method="POST"
    )

    with urlopen(request, timeout=15) as response:
        result = json.load(response)

    return result["access_token"]

def polaris_request_json(url, token, realm, method="GET", payload=None, allow_404=False):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None

    request = Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Polaris-Realm": realm,
            "Content-type": "application/json"
        },
        method=method
    )

    try:
        with urlopen(request,timeout=15) as response:
            body = response.read()
            return json.loads(body) if body else None

    except HTTPError as error:
        if error.code == 404 and allow_404:
            return None

        body = error.read().decode("utf-8", errors="replace")

        raise RuntimeError(
            f"Polaris API retornou HTTP {error.code} em {method} {url}. Resposta: {body}"
        ) from error

def get_principal(management_url, token, realm, spark_principal):
    url = f"{management_url}/principals/{spark_principal}"
    result = polaris_request_json(
        url,
        token,
        realm, 
        allow_404=True
    )
    return result

def read_credentials(secrets_file):
    if not secrets_file.exists():
        return None

    values = {}

    for line in secrets_file.read_text(encoding = "utf-8").splitlines():
        if "=" in line and not line.startswith("#"):
            key, value = line.split("=", 1)
            values[key] = value

    client_id = values.get("POLARIS_SPARK_CLIENT_ID")
    client_secret = values.get("POLARIS_SPARK_CLIENT_SECRET")

    if not client_id or not client_secret:
        return None

    return client_id, client_secret

def validate_credentials(token_url, realm, stored_credentials):
    
    client_id, client_secret = stored_credentials
    credentials = f"{client_id}:{client_secret}"
    encoded_credentials = base64.b64encode(
        credentials.encode("utf-8")
    ).decode("ascii")

    data = urlencode({
        "grant_type": "client_credentials",
        "scope": "PRINCIPAL_ROLE:ALL"
    }).encode("utf-8")

    request = Request(
        token_url,
        data=data,
        headers={
            "Authorization": f"Basic {encoded_credentials}",
            "Polaris-Realm": realm,
            "Content-Type": "application/x-www-form-urlencoded"
        },
        method="POST"
    )

    try:
        with urlopen(request, timeout=30) as response:
            result = json.load(response)

        return bool(result.get("access_token"))

    except HTTPError as error:
        try:
            body = json.load(error)
        except (json.JSONDecodeError, UnicodeDecodeError):
            body = {}

        if error.code in (400, 401) and body.get("error") in ("invalid_client", "unauthorized_client"):
            return False

        raise RuntimeError(
            f"Falha ao validar credenciais: HTTP {error.code}, erro Oauth2: {body.get('error', 'desconhecido')}"
        ) from error

def reset_credentials(management_url, token, realm, spark_principal):
    url = f"{management_url}/principals/{spark_principal}/reset"

    return polaris_request_json(
        url,
        token,
        realm,
        method="POST"
    )

def create_principal(management_url, token, realm, spark_principal):
    url = f"{management_url}/principals"
    payload = {
        "principal":{
            "name": spark_principal
        }
    }
    return polaris_request_json(
        url,
        token,
        realm,
        method="POST",
        payload=payload
    )

def save_credentials(secrets_file, credentials):
    secrets_file.parent.mkdir(parents=True, exist_ok=True)

    client_id = credentials["clientId"]
    client_secret = credentials["clientSecret"]

    content = (
        f"POLARIS_SPARK_CLIENT_ID={client_id}\n"
        f"POLARIS_SPARK_CLIENT_SECRET={client_secret}"
    )

    fd, temporary_path = tempfile.mkstemp(
        prefix=".polaris-credentials-",
        dir=secrets_file.parent,
        text=True
    )

    try:
        os.chmod(temporary_path, 0o600)

        with os.fdopen(fd, "w", encoding="utf-8") as file:
            file.write(content)
            file.flush()
            os.fsync(file.fileno())

        os.replace(temporary_path, secrets_file)

    finally:
        if os.path.exists(temporary_path):
            os.unlink(temporary_path)

def ensure_spark_principal(api_url, token, realm, spark_principal):
    management_url = f"{api_url.rstrip('/')}/api/management/v1"
    token_url = f"{api_url.rstrip('/')}/api/catalog/v1/oauth/tokens"
    secrets_path = "/opt/polaris/secrets/.env.secrets"

    secrets_file = Path(secrets_path)
    principal = get_principal(management_url, token, realm, spark_principal)
    stored_credentials = read_credentials(secrets_file)

    #Cenario 1: principal e credentials existem
    if principal and stored_credentials:
        if validate_credentials(token_url, realm, stored_credentials):
            print(f"Principal '{spark_principal}' validado.")
            return

        #Credenciais existem mas são inválidas
        print("Credenciais inválidas. Solicitando redefinição.")
        result = reset_credentials(management_url, token, realm, spark_principal)

    #Cenário 2: principal não existe
    elif not principal:
        print(f"Principal '{spark_principal}' ausente.\nCriando Principal.")
        result = create_principal(management_url, token, realm, spark_principal)

    else:
        print("Credenciais ausentes, criando redefinição.")
        result = reset_credentials(management_url, token, realm, spark_principal)

    credentials = result["credentials"]

    save_credentials(secrets_file, credentials)

    new_credentials = (
        credentials["clientId"],
        credentials["clientSecret"]
    )

    try:
        valid = validate_credentials(token_url, realm, new_credentials)
    except Exception as error:
        raise RuntimeError(
            f"Credenciais do principal '{spark_principal}' persistidas, porém não foi possível valida-las."
        ) from error

    #Valida as novas credenciais
    if not valid:
        raise RuntimeError(
            f"Credenciais do principal '{spark_principal}' porém deu falha de validação."
        )
    else:
        print(f"Principal '{spark_principal}' configurado com sucesso.")

def get_catalog(api_url, token, realm, catalog):
    url = f"{api_url.rstrip('/')}/api/management/v1/catalogs/{catalog}"
    result = polaris_request_json(
        url,
        token,
        realm,
        allow_404=True
    )
    return result

def create_catalog(api_url, token, realm, catalog_config):
    payload = {
        "catalog": catalog_config
    }
    url = f"{api_url.rstrip('/')}/api/management/v1/catalogs"
    polaris_request_json(
        url,
        token,
        realm,
        method="POST",
        payload=payload
    )

def get_namespace(api_url, token, realm, catalog, namespace):
    url = f"{api_url.rstrip('/')}/api/catalog/v1/{catalog}/namespaces"
    result = polaris_request_json(url, token, realm)
    return [namespace] in result["namespaces"]

def create_namespace(api_url, token, realm, catalog, namespace):
    payload = {
        "namespace": [namespace]
    }
    url = f"{api_url.rstrip('/')}/api/catalog/v1/{catalog}/namespaces"
    polaris_request_json(
        url,
        token,
        realm,
        method="POST",
        payload=payload
    )

def validate_catalog(catalog_data, catalog_config):
    divergences = []

    def compare(actual, expected, path=""):
        for key, expected_value in expected.items():

            #Acopla prefixo da key caso seja a recursão de um campo de dicionário
            current_path = f"{path}.{key}" if path else key

            #Verifica se a propriedade existe
            if key not in actual:
                divergences.append(
                    f"{current_path}: propriedade ausente."
                )
                continue
            actual_value = actual[key]

            #Se o valor esperado é um dicionário, compara recursivamente
            if isinstance(expected_value, dict):
                if not isinstance(actual_value, dict):
                    divergences.append(
                        f"{current_path}: esperado um dicionário, encontrado {actual_value!r}"
                    )
                    continue
                compare(actual_value, expected_value, current_path)

            #Caso contrário, compara diretamente
            elif actual_value != expected_value:
                divergences.append(
                    f"{current_path}: esperado {expected_value!r}, encontrado {actual_value!r}"
                )

    compare(catalog_data, catalog_config)

    if divergences:
        raise RuntimeError(
            f"Configurações divergentes no catálogo {catalog_config['name']!r}: \n"
            + "\n".join(f"- {error}" for error in divergences)
        )

#para testes.
def delete_principal(management_url, token, realm, spark_principal):
    url = f"{management_url}/principals/{spark_principal}"
    polaris_request_json(url, token, realm, method="DELETE")

def get_principal_role(management_url, token, realm, spark_principal_role):
    url = f"{management_url.rstrip('/')}/principal-roles/{spark_principal_role}"
    result = polaris_request_json(
        url,
        token,
        realm,
        allow_404=True
    )
    return result

def create_principal_role(management_url, token, realm, spark_principal_role):
    url = f"{management_url.rstrip('/')}/principal-roles"
    payload = {
        "principalRole": {
            "name": spark_principal_role
        }
    }
    result = polaris_request_json(
        url,
        token,
        realm,
        method="POST",
        payload=payload,
    )
    return result

def get_list_principal_roles(management_url, token, realm, spark_principal):
    url = f"{management_url.rstrip('/')}/principals/{spark_principal}/principal-roles"
    result = polaris_request_json(
        url,
        token,
        realm
    )
    return result

def assign_principal_role(management_url, token, realm, spark_principal, spark_principal_role):
    url = f"{management_url.rstrip('/')}/principals/{spark_principal}/principal-roles"
    payload = {
        "principalRole": {
            "name": spark_principal_role
        }
    }
    polaris_request_json(
        url,
        token,
        realm,
        method="PUT",
        payload=payload
    )

def is_role_assigned(list_roles, role_name):
    is_assigned = False
    for role in list_roles["roles"]:
            if role["name"] == role_name:
                is_assigned = True
    return is_assigned

def ensure_spark_principal_role(management_url, token, realm, spark_principal, spark_principal_role):
    principal_role = get_principal_role(management_url, token, realm, spark_principal_role)

    #Se o principal role não existe, cria.
    if principal_role is None:
        create_principal_role(management_url, token, realm, spark_principal_role)

        principal_role = get_principal_role(management_url, token, realm, spark_principal_role)

        if principal_role is None:
            raise RuntimeError(f"Não foi possível criar o principal role '{spark_principal_role}', investigação necessária.")

        print(f"Principal role '{spark_principal_role}' criada com sucesso.")
    else:
        print(f"Principal role '{spark_principal_role}' validada.")

    #Se o principal não estiver associado ao principal role, associa.
    list_principal_roles = get_list_principal_roles(management_url, token, realm, spark_principal)

    is_assigned = is_role_assigned(list_principal_roles, spark_principal_role)
    
    if not is_assigned:
        assign_principal_role(management_url, token, realm, spark_principal, spark_principal_role)

        #valida novamente
        list_principal_roles = get_list_principal_roles(management_url, token, realm, spark_principal)
        is_assigned = is_role_assigned(list_principal_roles, spark_principal_role)

        if not is_assigned:
            raise RuntimeError(f"Falha na associação do principal '{spark_principal}' ao principal role '{spark_principal_role}'")
        else:
            print(f"Principal '{spark_principal}' associado ao principal_role '{spark_principal_role}' com sucesso.")

    else:
        print(f"Principal role '{spark_principal_role}' validada e  principal '{spark_principal}' já associado.")

def get_catalog_role(management_url, token, realm, catalog, catalog_role_name):
    url = f"{management_url.rstrip('/')}/catalogs/{catalog}/catalog-roles/{catalog_role_name}"
    result = polaris_request_json(
        url,
        token,
        realm,
        allow_404=True
    )
    return result

def create_catalog_role(management_url, token, realm, catalog, catalog_role_name):
    url = f"{management_url.rstrip('/')}/catalogs/{catalog}/catalog-roles"
    payload = {
        "catalogRole": {
            "name": catalog_role_name
        }
    }
    polaris_request_json(
        url,
        token,
        realm,
        method="POST",
        payload=payload
    )

def get_list_catalog_roles(management_url, token, realm, catalog, spark_principal_role):
    url = f"{management_url.rstrip('/')}/principal-roles/{spark_principal_role}/catalog-roles/{catalog}"
    result = polaris_request_json(
        url,
        token,
        realm
    )
    return result

def assign_catalog_role(management_url, token, realm, catalog, spark_principal_role, catalog_role_name):
    url = f"{management_url.rstrip('/')}/principal-roles/{spark_principal_role}/catalog-roles/{catalog}"
    payload = {
        "catalogRole": {
            "name": catalog_role_name
        }
    }
    polaris_request_json(
        url,
        token,
        realm,
        method="PUT",
        payload=payload
    )

def ensure_catalog_role(management_url, token, realm, catalog, catalog_role_name, spark_principal_role):
    catalog_role = get_catalog_role(management_url, token, realm, catalog, catalog_role_name)
    
    #Se a catalog role não existe, cria.
    if catalog_role is None:
        create_catalog_role(management_url, token, realm, catalog, catalog_role_name)
        catalog_role = get_catalog_role(management_url, token, realm, catalog, catalog_role_name)

        if catalog_role is None:
            raise RuntimeError(f"Não foi possível criar a Catalog role '{catalog_role_name}', investigação necessária.")

        print(f"Catalog role '{catalog_role_name}' criada com sucesso.")
    else:
        print(f"Catalog role '{catalog_role_name}' validada.")

    #Se a catalog role não estiver associada ao principal_role, associa.
    list_catalog_roles = get_list_catalog_roles(management_url, token, realm, catalog, spark_principal_role)
    is_assigned = is_role_assigned(list_catalog_roles, catalog_role_name)
    
    if not is_assigned:
        assign_catalog_role(management_url, token, realm, catalog, spark_principal_role, catalog_role_name)

        #valida novamente
        list_catalog_roles = get_list_catalog_roles(management_url, token, realm, catalog, spark_principal_role)
        is_assigned = is_role_assigned(list_catalog_roles, catalog_role_name)

        if not is_assigned:
            raise RuntimeError(f"Falha na associação da catalog role '{catalog_role_name}' à principal role '{spark_principal_role}'")
        else:
            print(f"Catalog Role '{catalog_role_name}' associada à principal role '{spark_principal_role}' com sucesso.")

    else:
        print(f"Catalog role '{catalog_role_name}' validada e já associada à principal role '{spark_principal_role}'.")

def get_catalog_role_privileges(management_url, token, realm, catalog, catalog_role_name):
    url = f"{management_url.rstrip('/')}/catalogs/{catalog}/catalog-roles/{catalog_role_name}/grants"
    result = polaris_request_json(
        url,
        token,
        realm
    )
    return result

def get_privileges_existing(privileges_granted, namespace):
    result = {
        grant["privilege"]
        for grant in privileges_granted["grants"]
        if grant["type"] == "namespace" and grant["namespace"] == [namespace]
    }
    return result

def put_catalog_role_privileges(management_url, token, realm, catalog, catalog_role_name, namespace, privilege):
    url = f"{management_url.rstrip('/')}/catalogs/{catalog}/catalog-roles/{catalog_role_name}/grants"
    payload = {
        "grant": {
            "type": "namespace",
            "namespace": [namespace],
            "privilege": privilege
        }
    }
    polaris_request_json(
        url,
        token,
        realm,
        method="PUT",
        payload=payload
    )

def ensure_catalog_role_privileges(management_url, token, realm, catalog, namespace, catalog_role_name):
    catalog_role_privileges = get_catalog_role_privileges(management_url, token, realm, catalog, catalog_role_name)
    privileges_existing = get_privileges_existing(catalog_role_privileges, namespace)

    privileges_missing = privileges_list - privileges_existing #privilege faltando
    privileges_extras = privileges_existing - privileges_list #privilege sobrando

    if privileges_extras:
        print(f"ATENÇÃO: O catalog role '{catalog_role_name}' possui privileges fora da política no namespace '{namespace}': {sorted(privileges_extras)}")

    if privileges_missing:
        for privilege in sorted(privileges_missing):
            put_catalog_role_privileges(management_url, token, realm, catalog, catalog_role_name, namespace, privilege)
            print(f"Privilege '{privilege}' concedido à catalog role '{catalog_role_name}' no namespace '{namespace}'.")

        #valida de novo
        catalog_role_privileges = get_catalog_role_privileges(management_url, token, realm, catalog, catalog_role_name)
        privileges_existing = get_privileges_existing(catalog_role_privileges, namespace)

        privileges_still_missing = privileges_list - privileges_existing #privilege faltando novamente

        if privileges_still_missing:
            raise RuntimeError(f"Privileges não confirmados na catalog role '{catalog_role_name}' no namespace '{namespace}': {sorted(privileges_still_missing)}")

        print(f"Privileges da catalog role '{catalog_role_name}' no namespace '{namespace}' configurados com sucesso: {sorted(privileges_list)}")

    else:
        print(f"Política de privileges da catalog role '{catalog_role_name}' no namespace '{namespace}' já concedidos")

def main():
    (api_url, realm, user, root_client_secret, catalog, namespace,
     spark_principal, spark_principal_role, catalog_role_name) = get_vars()

    token = get_token(api_url, realm, user, root_client_secret)

    #Criação dos recursos: catalogo (Polaris) e namespace (Iceberg)
    catalog_config = get_catalog_config(catalog)
    catalog_data = get_catalog(api_url, token, realm, catalog)

    if catalog_data is None:
        print(f"Catalogo '{catalog}' não encontrado.")
        create_catalog(api_url, token, realm, catalog_config)
        catalog_data = get_catalog(api_url, token, realm, catalog)

        if catalog_data is None:
            raise RuntimeError(f"Catalogo '{catalog}' não encontrado após a criação.")
        print(f"Catálogo '{catalog}' criado com sucesso.")
    else:
        print(f"Catalogo '{catalog}' encontrado.")

    validate_catalog(catalog_data, catalog_config)        

    namespace_data = get_namespace(api_url, token, realm, catalog, namespace)

    if namespace_data:
        print(f"Namespace '{namespace}' encontrado.")
    else:
        print(f"Namespace '{namespace}' não encontrado.")
        create_namespace(api_url, token, realm, catalog, namespace)
        namespace_data = get_namespace(api_url, token, realm, catalog, namespace)

        if not namespace_data:
            raise RuntimeError(f"Namespace '{namespace}' não encontrado após a criação.")
        print(f"Namespace '{namespace}' criado com sucesso.")

    #Validação e, caso não exista, criação do permissionamento completo (principal, principal_role, catalog_role e privileges)
    
    ensure_spark_principal(api_url, token, realm, spark_principal)

    management_url = f"{api_url.rstrip('/')}/api/management/v1"
    ensure_spark_principal_role(management_url, token, realm, spark_principal, spark_principal_role)
    ensure_catalog_role(management_url, token, realm, catalog, catalog_role_name, spark_principal_role)
    ensure_catalog_role_privileges(management_url, token, realm, catalog, namespace, catalog_role_name)
    
    return 0


if __name__ == "__main__":
    main()