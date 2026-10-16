import os
import json
import base64
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError

def get_vars():
    api_url = os.environ["POLARIS_API_URL"]
    realm = os.environ["POLARIS_REALM"]
    user = os.environ["POLARIS_USER"]
    root_client_secret = os.environ["POLARIS_ROOT_CLIENT_SECRET"]
    catalog = os.environ["POLARIS_CATALOG"]
    namespace = os.environ["POLARIS_NAMESPACE"]

    return (api_url, realm, user, root_client_secret,
            catalog, namespace)

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

def get_catalog(api_url, realm, token, catalog):

    request = Request(
        url = f"{api_url.rstrip('/')}/api/management/v1/catalogs/{catalog}",
        headers = {
            "Authorization": f"Bearer {token}",
            "Polaris-Realm": realm
        },
        method="GET"
    )

    try:
        with urlopen(request, timeout=15) as response:
           result = json.load(response)
    except HTTPError as error:
        if error.code == 404:
            return None
        raise
    
    return result

def create_catalog(api_url, realm, token, catalog_config):

    payload = {
        "catalog": catalog_config
    }

    request = Request(
        url = f"{api_url.rstrip('/')}/api/management/v1/catalogs",
        data = json.dumps(payload).encode("utf-8"),
        headers = {
            "Authorization": f"Bearer {token}",
            "Polaris-Realm": realm,
            "Content-type": "application/json"
        },
        method = "POST"
    )

    with urlopen(request, timeout=15) as response:
        pass

def get_namespace(api_url, realm, token, catalog, namespace):

    request = Request(
        url = f"{api_url.rstrip('/')}/api/catalog/v1/{catalog}/namespaces",
        headers = {
            "Authorization": f"Bearer {token}",
            "Polaris-Realm": realm
        },
        method = "GET"
    )

    with urlopen(request, timeout=15) as response:
        result = json.load(response)

    return [namespace] in result["namespaces"]

def create_namespace(api_url, realm, token, catalog, namespace):

    payload = {
        "namespace": [namespace]
    }

    request = Request(
        url = f"{api_url.rstrip('/')}/api/catalog/v1/{catalog}/namespaces",
        data = json.dumps(payload).encode("utf-8"),
        headers = {
            "Authorization": f"Bearer {token}",
            "Polaris-Realm": realm,
            "Content-type": "application/json"
        },
        method = "POST"
    )

    with urlopen(request, timeout=15) as response:
        pass

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

def main():

    (api_url, realm, user, root_client_secret,
     catalog, namespace) = get_vars()

    token = get_token(api_url, realm, user, root_client_secret)

    catalog_config = get_catalog_config(catalog)

    catalog_data = get_catalog(api_url, realm, token, catalog)

    if catalog_data is None:
        print(f"Catalogo '{catalog}' não encontrado.")
        create_catalog(api_url, realm, token, catalog_config)
        catalog_data = get_catalog(api_url, realm, token, catalog)

        if catalog_data is None:
            raise RuntimeError(f"Catalogo '{catalog}' não encontrado após a criação.")
        print(f"Catálogo '{catalog}' criado com sucesso.")
    else:
        print(f"Catalogo '{catalog}' encontrado.")

    validate_catalog(catalog_data, catalog_config)        

    namespace_data = get_namespace(api_url, realm, token, catalog, namespace)

    if namespace_data:
        print(f"Namespace {namespace} encontrado.")
    else:
        print(f"Namespace {namespace} não encontrado.")
        create_namespace(api_url, realm, token, catalog, namespace)
        namespace_data = get_namespace(api_url, realm, token, catalog, namespace)

        if not namespace_data:
            raise RuntimeError(f"Namespace '{namespace}' não encontrado após a criação.")
        print(f"Namespace '{namespace}' criado com sucesso.")

    return 0


if __name__ == "__main__":
    main()