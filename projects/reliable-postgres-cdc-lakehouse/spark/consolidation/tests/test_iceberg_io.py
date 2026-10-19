import os
from uuid import uuid4
from pyspark.sql import SparkSession

TABLE_NAME = f"lakehouse.analytics.test_iceberg_io{uuid4().hex}"

def main():
    polaris_secret = os.environ["POLARIS_ROOT_CLIENT_SECRET"]
    minio_user = os.environ["MINIO_ROOT_USER"]
    minio_password = os.environ["MINIO_ROOT_PASSWORD"]

    spark = (
        SparkSession.builder
        .appName("test-iceberg-io")
        .config(
            "spark.sql.catalog.lakehouse.credential",
            f"root:{polaris_secret}"
        )
        .config(
            "spark.sql.catalog.lakehouse.s3.access-key-id",
            minio_user
        )
        .config(
            "spark.sql.catalog.lakehouse.s3.secret-access-key",
            minio_password
        )
        .getOrCreate()
    )

    table_created = False

    try:
        #Criar tabela Iceberg
        print("\nCriando tabela Iceberg: \n")
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id BIGINT,
                description STRING
            )
            USING iceberg
            """
        )
        table_created = True

        #Inserir registros
        print("\nInserindo registros: \n")
        spark.sql(
            f"""
            INSERT INTO {TABLE_NAME}
            VALUES (1, 'primeiro'), (2, 'segundo'), (3, 'terceiro')
            """
        )

        #Ler registros
        print("\nConsultando registros: \n")
        result_df = spark.sql(
            f"""
            SELECT id, description
            FROM {TABLE_NAME}
            ORDER BY id
            """
        )

        result_df.show(truncate=False)

        #Validar resultado
        print("\nValidando resultado: \n")

        actual = [
            (row.id, row.description)
            for row in result_df.collect()
        ]

        expected = [
            (1,"primeiro"),
            (2,"segundo"),
            (3,"terceiro")
        ]

        assert actual == expected, (
            f"Resultado inesperado.\n Esperado: {expected}\n Obtido: {actual}"
        )
        print("\nTeste concluído com sucesso.")
    finally:
        try:
            #Apagando tabela de teste criada
            print("\nApagando tabela de teste: \n")
            if table_created:
                spark.sql(f"DROP TABLE {TABLE_NAME} PURGE")
        finally:
            spark.stop()
            
if __name__ == "__main__":
    main()