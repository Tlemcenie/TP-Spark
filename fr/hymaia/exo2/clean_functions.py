from pyspark.sql.functions import col, when, substring, regexp_extract

def filter_adult_clients(df):

    return df.filter(col("age") >= 18)

def join_city(df_clients, df_villes):
    return df_clients.join(df_villes, on="zip", how="inner")

def extract_department(df):
    return df.withColumn("departement", substring("zip", 1, 2))

def handle_corse_department(df):
    return df.withColumn(
        "departement",
        when((regexp_extract(col("zip"), "^[0-9]{5}$", 0) == ""), None)  # Gère les valeurs non numériques
        .when((col("zip") >= "20000") & (col("zip") <= "20190"), "2A")  # Corse-du-Sud
        .when((col("zip") >= "20200") & (col("zip") <= "20290"), "2B")  # Haute-Corse
        .otherwise(substring("zip", 1, 2))
    )
