from pyspark.sql.functions import count, col

def calculate_population_by_department(df):
    return df.groupBy("departement").agg(count("name").alias("nb_people")).orderBy(col("nb_people").desc(), col("departement"))
