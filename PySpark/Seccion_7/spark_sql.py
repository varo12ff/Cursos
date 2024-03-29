from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col, count

def run():
    spark = SparkSession.builder.getOrCreate()
    
    ###Ejercicio 1
    print("\n****Ejercicio 1****\n")
    
    df_casos = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("./data/Case.csv")
    
    df_casos = df_casos.filter((col("city") != '-') & (col("city") != 'from other city')).orderBy(col("confirmed").desc()).select("province", "city", "confirmed").limit(3)

    df_casos.show()
    
    ###Ejercicio 2
    print("\n****Ejercicio 2.a****\n")

    df_patients = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("./data/PatientInfo.csv")
    
    df_patients = df_patients.na.drop(subset=['infected_by'])
    
    print("Los pacientes de los cuales se han reportado por quien han sido infectados son un total de: \n")
    df_patients.select(count("infected_by").alias("cont")).show()
    
    print("\n****Ejercicio 2.b****\n")
    df_patients_female = df_patients.filter((col("sex") == "female") & (col("sex").isNotNull())).drop("released_date", "deceased_date")
    
    df_patients_female.show()
    
    print("\n****Ejercicio 2.c****\n")
    print("Escribiendo...")
    df_patients_female.coalesce(2).write.partitionBy('province').mode("overwrite").parquet("./data/results")
        
if __name__ == '__main__':
    run()