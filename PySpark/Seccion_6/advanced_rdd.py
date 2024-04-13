from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

def run():

    spark = SparkSession.builder.master("local[*]").appName("Cap2").getOrCreate()

    ##### EJERCICIO 1
    print("*****EJERCICIO 1*****\n")
    rdd_importes = spark.sparkContext.textFile("./Seccion_6/data/rdd.txt")



    acumulator_count_ventas = spark.sparkContext.accumulator(0)
    acumulator_importe_ventas = spark.sparkContext.accumulator(0)

    rdd_importes.foreach(lambda x: acumulator_count_ventas.add(1))
    rdd_importes.foreach(lambda x: acumulator_importe_ventas.add(int(x)))

    print(f'Total de ventas ----> {acumulator_count_ventas.value}\n')
    print(f'Total importe de ventas ----> {acumulator_importe_ventas.value}\n')

    ##### EJERCICIO 2
    print("*****EJERCICIO 2*****\n")
    print("*****EJERCICIO 2.a.b*****\n")
    impuesto = 10
    br_impuesto = spark.sparkContext.broadcast(impuesto)
    rdd_importes_impuestos = rdd_importes.map(lambda x: int(x) - br_impuesto.value)
    print(f'Total de ventas ----> {rdd_importes_impuestos.collect()}\n')

    print("*****EJERCICIO 2.c*****\n")
    br_impuesto.unpersist()
    br_impuesto.destroy()

    try:
        print(f'Variable broadcast ----> {br_impuesto.value}\n')
    except:
        print("No existe variable\n")

    ##### EJERCICIO 3
    print("*****EJERCICIO 3*****\n")
    rdd_importes_impuestos.persist(StorageLevel.MEMORY_ONLY)
    print("Se ha alamcenado solo en memoria\n")
    rdd_importes_impuestos.unpersist()
    rdd_importes_impuestos.persist(StorageLevel.DISK_ONLY)
    print("Se ha alamcenado solo en disco\n")
    rdd_importes_impuestos.unpersist()
    rdd_importes_impuestos.persist(StorageLevel.MEMORY_AND_DISK)
    print("Se ha alamcenado en memoria y disco\n")

    spark.stop()

if __name__ == "__main__":
    run()