from pyspark.sql import SparkSession

def run():
    ##### EJERCICIO 1
    print("*****EJERCICIO 1*****\n")
    spark = SparkSession.builder.master("local[*]").appName("Cap2").getOrCreate()
    
    ##### EJERCICIO 2
    print("*****EJERCICIO 2*****\n")
    rdd_vacio_0 = spark.sparkContext.emptyRDD()
    print(f'RDD vacio ----> {rdd_vacio_0.collect()}\nTiene 0 particiones ----> {rdd_vacio_0.getNumPartitions()}\n')
    
    rdd_vacio_parts = spark.sparkContext.parallelize([], 5)
    print(f'RDD vacio ----> {rdd_vacio_parts.collect()}\nTiene 5 particiones ----> {rdd_vacio_parts.getNumPartitions()}\n')
    
    ##### EJERCICIO 3
    print("*****EJERCICIO 3*****\n")
    rdd_primos = spark.sparkContext.parallelize([2,3,5,7,11,13,17,19], 5)
    print(f'RDD con números primos ----> {rdd_primos.collect()}\n')
    
    ##### EJERCICIO 4
    print("*****EJERCICIO 4*****\n")
    rdd = rdd_primos.filter(lambda x: x > 10)
    print(f'RDD primos mayores que 10 ---->{rdd.collect()}\n')
    
    ##### EJERCICIO 5
    print("*****EJERCICIO 5,A*****\n")
    rdd_data = spark.sparkContext.wholeTextFiles("./data/*")
    print(f'RDD con archivo en una sola linea ---->{rdd_data.collect()}\n')
    
    # Para saber la direccion del archivo solo hay que acceder a la primera columna de la lista
    print(f'Path del archivo ---->{rdd_data.collect()[0][0]}\n')
    
    print("*****EJERCICIO 5,B*****\n")
    rdd_data_2 = spark.sparkContext.textFile("./data/*")
    print(f'RDD con archivo separado por lineas ---->{rdd_data_2.collect()}\n')
if __name__ == "__main__":
    
    run()