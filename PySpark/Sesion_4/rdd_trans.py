from pyspark.sql import SparkSession
import math

def proceso(s):
    return s.replace('(', '').replace(')', '').split(', ')

def run():

    spark = SparkSession.builder.master("local[*]").appName("Cap2").getOrCreate()

    ##### EJERCICIO 1
    print("*****EJERCICIO 1*****\n")
    rdd_langs = spark.sparkContext.parallelize(["Pyhon", "R", "C", "Scala", "Rugby", "SQL"], 5)
    print(f'RDD con lenguajes ----> {rdd_langs.collect()}\n')

    print("*****EJERCICIO 1.a*****\n")
    rdd_langs_mayus = rdd_langs.map(lambda x: x.upper())
    print(f'RDD con todo mayuscula ----> {rdd_langs_mayus.collect()}\n')

    print("*****EJERCICIO 1.b*****\n")
    rdd_langs_minus = rdd_langs.map(lambda x: x.lower())
    print(f'RDD con todo minuscula ----> {rdd_langs_minus.collect()}\n')

    print("*****EJERCICIO 1.c*****\n")
    rdd_only_r = rdd_langs.filter(lambda x: x.startswith("R"))
    print(f'RDD con todo minuscula ----> {rdd_only_r.collect()}\n')

    ##### EJERCICIO 2
    print("*****EJERCICIO 2*****\n")
    nums = range(20, 31)
    rdd_int = spark.sparkContext.parallelize(nums, 5)
    rdd_pares = rdd_int.filter(lambda x : x % 2 == 0)
    print(f'RDD con numeros pares del 20 al 30 ----> {rdd_pares.collect()}\n')

    print("*****EJERCICIO 2.a*****\n")
    rdd_sqrt = rdd_pares.map(lambda x : math.sqrt(x))
    print(f'RDD con raices de numeros pares del 20 al 30 ----> {rdd_sqrt.collect()}\n')

    print("*****EJERCICIO 2.b*****\n")
    rdd_par_sqrt = rdd_pares.flatMap(lambda x: (x, math.sqrt(x)))
    print(f'RDD con raices de numeros pares del 20 al 30 ----> {rdd_par_sqrt.collect()}\n')

    print("*****EJERCICIO 2.c*****\n")
    rdd_sqrt = rdd_sqrt.repartition(20)
    print(f'RDD sqrt ----> {rdd_sqrt.collect()}\nTiene 20 particiones ----> {rdd_sqrt.getNumPartitions()}\n')

    print("*****EJERCICIO 2.d*****\n")
    print(f'Se haría con coallesce siempre y cuando no vaya a ser un cambio muy brusco.\n')

    ##### EJERCICIO 3
    print("*****EJERCICIO 3*****\n")
    rdd_data = spark.sparkContext.textFile("./Sesion_4/data/*")
    rdd_final_data = rdd_data.map(lambda x: proceso(x))
    rdd_final_data = rdd_final_data.reduceByKey(lambda x, y: x + y)
    print(f'RDD llave valor ----> {rdd_final_data.collect()}\n')









if __name__ == "__main__":
    run()