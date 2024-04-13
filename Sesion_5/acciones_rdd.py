from pyspark.sql import SparkSession
import math

def proceso(s):
    return s.replace('(', '').replace(')', '').split(', ')

def factorial(num, spark):
    if num == 0:
        return 1
    else:
        rdd_nums = spark.sparkContext.parallelize(list(range(1, num + 1)))

        factorial = rdd_nums.reduce(lambda x, y: x * y)

        return factorial
def run():

    spark = SparkSession.builder.master("local[*]").appName("Cap2").getOrCreate()

    ##### EJERCICIO 1
    print("*****EJERCICIO 1*****\n")
    rdd_importes = spark.sparkContext.textFile("./Sesion_5/data/num.txt")

    print(f'Número de regitros ----> {rdd_importes.count()}\n'
          f'Menor número ----> {rdd_importes.min()}\n'
          f'Mayor número ----> {rdd_importes.max()}')

    top_15 = rdd_importes.top(15)
    rdd_top_15 = spark.sparkContext.parallelize(top_15)

    print(f'RDD con el top 15 ---->{rdd_top_15.collect()}\n')

    rdd_top_15.repartition(1).saveAsTextFile("./Sesion_5/data/salida/")

    ##### EJERCICIO 2
    print("*****EJERCICIO 2*****\n")
    factorial_num = factorial(5, spark)

    print(f'Factorial de 5 ---->{factorial_num}\n')


if __name__ == "__main__":
    run()