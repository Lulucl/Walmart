# coding: utf-8

#commande spark-submit walmart.py, ou pyspark, et rentrer les instructions une à une
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, month, year
from pyspark.sql.types import IntegerType, FloatType
import sys



#1- Start a simple Spark Session
spark = SparkSession.builder.appName("Walmart Socket").getOrCreate()
sc = spark.sparkContext
#2- Load the Walmart Stock CSV File
#3- What are the columns names ?
# option("header", True) pour afficher les noms des colonnes
df = spark.read.option("header", True).csv("./../Data/walmart_stock.csv")


#4- What does the Schema look like ?
#df.show() affiche les 20 premieres lignes
df.printSchema()

colonne = df._jdf.schema().treeString()
sc.parallelize([colonne]).saveAsTextFile("./../OUTPUT/Question_3/")

#5- Create a new dataframe with a comuln called HV_Ratio that is the ratio of the High Price versus volume of stock traded for a day
df2 = df.withColumn("HV_Ratio", col("High")/col("Volume")) # Cree nouvelle colonne a partir de df
df2.show()


df2.write.option("header", True).csv("./../OUTPUT/Question_5/")

#6- What day had the peak High in Price ?spark-submit walmart.py

# Transformation du dataframe en table pour utiliser les fonctions orderBy, etc ...
df.createOrReplaceTempView("dftable")

# Methode DSL
res = df.orderBy(col("High").desc()).select("Date").head()
print(res)


sc.parallelize([res]).saveAsTextFile("./../OUTPUT/Question_6/DSL/")


# Methode SQL
spark.sql("select Date From dftable where High = (select max(High) from dftable)").show()

spark.sql("select Date From dftable where High = (select max(High) from dftable)").write.format("com.databricks.spark.csv").option("header", "true").save("./../OUTPUT/Question_6/SQL")

#7- What is the mean of the Close column ?
# Close est de type string, donc on le cast en Float !

df = df.withColumn("Close", col("Close").cast(FloatType()))
# On remet a jour la table de df
df.createOrReplaceTempView("dftable")
df.agg({"Close" : "mean"}).show() # DSL
spark.sql("select mean(Close) from dftable").show() # SQL


df.agg({"Close" : "mean"}).write.format("com.databricks.spark.csv").option("header", "true").save("./../OUTPUT/Question_7/DSL")


spark.sql("select mean(Close) from dftable").write.format("com.databricks.spark.csv").option("header", "true").save("./../OUTPUT/Question_7/SQL")

#8 What is the max and min of the Volume column ?
# Volume est de type string, donc on le cast en Int !
df = df.withColumn("Volume", col("Volume").cast(IntegerType()))
# On remet a jour la table de df
df.createOrReplaceTempView("dftable")
df.agg({"Volume" : "max"}).show()#DSL
spark.sql("select max(Volume) from dftable").show() #SQL

df.agg({"Volume" : "min"}).show()#DSL
spark.sql("select min(Volume) from dftable").show() # SQL


df.agg({"Volume" : "max"}).write.format("com.databricks.spark.csv").option("header", "true").save("./../OUTPUT/Question_8/max/DSL")

spark.sql("select max(Volume) from dftable").write.format("com.databricks.spark.csv").option("header", "true").save("./../OUTPUT/Question_8/max/SQL")


df.agg({"Volume" : "min"}).write.format("com.databricks.spark.csv").option("header", "true").save("./../OUTPUT/Question_8/min/DSL")


spark.sql("select min(Volume) from dftable").write.format("com.databricks.spark.csv").option("header", "true").save("./../OUTPUT/Question_8/min/SQL")

#9- How many days was the Close lower than 60 dollars ? 
nbjours = df.filter(df.Close < 60).count()
print(nbjours) # DSL

sc.parallelize([nbjours]).saveAsTextFile("./../OUTPUT/Question_9/")
#10- What pourcentage of the time was the High greater than 80 ?<=> (Days High > 80 / Total Days)*100
pourcent = (df.filter(df.High > 80).count() / float(df.count()))*100  # on cast en float pour Python
print(pourcent)


sc.parallelize([pourcent]).saveAsTextFile("./../OUTPUT/Question_10/")

#11- What is the max High per year ?
df.groupBy(year("Date").alias("Annee")).agg({"High" : "max"}).orderBy(col("Annee").desc()).show()


df.groupBy(year("Date").alias("Annee")).agg({"High" : "max"}).orderBy(col("Annee").asc()).write.format("com.databricks.spark.csv").option("header", "true").save("./../OUTPUT/Question_11")

#12- What is the Average Close for each calendar Month ?
df.groupBy(month("Date").alias("Mois")).agg({"Close" : "avg"}).orderBy(col("Mois").asc()).show()


df.groupBy(month("Date").alias("Mois")).agg({"Close" : "avg"}).orderBy(col("Mois").asc()).write.format("com.databricks.spark.csv").option("header", "true").save("./../OUTPUT/Question_12")
