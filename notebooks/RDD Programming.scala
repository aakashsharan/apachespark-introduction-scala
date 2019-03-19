// Databricks notebook source
// MAGIC %md
// MAGIC RDDs can be created by two ways: **parallelizing** an existing collection or referencing a dataset in an external storage system

// COMMAND ----------

val data = Array(1,11,21,31,41)
val parData = sc.parallelize(data)

// COMMAND ----------

val extFile = sc.textFile("/FileStore/tables/kjvb.txt")

// COMMAND ----------

// MAGIC %md
// MAGIC **RDD Operations:** transformation and action. Transformation create a new dataset from an existing one while actions return a value to the driver program after running a computation on the dataset. *Transformations are lazy*, in that they do not compute their results right away. Instead, they just remember the transformations applied to some dataset. The transformations are only computed when an action requires a result to be returned to the driver program.

// COMMAND ----------

val lineLengths = extFile.map(s => s.length) //tranformation
val totalLength = lineLengths.reduce((a,b) => a + b) //action
println("lineLengths are: "+lineLengths)
println("totalLength is: "+totalLength)

// COMMAND ----------

val lowerFile = extFile.map(line => line.toLowerCase)
val gods = lowerFile.filter(line => line.contains("god"))
val count = gods.count()

// COMMAND ----------

// MAGIC %md
// MAGIC Passing Functions

// COMMAND ----------

import org.apache.spark.rdd.RDD

def peek(rdd: RDD[_], n:Int = 5): Unit = {
  rdd.take(n).foreach(println)
  println("================")
}

// COMMAND ----------

peek(lowerFile)

// COMMAND ----------

// splitting each line to words. Eventually we will create a word count.
val words = lowerFile.flatMap(line => line.split("""[^\p{IsAlphabetic}]+"""))
peek(words)

// COMMAND ----------

val wordGroups = words.groupBy(word => word)
peek(wordGroups)

// COMMAND ----------

val wordCount = wordGroups.mapValues(group => group.size)
peek(wordCount, 15)

// COMMAND ----------

// MAGIC %md
// MAGIC We have used most of the usual RDD operations. Will keep updating this notebook as I keep learning Spark.
