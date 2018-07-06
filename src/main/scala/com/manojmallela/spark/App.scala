package com.manojmallela.spark

import org.apache.spark.sql.SparkSession

object App {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    val schema = spark.read
      .format("com.manojmallela.spark.DatasourceReader")
      .option("path", "~/caliPlaces-avro/")
      .load()


    //schema.printSchema()
    schema.select("FEATURE_NAME").show(false)




  }

}
