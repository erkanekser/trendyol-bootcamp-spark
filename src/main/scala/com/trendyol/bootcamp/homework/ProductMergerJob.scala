package com.trendyol.bootcamp.homework

import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.dense_rank

object ProductMergerJob {

  def main(args: Array[String]): Unit = {

    /**
    * Find the latest version of each product in every run, and save it as snapshot.
    *
    * Product data stored under the data/homework folder.
    * Read data/homework/initial_data.json for the first run.
    * Read data/homework/cdc_data.json for the nex runs.
    *
    * Save results as json, parquet or etc.
    *
    * Note: You can use SQL, dataframe or dataset APIs, but type safe implementation is recommended.
    */

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Product Merger Job")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val initialDataSchema = Encoders.product[commonDataSchema].schema
    val initialData = spark.read
      .schema(initialDataSchema)
      .json("data/homework/initial_data.json")
      .as[commonDataSchema]

    val cdcDataSchema = Encoders.product[commonDataSchema].schema
    val cdcData = spark.read
      .schema(cdcDataSchema)
      .json("data/homework/cdc_data.json")
      .as[commonDataSchema]

    cdcData.show()

    val w = Window
      .partitionBy('id)
      .orderBy('timestamp desc)

    val last_df = initialData
      .union(cdcData)
      .withColumn("rank",dense_rank() over w)
      .filter("rank = 1")
      .orderBy("timestamp")
      .drop("rank")

    last_df
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .json("data/homework/last_df")

  }

}

case class commonDataSchema(id: Int, name: String, category: String, brand: String, color: String, price: Double, timestamp: Long)