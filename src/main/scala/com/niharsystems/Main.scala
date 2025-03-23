package com.niharsystems

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import io.delta.tables._
import org.apache.spark.sql.functions._

object Main extends App {
    val deltaTableBucket = "s3a://vj-bucket"
    val deltaTablePath = s"$deltaTableBucket/delta-table-"+java.time.LocalDateTime.now.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd-HHmm"))
    val onlyDisplay = false
    val isInsert = !onlyDisplay && true
    val isUpdate = !onlyDisplay && !isInsert
    val isDelete = false

    // Create SparkSession
    val spark = SparkSession.builder
      .appName("Simple Spark Example")
      .master("local[*]")
      .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.0")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      .config("spark.hadoop.fs.s3a.access.key", "2nZqqHPWEzu9JooKNoXO")
      .config("spark.hadoop.fs.s3a.secret.key", "DfFaWePTJsp5mB50pS2a7Iz00A6AgJEmdXWGyIOx")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .getOrCreate()

    try {
      // Read JSON data
      val df1 = spark.read.option("multiline", "true").json(s"$deltaTableBucket/sample.json")
      df1.printSchema()
      df1.select("user.address.zip").show()

      // Create dataset
      val data = List(
        Row(1, "Vijay Donthireddy", 50, "Engineering", 80, false),
        Row(2, "Kavitha Padera", 47, "Manager", 80, false),
        Row (3, "Nihar Donthireddy", 18, "College", 80, false),
        Row (4, "Nirav1 Donthireddy", 12, "Middle School", 83, false),
        Row (5, "Nirav2 Donthireddy", 12, "High School", 90, false),
        Row(6, "Nirav3 Donthireddy", 12, "Elementary School", 83, false),
        Row(7, "Nirav4 Donthireddy", 12, "University of CA LA", 76, false)
      )

      // Create schema
      val schema1 = StructType(Array(
        StructField("id", IntegerType, false),
        StructField("name", StringType, false),
        StructField("age", IntegerType, false),
        StructField("department", StringType, false),
        StructField("marks", IntegerType, false),
        StructField("isdeleted", BooleanType, false)
      ))

      if (onlyDisplay) {
        println("Vijay Display Only:")
        val dtReadOnly = DeltaTable.forPath(spark, deltaTablePath)

        // Various sorting examples
        dtReadOnly.toDF.orderBy("id").show()
        dtReadOnly.toDF.orderBy("age").show()
        dtReadOnly.toDF.orderBy(lower(col("name"))).show()
        dtReadOnly.toDF.orderBy(col("marks"), lower(col("name"))).show()

        spark.read
          .format("delta")
          .load(deltaTablePath)
          .orderBy("isdeleted")
          .show()

      } else {
        if (isInsert) {
          val rdd = spark.sparkContext.parallelize(data)
          val df = spark.createDataFrame(rdd, schema1)
          println("Vijay Schema:")
          df.printSchema()

          df.write
            .format("delta")
            .mode("append")
            .partitionBy("id")
            .save(deltaTablePath)

          spark.read
            .format("delta")
            .load(deltaTablePath)
            .show()
        }

        if (isUpdate) {
          val dfRead = spark.read
            .format("delta")
            .load(deltaTablePath)
            .orderBy("id")
          dfRead.show()

          val dtPeople = DeltaTable.forPath(spark, deltaTablePath)

          // New data for update
          val newData = List(
            Row(1, "Vijay Donthireddy", 49, "Engineering", 98, true),
            Row(2, "Kavitha Padera", 47, "Manager", 99, true),
            Row(8, "new Dummy Donthireddy", 20, "UCI", 88, false),
            Row(3, "Nihar Donthireddy", 18, "College", 98, false),
            Row(4, "Nirav1 Donthireddy", 12, "Middle School", 83, false),
            Row(5, "Nirav2 Donthireddy", 12, "High School", 98, false),
            Row(6, "Nirav3 Donthireddy", 12, "Elementary School", 98, false),
            Row(7, "Nirav4 Donthireddy", 12, "University of CA LA", 98, false)
          )

          val rddNew = spark.sparkContext.parallelize(newData)
          val newDF = spark.createDataFrame(rddNew, schema1)

          val mapUpdate = Map(
            "oldData.id" -> "newData.id",
            "oldData.name" -> "newData.name",
            "oldData.age" -> "newData.age",
            "oldData.marks" -> "newData.marks",
            "oldData.department" -> "newData.department",
            "oldData.isdeleted" -> "newData.isdeleted"
          )

          val mapInsert = Map(
            "id" -> "newData.id",
            "name" -> "newData.name",
            "age" -> "newData.age",
            "marks" -> "newData.marks",
            "department" -> "newData.department",
            "isdeleted" -> "newData.isdeleted"
          )

          dtPeople.as("oldData")
            .merge(
              newDF.as("newData"),
              "oldData.id = newData.id")
            .whenMatched()
            .updateExpr(mapUpdate)
            .whenNotMatched()
            .insertExpr(mapInsert)
            .execute()

          println("Vijay After updated:")
          spark.read
            .format("delta")
            .load(deltaTablePath)
            .orderBy("id")
            .show()
        }

        if (isDelete) {
          println("Vijay - Deleting rows")
          val dtDelete = DeltaTable.forPath(spark, deltaTablePath)
          val deleteCondition = "age > 75"
          dtDelete.delete(deleteCondition)
        }
      }
    } finally {
      spark.stop()
    }
}
