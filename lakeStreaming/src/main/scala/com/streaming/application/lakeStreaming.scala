package com.streaming.application

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.amazonaws.services.kinesis.AmazonKinesis
import scala.collection.JavaConverters._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.AmazonKinesisClient
import org.apache.log4j.{Level, Logger}
import com.audienceproject.spark.dynamodb.implicits._
import java.util.Properties
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
import com.amazonaws.services.secretsmanager.model._
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.kinesis._
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import scala.collection.JavaConverters._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read,write}
import org.apache.spark.sql.SaveMode
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import com.amazonaws.awslabs.sparkstreaming.listener
import com.amazonaws.awslabs.sparkstreaming.listener._
import org.apache.spark.sql.types._


object LakehouseStreaming {
  def getRegionNameByEndpoint(endpoint: String): String = {
    val uri = new java.net.URI(endpoint)
    RegionUtils.getRegionsForService(AmazonKinesis.ENDPOINT_PREFIX)
      .asScala
      .find(_.getAvailableEndpoints.asScala.toSeq.contains(uri.getHost))
      .map(_.getName)
      .getOrElse(
        throw new IllegalArgumentException(s"Endpoint not found : $endpoint"))
  }

  def main(args: Array[String]) {

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val Array(appName, streamName) = args

    val conf = new SparkConf().setAppName(appName).setMaster("yarn")
                    .set("spark.sql.warehouse.dir", "hdfs:///user/spark/warehouse")
                    .set("spark.sql.catalogImplementation", "hive")
                    .set("spark.sql.hive.metastore.sharedPrefixes", "com.amazonaws.services.dynamodbv2")
                    .set("spark.databricks.hive.metastore.glueCatalog.enabled", "true")

    val ssc = new StreamingContext(conf, Seconds(100))

    val cwListener = new CloudWatchSparkListener("LakeStreaming")

    ssc.addStreamingListener(cwListener)

    println("Launching")
    println(streamName)
    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()
    val endpointUrl = "https://kinesis.us-east-2.amazonaws.com"

    println(credentials)
    val kinesisClient = new AmazonKinesisClient(credentials)
    kinesisClient.setEndpoint(endpointUrl)
    val numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards().size

    val numStreams = numShards

    val batchInterval = Seconds(100)

    val kinesisCheckpointInterval = batchInterval

    val regionName = getRegionNameByEndpoint(endpointUrl)

    val kinesisStreams = (0 until numStreams).map { i =>
      KinesisInputDStream.builder
        .streamingContext(ssc)
        .streamName(streamName)
        .endpointUrl(endpointUrl)
        .regionName(regionName)
        .initialPositionInStream(InitialPositionInStream.LATEST)
        .checkpointAppName(appName)
        .checkpointInterval(kinesisCheckpointInterval)
        .storageLevel(StorageLevel.MEMORY_ONLY)
        .build()
    }

    val unionStreams = ssc.union(kinesisStreams)

    val streamsArray = unionStreams.map(byteArray => new String(byteArray))

    streamsArray.foreachRDD { rdd =>
      if(!rdd.partitions.isEmpty){
          val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
          import spark.implicits._
          spark.sql("set spark.sql.caseSensitive=true")
          val inputStreamDataDF = spark.read.json(rdd)
          println(inputStreamDataDF.select(col("date_action")).show(10))

          val streamDataDFRepartition = inputStreamDataDF.repartition(700)
          streamDataDFRepartition.persist(StorageLevel.MEMORY_ONLY)

          val df_count = streamDataDFRepartition.count()
          println(df_count)

          val df_filtered = streamDataDFRepartition.select(col("table"), col("schema"))
                              .dropDuplicates(List("table", "schema"))
          val list_filters = df_filtered.select(col("table"), col("schema")).collect
          val par_list = list_filters.par
          par_list.foreach(i => runner(i.getString(0), i.getString(1), streamDataDFRepartition, spark))

          streamDataDFRepartition.unpersist()
      }

    }

    ssc.start()
    ssc.awaitTermination()
  }

  def runner(table: String, schema: String, df: DataFrame, spark: SparkSession): Unit = {
      spark.sparkContext.setLocalProperty("spark.scheduler.pool", "fair_pool")

      val ipAdress = spark.sparkContext.getConf.get("spark.yarn.historyServer.address")
      val ipSplit = ipAdress.split(":")

      val schema_map = getDBSchema(spark, schema, table)

      val df_filtered = df.filter((col("table") === table)
                                  && (col("schema") === schema))

      val deleteEvent = df_filtered.filter(col("log_type") === "DeleteRowsEvent")

      val upsertEvent = df_filtered.filter(col("log_type") !== "DeleteRowsEvent" )


      try{
          val dfUpsert = schemaType(upsertEvent, schema_map, table)
          eventToHudi(dfUpsert, schema, table, "upsert", ipSplit(0))

      } catch {
          case e: AnalysisException => println(s"No event $table")
          case writeEx: Throwable => println(s"problem upserting $table")
         // case writeEx: Throwable => handleWriteExceptions(writeEvent)
      }


      if(!deleteEvent.head(1).isEmpty){

          try{
              val dfDelete = schemaType(deleteEvent, schema_map, table)
              eventToHudi(dfDelete, schema, table, "delete", ipSplit(0))
          } catch {
              case e: AnalysisException => println(s"Delete Event $table")
              case _: Throwable => println(s"Problem Deleting $table")
          }
      }

  }

  def eventToHudi(df: DataFrame, database: String, table: String, event: String, ipAdress: String){
      val hudiTablePath = s"s3://mmlake-transformation-zone/lake_streaming/hudi_$database/$table"
      val table_id = getTableId(database, table)
      println(table_id)


      val hudiOptions = Map[String,String](
        DataSourceWriteOptions.HIVE_URL_OPT_KEY -> s"jdbc:hive2://$ipAdress:10000",
        DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> table_id,
        HoodieWriteConfig.TABLE_NAME -> s"$table",
        DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY -> "true",
        DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY -> s"hudi_$database",
        DataSourceWriteOptions.HIVE_TABLE_OPT_KEY -> s"$table",
        DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY -> "date_action",
        DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "date_action",
        DataSourceWriteOptions.HIVE_ASSUME_DATE_PARTITION_OPT_KEY -> "true",
        DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY ->
            classOf[MultiPartKeysValueExtractor].getName)

      if(event == "upsert"){
            df
            .orderBy(col("date_action").asc,
                     col("log_position").asc)
            .write
            .format("org.apache.hudi")
            .options(hudiOptions)
            .option("hoodie.upsert.shuffle.parallelism", "200")
            .mode(SaveMode.Append)
            .save(hudiTablePath)


      }

      else if(event == "delete"){
          df
          .orderBy(col("date_action").asc,
                   col("log_position").asc)
          .write
          .format("org.apache.hudi")
          .options(hudiOptions)
          .option(DataSourceWriteOptions.OPERATION_OPT_KEY,
                  DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
          .option(DataSourceWriteOptions.PAYLOAD_CLASS_OPT_KEY,
                  "org.apache.hudi.EmptyHoodieRecordPayload")
          .mode(SaveMode.Append)
          .save(hudiTablePath)
      }

  }

  def handleWriteExceptions(df: DataFrame): Unit = {
      val hudiTablePath = s"s3://dev-mmlake-transformation-zone/hudi_teste/streaming_error_handler"

      val hudiOptions = Map[String,String](
          HoodieWriteConfig.TABLE_NAME -> "streaming_error_handler",
          DataSourceWriteOptions.OPERATION_OPT_KEY ->
              DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
          DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "Date_Action",
          DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY -> "true",
          DataSourceWriteOptions.HIVE_TABLE_OPT_KEY -> "streaming_error_handler",
          DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY -> "Date_Action",
          DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY ->
              classOf[MultiPartKeysValueExtractor].getName)
  }

  def schemaType(df: DataFrame, schema_map: Map[String, String], table: String): DataFrame = {
      var typeDF = df
      schema_map.foreach{keyVal =>
          val column_name = "row.values." + keyVal._1
          typeDF = typeDF.withColumn(keyVal._1, col(column_name).cast(keyVal._2))
      }

      typeDF = typeDF.drop("row", "schema", "log_type", "table")
      typeDF = typeDF.withColumn("Action", lit("Streaming"))

      if(table == "b2c_pedped")
         typeDF = typeDF.withColumn("id_clicli", col("id_clicli").cast(StringType))

      return typeDF
  }


  def getDBSchema(spark: SparkSession, database: String, table: String): Map[String, String] = {
      import spark.implicits._
      implicit val formats = DefaultFormats

      val secrets = getSecret(database)
      val secrets_values = read[MySecretsManager](secrets)

      val engine = secrets_values.engine
      val host = secrets_values.host
      val port = secrets_values.port
      val user = secrets_values.username
      val password = secrets_values.password

      val match_types = Map(("bigint", "Long"), ("binary", "String"), ("bit", "Long"), ("char", "String"),
                      ("date", "Datetime"), ("datetime", "Datetime"), ("decimal", "Double"), ("enum", "String"),
                      ("float", "Double"), ("int", "Long"), ("json", "String"), ("longtext", "String"),
                      ("mediumint", "Long"), ("mediumtext", "String"), ("set", "String"), ("smallint", "Long"),
                      ("text", "String"), ("time", "String"), ("timestamp", "Timestamp"), ("tinyint", "Long"),
                      ("tinytext", "String"), ("varbinary", "String"), ("varchar", "String"), ("double", "Double"))

      val url = s"jdbc:mysql://$host:$port"
      val query = s"(SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '$database' AND TABLE_NAME = '$table') foo"
      val properties = new Properties()
      properties.put("user", user)
      properties.put("password", password)
      properties.put("driver", "com.mysql.jdbc.Driver")
      val jdbcDF = spark.read.jdbc(url, query, properties)

      val matchDF = jdbcDF.map(row=>{
          val value = row.getString(1)
          val new_types = match_types.get(value)
          (row.getString(0), new_types)
      })

      val types_map = matchDF.select(col("_1"), col("_2"))
                      .as[(String, String)].collect().toMap
      return types_map

  }

  def getTableId(database: String,  table_db: String): String = {
      val client = AmazonDynamoDBClientBuilder.standard().build()
      val dynamodb = new DynamoDB(client)

      val table = dynamodb.getTable("spark_streaming")
      val item = table.getItem("database", database, "table", table_db)
      val table_id = item.getString("table_id")

      return table_id
  }

  def getSecret(database: String): String = {
      val awsSecretId = s"lake/prod/$database/mysql"
      val aws_sm_client = AWSSecretsManagerClientBuilder.standard()
                          .withRegion("us-east-2")
                          .build()

      val getSecretValue = new GetSecretValueRequest()
                          .withSecretId(awsSecretId)

      var secret = ""

      try{
          val getSecretValueResult = aws_sm_client.getSecretValue(getSecretValue)

          secret = getSecretValueResult.getSecretString

      }catch {
          case decryptionEx :DecryptionFailureException => println("Unable to decrypt info ",decryptionEx)
          case internalEx :InternalServiceErrorException => println("Internal error ",internalEx)
          case invalidParamEx :InvalidParameterException => println("Invalid param error ",invalidParamEx)
          case invalidReqEx :InvalidRequestException => println("Invalid request error ",invalidReqEx)
          case resourceEx :ResourceNotFoundException => println("Resource not found error ",resourceEx)
      }

      return secret
  }

  case class MySecretsManager(username: String, password: String, host: String, port: String, dbname: String, engine: String)
}
