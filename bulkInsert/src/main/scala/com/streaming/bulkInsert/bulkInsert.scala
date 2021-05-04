package com.streaming.bulkInsert

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.
    {StringType,LongType, IntegerType, DateType, TimestampType}
import org.apache.spark.sql._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark._

import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.amazonaws.services.dynamodbv2.document
import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.amazonaws.services.dynamodbv2.document.Item

import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
import com.amazonaws.services.secretsmanager.model._

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read,write}

import scala.collection.JavaConverters._
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Properties

//spark-submit --deploy-mode client --master yarn --executor-memory 18g --num-executors 8 --executor-cores 5 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.hive.convertMetastoreParquet=false --class com.streaming.bulkInsert.LakeInsertion s3://mmlake-jobs/streaming/bulkInsert-assembly-0.1.0-SNAPSHOT.jar

object LakeInsertion {
    val spark = SparkSession.builder()
    .appName("Hudi insertion")
    .master("yarn")
    .config("spark.sql.warehouse.dir", "hdfs:///user/spark/warehouse")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.databricks.hive.metastore.glueCatalog.enabled", "true")
   // .config("spark.kryo.registrator", "com.nvidia.spark.rapids.GpuKryoRegistrator")
   // .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
    .getOrCreate()

    val ipAdress = spark.sparkContext.getConf.get("spark.yarn.historyServer.address")
    val ipSplit = ipAdress.split(":")

    def main(args: Array[String]): Unit = {
        implicit val formats = DefaultFormats
        spark.sql("set spark.sql.caseSensitive=true")

        val client = AmazonDynamoDBClientBuilder.standard().build()
        val dynamodb = new DynamoDB(client)
        val table = dynamodb.getTable("mm-lakehouse")

        val result = table.scan()
        val iterator = result.iterator()

        while (iterator.hasNext()){
            val itemJson = iterator.next().toJSONPretty()
            val lakeTables = read[LakeTables](itemJson)
            println(lakeTables.database, lakeTables.table)

            val currentDate = dateAddDay(lakeTables.last_date, 1)
            println("date addedded")
            val dateSplit = currentDate.split("-")

            val year = dateSplit(0)
            val month = dateSplit(1)
            val day = dateSplit(2)

            // val dbDateList = getDBData(lakeTables.database, lakeTables.table)
            // println(s"Date List: $dbDateList")
            
            if(lakeTables.first_load) historicalHandler(lakeTables.database, lakeTables.table, lakeTables.table_id, currentDate, dateSplit)
            //else dailyHandler(lakeTables.database, lakeTables.table, lakeTables.table_id, currentDate, dateSplit)


            val updateTable = new Item()
            .withPrimaryKey("database", lakeTables.database)
            .withString("table", lakeTables.table)
            .withString("table_id", lakeTables.table_id)
            .withString("last_date", currentDate)
            .withBoolean("is_stream", false)
            .withBoolean("first_load", false)

            table.putItem(updateTable)
            println("DONE")
        }

    }

    def historicalHandler(database: String, table: String, table_id: String, currentDate: String, dateSplit: Array[String]): Unit = {
        var df = spark.read.parquet(s"s3://mmlake-landing-zone/debezium/sqoop_final/warmachine/pedido/")
        println("AFTER READING")
        df = df.withColumn("__op", lit("s"))
        df = df.withColumn("__table", lit(table))
        df = df.withColumn("__source_ts_ms", lit("1618790418000"))
        df = df.withColumn("__source_pos", lit(0))
        //df = df.withColumn("__source_ts_ms", col("__source_ts_ms").cast(LongType))
        df = df.withColumn("__source_pos", col("__source_pos").cast(LongType))
        val schemaMap = getDBSchema(database, table)
  
        df = df.na.fill(0)
        df = df.na.fill("")

        val dbSchema = schemaType(df, schemaMap, table)

        hudiEvent(dbSchema, database, table, table_id, "sqoop")
    }

    def dailyHandler(database: String, table: String, table_id: String, currentDate: String, dateSplit: Array[String]): Unit = {
        
        val df = spark.read.parquet(s"s3://mmlake-landing-zone/debezium/warmachine/madeiramadeira.mmlake.warmachine.warmachine.produto/$database/$table/$dateSplit(0)/$dateSplit(1)/$dateSplit(2)")
        
        val upsertEvent = df.filter(col("__op") === "c" || (col("__op") === "u"))
        val deleteEvent = df.filter(col("__op") === "d")
        hudiEvent(upsertEvent, database, table, table_id, "upsert")

        if(!deleteEvent.head(1).isEmpty)
            hudiEvent(deleteEvent, database, table, table_id, "delete")
    }

    def hudiEvent(df: DataFrame, database: String, table: String, tableId:String, event: String){
        val hudiTablePath = s"s3://dev-mmlake-presentation-zone/lake_batch/hudi_$database/$table"

        val hudiOptions = Map[String,String](
            DataSourceWriteOptions.HIVE_URL_OPT_KEY -> s"jdbc:hive2://$ipSplit(0):10000",
            DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> tableId,
            HoodieWriteConfig.TABLE_NAME -> s"$table",
            DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY -> "true",
            DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY -> s"hudi_$database",
            DataSourceWriteOptions.HIVE_TABLE_OPT_KEY -> s"$table",
            DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY -> "__source_ts_ms",
            DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "__source_ts_ms",
            DataSourceWriteOptions.HIVE_SUPPORT_TIMESTAMP -> "true",
            DataSourceWriteOptions.HIVE_ASSUME_DATE_PARTITION_OPT_KEY -> "true",
            DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY ->
                classOf[MultiPartKeysValueExtractor].getName)


        if (event == "sqoop"){
            df.write
            .format("org.apache.hudi")
            .options(hudiOptions)
            .option("hoodie.datasource.write.operation", "bulkinsert")
            .mode(SaveMode.Overwrite)
            .save(hudiTablePath)
        }

        else if(event == "upsert"){
            df
            .orderBy(col("__source_ts_ms").asc,
                    col("__source_pos").asc)
            .write
            .format("org.apache.hudi")
            .options(hudiOptions)
            .option("hoodie.upsert.shuffle.parallelism", "200")
            .mode(SaveMode.Append)
            .save(hudiTablePath)
        } 
        else {
            df
            .orderBy(col("__source_ts_ms").asc,
                    col("__source_pos").asc)
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

    def schemaType(df: DataFrame, schemaMap: Map[String, String], table: String): DataFrame = {
        var typeDF = df
        schemaMap.foreach{keyVal =>
            val column_name = keyVal._1
            typeDF = typeDF.withColumn(keyVal._1, col(column_name).cast(keyVal._2))
        }

        typeDF = typeDF.withColumn("Action", lit("Hinata"))

        if(table == "b2c_pedped")
            typeDF = typeDF.withColumn("id_clicli", col("id_clicli").cast(StringType))

        return typeDF
    }

    def getDBSchema(database: String, table: String): Map[String, String] = {
        import spark.implicits._
        implicit val formats = DefaultFormats

        val secrets = getSecret(database)
        val secretsValues = read[MySecretsManager](secrets)

        val engine = secretsValues.engine
        val host = secretsValues.host
        val port = secretsValues.port
        val user = secretsValues.username
        val password = secretsValues.password

        val matchTypes = Map(("bigint", "Long"), ("binary", "String"), ("bit", "Long"), ("char", "String"),
                        ("date", "Date"), ("datetime", "Timestamp"), ("decimal", "Double"), ("enum", "String"),
                        ("float", "Double"), ("int", "Long"), ("json", "String"), ("longtext", "String"),
                        ("mediumint", "Long"), ("mediumtext", "String"), ("set", "String"), ("smallint", "Long"),
                        ("text", "String"), ("time", "String"), ("timestamp", "Timestamp"), ("tinyint", "Long"),
                        ("tinytext", "String"), ("varbinary", "String"), ("varchar", "String"), ("double", "Double"))

        val url = s"jdbc:mysql://$host:$port?useSSL=false"
        val query = s"(SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '$database' AND TABLE_NAME = '$table') foo"
        val properties = new Properties()
        properties.put("user", user)
        properties.put("password", password)
        properties.put("driver", "com.mysql.cj.jdbc.Driver")
        val jdbcDF = spark.read.jdbc(url, query, properties)
    
        val matchDF = jdbcDF.map(row=>{
            val value = row.getString(1)
            val newTypes = matchTypes.get(value)
            (row.getString(0), newTypes)
        })

        val typesMap = matchDF.select(col("_1"), col("_2"))
                        .as[(String, String)].collect().toMap

        return typesMap
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


    def dateAddDay(date: String, days: Int) : String = {

        val dateAux = Calendar.getInstance()
        dateAux.setTime(new SimpleDateFormat("yyy-MM-dd").parse(date))
        dateAux.add(Calendar.DATE, days)
        return new SimpleDateFormat("yyy-MM-dd").format(dateAux.getTime())

    }

    case class LakeTables(database: String, table: String, table_id: String, last_date: String, first_load: Boolean)
    case class MySecretsManager(username: String, password: String, host: String, port: String, dbname: String, engine: String)

}