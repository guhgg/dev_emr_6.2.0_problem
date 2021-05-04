version := "0.1"

ThisBuild / scalaVersion := "2.12.10"
ThisBuild / organization := "MadeiraMadeira"

lazy val global = project
    .in(file("."))
    .aggregate(
        lakeStreaming,
        bulkInsert
    )


lazy val lakeStreaming = (project in file("lakeStreaming"))
    .settings(
        name:= "lakeStreaming",
        assemblySettings,
        libraryDependencies ++= commonDependencies ++ Seq(
            dependencies.guava,
            dependencies.awsJavaSdkDynamodb,
            dependencies.sparkStreaming,
            dependencies.sparkStreamingKinesisAsl,
            dependencies.jacksonDataformatCbor,
            dependencies.mysqlConnectorJava,
            dependencies.awsJavaSdkSecretsmanager
        )
    )

lazy val bulkInsert = (project in file("bulkInsert"))
    .settings(
        name:= "bulkInsert",
        assemblySettings,
        libraryDependencies ++= commonDependencies
    )


lazy val dependencies =
    new {
        val sparkVersion                    = "3.0.1"
        val sparkDynamodbVersion            = "0.4.1"
        val guavaVersion                    = "30.1-jre"
        val awsJavaSdkDynamodbVersion       = "1.11.466"
        val jacksonDataformatCborVersion    = "2.12.2"
        val mysqlConnectorJavaVersion       = "8.0.22"
        val json4sNativeVersion             = "3.6.10"
        val awsJavaSdkSecretsmanagerVersion = "1.11.886"
        val hudiSparkBundleVersion          = "0.8.0"
        val sparkAvroVersion                = "3.0.1"

        val sparkDynamodb            = "com.audienceproject"              %% "spark-dynamodb"                   % sparkDynamodbVersion
        val sparkSql                 = "org.apache.spark"                 %% "spark-sql"                        % sparkVersion
        val guava                    = "com.google.guava"                 % "guava"                             % guavaVersion
        val awsJavaSdkDynamodb       = "com.amazonaws"                    % "aws-java-sdk-dynamodb"             % awsJavaSdkDynamodbVersion
        val sparkStreaming           = "org.apache.spark"                 %% "spark-streaming"                  % sparkVersion
        val sparkStreamingKinesisAsl = "org.apache.spark"                 %% "spark-streaming-kinesis-asl"      % sparkVersion
        val sparkCore                = "org.apache.spark"                 %% "spark-core"                       % sparkVersion
        val jacksonDataformatCbor    = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor"           % jacksonDataformatCborVersion
        val mysqlConnectorJava       = "mysql"                            % "mysql-connector-java"              % mysqlConnectorJavaVersion
        val json4sNative             = "org.json4s"                       %% "json4s-native"                    % json4sNativeVersion
        val awsJavaSdkSecretsmanager = "com.amazonaws"                    % "aws-java-sdk-secretsmanager"       % awsJavaSdkSecretsmanagerVersion
        val hudiSparkBundle          = "org.apache.hudi"                  %% "hudi-spark-bundle"                % hudiSparkBundleVersion
        val sparkAvro                = "org.apache.spark"                 %% "spark-avro"                       % sparkAvroVersion
    }

lazy val commonDependencies = Seq(
    dependencies.sparkDynamodb,
    dependencies.awsJavaSdkSecretsmanager,
    dependencies.sparkSql,
    dependencies.sparkCore,
    dependencies.json4sNative,
    dependencies.hudiSparkBundle,
    dependencies.sparkAvro,
)


lazy val assemblySettings = Seq(
    assemblyMergeStrategy in assembly := {
        case PathList("META-INF", xs @ _*) => MergeStrategy.discard
        case x                             => MergeStrategy.first
    }
)
