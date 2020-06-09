package kafka

import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.classification._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.tuning._
import org.apache.log4j._
import org.apache.spark.streaming._
import org.apache.spark.ml._

object mainNetwork {
  case class Conection (duration: Integer,protocol_type: String,service: String,flag: String,src_bytes: Integer,
                        dst_bytes: Integer ,land: Integer,wrong_fragment: Integer,urgent: Integer,hot: Integer,num_failed_logins: Integer,logged_in: Integer,
                        num_compromised: Integer,root_shell: Integer,su_attempted: Integer,num_root:Integer,num_file_creations: Integer,
                        num_shells: Integer, num_access_files: Integer,num_outbound_cmds: Integer ,is_host_login: Integer ,is_guest_login: Integer,
                        count : Integer, srv_count: Integer,serror_rate: Double, srv_serror_rate: Double,
                        rerror_rate: Double ,srv_rerror_rate: Double, same_srv_rate: Double, diff_srv_rate: Double, srv_diff_host_rate: Double ,
                        dst_host_count: Integer,  dst_host_srv_count: Integer ,dst_host_same_srv_rate: Double , dst_host_diff_srv_rate: Double ,
                        dst_host_same_src_port_rate: Double, dst_host_srv_diff_host_rate: Double , dst_host_serror_rate: Double,
                        dst_host_srv_serror_rate: Double , dst_host_rerror_rate: Double, dst_host_srv_rerror_rate: Double, label_sting: String, difficulty_level: Integer )

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("Streaming").master("local").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions","10")
    val data = spark.read.
      option("inferSchema", true).
      option("header", false).
      csv(args(0)).
      toDF(
        "duration", "protocol_type", "service", "flag",
        "src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent",
        "hot", "num_failed_logins", "logged_in", "num_compromised",
        "root_shell", "su_attempted", "num_root", "num_file_creations",
        "num_shells", "num_access_files", "num_outbound_cmds",
        "is_host_login", "is_guest_login", "count", "srv_count",
        "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate",
        "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate",
        "dst_host_count", "dst_host_srv_count",
        "dst_host_same_srv_rate", "dst_host_diff_srv_rate",
        "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate",
        "dst_host_serror_rate", "dst_host_srv_serror_rate",
        "dst_host_rerror_rate", "dst_host_srv_rerror_rate",
        "label_sting","difficulty_level")

    val testData = spark.read.
      option("inferSchema", true).
      option("header", false).
      csv(args(1)).
      toDF(
        "duration", "protocol_type", "service", "flag",
        "src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent",
        "hot", "num_failed_logins", "logged_in", "num_compromised",
        "root_shell", "su_attempted", "num_root", "num_file_creations",
        "num_shells", "num_access_files", "num_outbound_cmds",
        "is_host_login", "is_guest_login", "count", "srv_count",
        "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate",
        "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate",
        "dst_host_count", "dst_host_srv_count",
        "dst_host_same_srv_rate", "dst_host_diff_srv_rate",
        "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate",
        "dst_host_serror_rate", "dst_host_srv_serror_rate",
        "dst_host_rerror_rate", "dst_host_srv_rerror_rate",
        "label_sting","difficulty_level")

    data.cache()
    testData.cache()
    data.show()
    //     data.printSchema()
    //     data.select("label").groupBy("label").count().orderBy(desc("count")).show(25)
    val  dataLabel=data.withColumn("label", when(col("label_sting").equalTo("normal"),lit(0)).otherwise(lit(1)));
    val  testDataLabel=testData.withColumn("label", when(col("label_sting").equalTo("normal"),lit(0)).otherwise(lit(1)));
    data.unpersist()
    testData.unpersist()
    dataLabel.cache()
    testDataLabel.cache()
    dataLabel.show()
    //     dataLabel.select("label_int").groupBy("label_int").count().orderBy(desc("count")).show()

    val stringColumn=Array("protocol_type","service","flag")

    val stringIndexers=stringColumn.map{
      colName=>
        new StringIndexer()
          .setInputCol(colName)
          .setOutputCol(colName+"Indexed")
          .fit(dataLabel)
    }

    val inputCol=dataLabel.columns.filter(_!="label_sting").filter(_!="label")
      .filter(_!="protocol_type").filter(_!="service").filter(_!="flag")

    val vecassembler=new VectorAssembler()
      .setInputCols(inputCol)
      .setOutputCol("features")

    val rfClassifier = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")


    val steps = stringIndexers ++ Array( vecassembler, rfClassifier)

    val pipeline = new Pipeline().setStages(steps)

    val paramGrid = new ParamGridBuilder()
      .addGrid(rfClassifier.maxBins, Array(100, 200))
      .addGrid(rfClassifier.maxDepth, Array(2, 8, 10))
      .addGrid(rfClassifier.numTrees, Array(5, 20))
      .addGrid(rfClassifier.impurity, Array("entropy", "gini"))
      .build()

    val evaluator = new BinaryClassificationEvaluator()

    val crossvalidator = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid).setNumFolds(3)

    val pipelineModel = crossvalidator.fit(dataLabel)


    val featureImportances = pipelineModel
      .bestModel.asInstanceOf[PipelineModel]
      .stages(stringIndexers.size + 1)
      .asInstanceOf[RandomForestClassificationModel]
      .featureImportances

    vecassembler.getInputCols
      .zip(featureImportances.toArray)
      .sortBy(-_._2)
      .foreach {
        case (feat, imp) =>
          println(s"feature: $feat, importance: $imp")
      }

    val bestEstimatorParamMap = pipelineModel
      .getEstimatorParamMaps
      .zip(pipelineModel.avgMetrics)
      .maxBy(_._2)
      ._1
    println(s"Best params:\n$bestEstimatorParamMap")

    val predictions = pipelineModel.transform(testDataLabel)

    predictions.show()

    val areaUnderROC = evaluator.evaluate(predictions)

    println("areaUnderROC", areaUnderROC)


    val df1 = spark.readStream.format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "networkConnection")
      .option("group.id", "testgroup")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", false)
      .load()

    println(df1.schema)



    def parseConnection(line:String):Conection={
      val x=line.split(",")
      Conection(x(0).toInt,x(1),x(2),x(3),x(4).toInt,x(5).toInt,x(6).toInt,x(7).toInt,x(8).toInt,x(9).toInt,x(10).toInt,x(11).toInt,
        x(12).toInt,x(13).toInt,x(14).toInt,x(15).toInt,x(16).toInt,x(17).toInt,x(18).toInt,x(19).toInt,x(20).toInt,x(21).toInt,x(22).toInt,x(23).toInt,x(24).toDouble,
        x(25).toDouble,x(26).toDouble,x(27).toDouble,x(28).toDouble,x(29).toDouble,x(30).toDouble,x(31).toInt,x(32).toInt,x(33).toDouble,x(34).toDouble,x(35).toDouble,x(36).toDouble,
        x(37).toDouble,x(38).toDouble,x(39).toDouble,x(40).toDouble,x(41),x(42).toInt)

    }

    import spark.implicits._


    spark.udf.register("deserialize", (message:String)=>parseConnection(message))
    val df2 = df1.selectExpr("""deserialize(CAST(value as STRING)) AS message""").select($"message".as[Conection])


    df2.printSchema()
    val  df3=df2.withColumn("label", when(col("label_sting").equalTo("normal"),lit(0)).otherwise(lit(1)));
    df3.printSchema();

    val streamData=pipelineModel.transform(df3)
    val streamingQuery=streamData.writeStream.format("console").outputMode("append").start;
    streamingQuery.awaitTermination()
  }
}
