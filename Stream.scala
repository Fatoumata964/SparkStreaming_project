package cancerpred.streaming

package stream
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{ Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object Stream2 {
  def main(args: Array[String]) {
    val modeldir="/home/ubuntu/modele/lr"
	
	// define sparkSession
    val spark: SparkSession = SparkSession.builder().appName("cancerpred")
      .master("local[*]").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.streaming.kafka.consumer.poll.ms", "50000000")

    import spark.implicits._
	//define a schema for our data
    val schema = StructType(Array(
      StructField("thickness", DoubleType, true),
      StructField("size", DoubleType, true),
      StructField("shape", DoubleType, true),
      StructField("madh", DoubleType, true),
      StructField("epsize", DoubleType, true),
      StructField("bnuc", DoubleType, true),
      StructField("bchrom", DoubleType, true),
      StructField("nNuc", DoubleType, true),
      StructField("mit", DoubleType, true),
      StructField("ville", StringType, true),
      StructField("nom", StringType, true),
      StructField("prenom", StringType, true),
      StructField("dateNaiss", StringType, true),
      StructField("lat", StringType, true),
      StructField("lon", StringType, true)

    ))
    val df = spark.createDataFrame(spark.sparkContext
      .emptyRDD[Row], schema)
	  
	//merge all columns used in our model into a vector column
    val assembler = new VectorAssembler().
      setInputCols(df.select("thickness", "size", "shape", "madh", "epsize", "bnuc", "bchrom", "nNuc", "mit").columns)
      .setOutputCol("features")
	  
    //readStream to read data from Kafka and load it
    val dfstreaming =spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "00.00.000.00:9092")
      .option("subscribe", Seq("listening3","listening2","listening1" )mkString(", "))
      .option("group.id", "spark_faan_cancerology_regressionlogistic1")
      .option("startingOffsets", "latest")
      .load()
	  
	//load the model
    val modelload=org.apache.spark.ml.classification.LogisticRegressionModel.load(modeldir)
	
	//apply the model to data collected from Kafka
    val df2s = dfstreaming.select($"value" cast "string" as "json").select(from_json($"json", schema) as "data").select("data.*")
    val df3s=modelload.transform(assembler.transform(df2s))
      .withColumn("agent_timestamp", date_format(current_timestamp(),"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
      .withColumn("location", concat($"lat", lit(','), $"lon"))
      .drop("features")
      .select(to_json(struct("*")).as("value"))
	  
	//write prediction results to Kafka
    df3s.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "00.00.000.00:9092")
      .option("topic", "writting")
      .option("checkpointLocation", "/tmp/checkpoint001")
      .start()
    spark.streams.awaitAnyTermination()
  }
}
