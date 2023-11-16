package cancerpred.streaming

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{ SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.Column
object modelLR{
  def func(column: Column) = column.cast(DoubleType)

  def main(args: Array[String]) {
    val datadir="src/main/scala/mosef/streaming/cancer.csv"
    val modeldir="/home/ubuntu/modele/lr"
	//define sparkSession
    val spark: SparkSession = SparkSession.builder().appName("cancerpred")
      .master("local[*]").getOrCreate()
    import spark.implicits._
	//read data 
    val df=spark.read.option("header", true)
      .option("InferSchema", true).csv(datadir)
      .filter($"bnuc"=!="?")
      .withColumn("bnuc", $"bnuc".cast(DoubleType))
      .withColumn("class", when($"class"===2, lit(0)).otherwise(lit(1)))
      .drop("id")
    val obsDF=df.select(df.columns.map(c=>func(col(c))): _*)

    //define the feature columns to put in the feature vector
    val featureCols = Array("thickness", "size", "shape", "madh", "epsize", "bnuc", "bchrom", "nNuc", "mit")
    //set the input and output column names
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    //return a dataframe with all of the  feature columns in  a vector column
    val df2 = assembler.transform(obsDF)
    //  Create a label column with the StringIndexer
    val labelIndexer = new StringIndexer().setInputCol("class").setOutputCol("label")
    val df3 = labelIndexer.fit(df2).transform(df2)
    //  split the dataframe into training and test data
    val splitSeed = 5043
    val Array(trainingData, testData) = df3.randomSplit(Array(0.7, 0.3), splitSeed)
    // create the classifier,  set parameters for training
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    //  use logistic regression to train (fit) the model with the training data
    val model = lr.fit(trainingData)
    model.write.overwrite().save(modeldir)
  }
}
