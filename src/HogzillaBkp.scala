

/**
 * @author pa
 */

import scala.math.random
import java.lang.{Math}
import org.apache.spark._
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.mllib.regression.{LabeledPoint,LinearRegressionModel,LinearRegressionWithSGD}
import org.apache.spark.mllib.linalg.Vectors


object HogzillaBkp {
  
  def main(args: Array[String])
  {
    val sparkConf = new SparkConf().setAppName("Hogzilla")
    val spark = new SparkContext(sparkConf)
    val tabela = "blogposts"
 
    val conf = HBaseConfiguration.create()
    
    conf.set(TableInputFormat.INPUT_TABLE, tabela)
    conf.set(TableInputFormat.SCAN_COLUMNS, "post:number")

   val admin = new HBaseAdmin(conf)
   if (!admin.isTableAvailable(tabela)) {
      //val tableDesc = new HTableDescriptor(TableName.valueOf(tabela))
     // admin.createTable(tableDesc)
     println("Table does not exist.")
    }
     val hBaseRDD = spark.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val max = hBaseRDD.map(tuple => tuple._2)
              .map(result => Bytes.toString(result.getValue(Bytes.toBytes("post"),Bytes.toBytes("number"))).toLong)
              .reduce((a,b) => Math.max(a,b))
    val splits= hBaseRDD.map(tuple => tuple._2).map(result => Bytes.toString(result.getValue(Bytes.toBytes("post"),Bytes.toBytes("number"))).toDouble).sortBy(p => p, true, 1).map(p => LabeledPoint(p,Vectors.dense(p))).randomSplit(Array(0.001, 0.999), seed = 11L)  
      
    val parsedData = splits(0).cache()
    parsedData.collect().foreach(println)       
 
    //hBaseRDD.map(tuple => tuple._2)
   //           .map(result => Bytes.toString(result.getValue(Bytes.toBytes("post"),Bytes.toBytes("number"))).toDouble)
   //           .collect().foreach(println)
    
    // Building the model
    val numIterations = 10
    val model = new LinearRegressionWithSGD()
    model.setIntercept(true).optimizer.setNumIterations(numIterations).setStepSize(.1)
    val modelResult = model.run(parsedData)
    
    // hBaseRDD.map(tuple => tuple._2).reduce((a,b) => Math.max(a,b))
   
    println("##########################################")
    println("##########################################")
    println("##########################################")
    println("Maximo: "+max)
    println("Model: "+model.toString())
    println("\t Pesos: "+modelResult.weights.toString())
    println("##########################################")
    println("##########################################")
    println("##########################################")
    
    spark.stop()
    admin.close()

  }
    /*
  val  = sc.textFile("spark-1.4.1-bin-hadoop2.6/data/mllib/ridge-data/lpsa.data")
val parsedData1 = data1.map { line =>
  val parts = line.split(',')
  LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
}.cache()
    
     
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    */
}