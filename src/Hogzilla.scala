/**
 * 
 * 
 * @author pa
 * 
 * 
 */

import scala.math.random
import java.lang.Math
import org.apache.spark._
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.mllib.regression.{LabeledPoint,LinearRegressionModel,LinearRegressionWithSGD}
import org.apache.spark.mllib.linalg.Vectors
import org.hogzilla.hbase._
import org.hogzilla.dns.HogDNS
import org.hogzilla.prepare._
import org.hogzilla.initiate.HogInitiate

/**
 * 
 * Keep it simple, robust, scalable and useful.
 * 
 * 
 */
object Hogzilla {
  
  def main(args: Array[String])
  {
    val sparkConf = new SparkConf().setAppName("Hogzilla")
    val spark = new SparkContext(sparkConf)
    
    // Get the HBase RDD
    val HogRDD = HogHBaseRDD.connect(spark);
    
    // Initiate HogZilla
    HogInitiate.initiate(spark);
    
    // Prepare the data
    //HogPrepare.prepare(HogRDD)
    
    // Run algorithms for DNS protocol
    HogDNS.run(HogRDD,spark);
    
    // Run algorithms for HTTP protocol
    //HogHTTP.run(HogRDD);
    
    // Run algorithms for SMTP protocol
    //HogSMTP.run(HogRDD);
    
    // Stop Spark
    spark.stop()
    
    // Close the HBase Connection
    HogHBaseRDD.close();

  }
  
}