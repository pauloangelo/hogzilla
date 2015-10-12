/*
* Copyright (C) 2015-2015 Paulo Angelo Alves Resende <pa@pauloangelo.com>
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License Version 2 as
* published by the Free Software Foundation.  You may not use, modify or
* distribute this program under any other version of the GNU General
* Public License.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this program; if not, write to the Free Software
* Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
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
import org.hogzilla.http.HogHTTP

/**
 * 
 * Keep it useful, simple, robust, and scalable.
 * 
 * 
 */
object Hogzilla {
  
  def main(args: Array[String])
  {
    val sparkConf = new SparkConf()
                          .setAppName("Hogzilla")
                          .set("spark.executor.memory", "1g")
                          .set("spark.default.parallelism", "160") // 160
                          
    val spark = new SparkContext(sparkConf)
    
    // Get the HBase RDD
    val HogRDD = HogHBaseRDD.connect(spark);
    
    // Initiate HogZilla
    HogInitiate.initiate(spark);
    
    // Prepare the data
    HogPrepare.prepare(HogRDD)
    
    // Run algorithms for DNS protocol
    HogDNS.run(HogRDD,spark);
    
    // Run algorithms for HTTP protocol
    HogHTTP.run(HogRDD,spark);
    
    // Run algorithms for SMTP protocol
    //HogSMTP.run(HogRDD);
    
    // Stop Spark
    spark.stop()
    
    // Close the HBase Connection
    HogHBaseRDD.close();

  }
  
}