/*
* Copyright (C) 2015-2016 Paulo Angelo Alves Resende <pa@pauloangelo.com>
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

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.hogzilla.hbase.HogHBaseRDD
import org.hogzilla.initiate.HogInitiate
import org.hogzilla.prepare.HogPrepare
import org.hogzilla.sflow._
import org.hogzilla.http.HogHTTP
import org.hogzilla.auth.HogAuth
import org.hogzilla.dns.HogDNS
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel

/**
 * 
 * Keep it useful, simple, robust, and scalable.
 * 
 * NOT RUNNING! DEPENDS ON IMPLEMENTATION ON AUTH2HZ!
 * 
 */
object HogzillaStream {
  
  def main(args: Array[String])
  {
    val sparkConf = new SparkConf()
                          .setAppName("HogzillaStream")
                          .setMaster("local[2]")
                          .set("spark.executor.memory", "512m")
                          .set("spark.default.parallelism", "16") // 160
      
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val spark = new SparkContext(sparkConf)
    
    // Get the HBase RDD
    val HogRDD = HogHBaseRDD.connect(spark);

    val lines = ssc.socketTextStream("localhost", 9999,StorageLevel.MEMORY_AND_DISK_SER)
    
    val HogRDDAuth = HogHBaseRDD.connectAuth(spark);
    HogAuth.run(HogRDDAuth,spark);
    
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    
    
    ssc.start()
    ssc.awaitTermination()
     
    
    // Stop Spark
    spark.stop()
    
    // Close the HBase Connection
    HogHBaseRDD.close();

  }
  
}