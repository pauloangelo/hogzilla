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
/** 
 *  REFERENCES:
 *   - http://ids-hogzilla.org/xxx/826000101
 */


package org.hogzilla.sflow

//import java.util.ArrayList
import java.util.HashMap
import java.util.Map
import scala.math.random
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD
import org.hogzilla.event.HogEvent
import org.hogzilla.event.HogSignature
import org.hogzilla.hbase.HogHBaseRDD
import org.hogzilla.util.HogFlow
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.filter.BinaryComparator
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.filter.Filter
import org.hogzilla.hbase.HogHBaseReputation
import scala.collection.mutable.ArraySeq
import scala.collection.mutable.HashSet

/**
 * 
 */
object HogSFlow {

  val signature = (HogSignature(3,"HZ: Top talker identified",2,1,826001001,826).saveHBase())
      
  
  /**
   * 
   * 
   * 
   */
  def run(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)],spark:SparkContext)
  {
    
   // TopTalkers, SMTP Talkers, XXX
   top(HogRDD)
 
  }
  
  
  /**
   * 
   * 
   * 
   */
  def populate(event:HogEvent):HogEvent =
  {
  
    val hostname:String = event.data.get("hostname")
    
    
    event.text = "This flow was detected by Hogzilla as an anormal activity. In what follows you can see more information.\n"+
                 "Hostname mentioned in HTTP flow: "+hostname+"\n"
                 //"Centroids:\n"+centroids+"\n"+
                 //"Vector: "+vector+"\n"+
                 //"(cluster,label nDPI): "+clusterLabel+"\n"
  
    event.signature_id = signature.signature_id
                 
    event
  }
  
  
  /**
   * 
   * 
   * 
   */
  def top(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)])
  {


    println("Filtering SflowRDD...")
    val SflowRDD = HogRDD.
        map { case (id,result) => {
          val map: Map[String,String] = new HashMap[String,String]
              map.put("flow:id",Bytes.toString(id.get).toString())
              HogHBaseRDD.columnsSFlow.foreach { column => 
                
                val ret = result.getValue(Bytes.toBytes(column.split(":")(0).toString()),Bytes.toBytes(column.split(":")(1).toString()))
                map.put(column, Bytes.toString(ret)) 
        }
                   
        val srcIP = result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("srcIP"))
        val dstIP = result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("dstIP"))
         // if(Bytes.toString(srcIP) < Bytes.toString(dstIP) )
         new HogFlow(map,srcIP,dstIP)
        //  else
          //  new HogFlow(map,dstIP,srcIP)
        }
    }.cache

  println("Counting SflowRDD...")
  val RDDtotalSize= SflowRDD.count()
  println("Filtered SflowRDD has "+RDDtotalSize+" rows!")

   val myNets =  new HashSet[String]
    val it = HogHBaseRDD.hogzilla_mynets.getScanner(new Scan()).iterator()
    while(it.hasNext())
    {
      myNets.add(Bytes.toString(it.next().getValue(Bytes.toBytes("net"),Bytes.toBytes("prefix"))))
    }
    
  
  val whiteTopTalkers = HogHBaseReputation.getReputationList("TTalker","whitelist")
  
  
  println("")
  println("Top Talkers (bytes):")
  println("(LowerIP <-> UpperIP, Bytes)")
  val g1: PairRDDFunctions[String, Long] = 
    SflowRDD.map( hogflow => (Bytes.toString(hogflow.lower_ip),hogflow.map.get("flow:packetSize").toLong) )
  
  g1.reduceByKey(_+_).sortBy(_._2, false, 10)
    .filter(tp => {  myNets.map { net => if( tp._1.startsWith(net) )
                                            { true } else{false} 
                                    }.contains(true)
          })
    .take(30+whiteTopTalkers.size)
    .filter(tp => !whiteTopTalkers.contains(tp._1) )
    .take(30)
    .foreach{ tp => println("("+tp._1+","+tp._2+")" ) }

    /*
  println("")
  println("Top Talkers (flows):")
  println("(LowerIP <-> UpperIP, Flows)")
  val g12: PairRDDFunctions[String, Long] = 
    SflowRDD.map( hogflow => (Bytes.toString(hogflow.lower_ip)+"<->"+Bytes.toString(hogflow.upper_ip),1L) )
    
  g12.reduceByKey(_+_).sortBy(_._2, false, 10)
    .take(30+whiteTopTalkers.size)
    .filter(tp => !whiteTopTalkers.contains(tp._1))
    .take(30)
    .foreach{ tp => println("("+tp._1+","+tp._2+")" ) }
  
  
  val whiteSMTPTalkers =  HogHBaseReputation.getReputationList("MX","whitelist")

  println("")
  println("SMTP Talkers:")
  println("(SRC IP, DST IP, Bytes, Qtd Flows)")
  val g2: PairRDDFunctions[(String,String), (Long,Long)] 
           = SflowRDD.filter { flow => flow.map.get("flow:dstPort").equals("25") || flow.map.get("flow:srcPort").equals("25") }
                     .map { hogflow => 
                               ((Bytes.toString(hogflow.lower_ip),Bytes.toString(hogflow.upper_ip)),
                                (hogflow.map.get("flow:packetSize").toLong,1L)) 
                           }
  g2.reduceByKey{case ((a1,b1),(a2,b2)) => (a1+a2,b1+b2)}
    .sortBy({case ((src,dst),(bytes,qtFlows)) => bytes} , false, 10)
    .take(30+whiteSMTPTalkers.size)
    .filter{case ((a1,b1),(a2,b2)) => ! (whiteSMTPTalkers.contains(a1) || whiteSMTPTalkers.contains(b1)) }
    .take(30)
    .foreach{case ((src,dst),(bytes,qtFlows)) => println("("+src+","+dst+","+bytes+","+qtFlows+")")}
  
 */

  
  

  }
  
  
  
}