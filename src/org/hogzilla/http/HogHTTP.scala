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


package org.hogzilla.http

import scala.math.random
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.hogzilla.hbase.HogHBaseRDD
import org.hogzilla.event.{HogEvent, HogSignature}
import java.util.HashSet
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.SVMWithSGD
import scala.tools.nsc.doc.base.comment.OrderedList
import org.apache.spark.mllib.optimization.L1Updater
import org.hogzilla.util.HogFlow
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map

/**
 * 
 */
object HogHTTP {

  val signature = (HogSignature(3,"HZ: Suspicious HTTP flow identified by K-Means clustering",2,1,826000101,826).saveHBase(),
                   HogSignature(3,"HZ: Suspicious HTTP flow identified by SuperBag",2,1,826000102,826).saveHBase())
                   
  val numberOfClusters=32
  val maxAnomalousClusterProportion=0.05
  val minDirtyProportion=0.001
  
  /**
   * 
   * 
   * 
   */
  def run(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)],spark:SparkContext)
  {
    
   // HTTP K-means clustering
   kmeans(HogRDD)
 
  }
  
  
  /**
   * 
   * 
   * 
   */
  def kmeansPopulate(event:HogEvent):HogEvent =
  {
    val centroids:String = event.data.get("centroids")
    val vector:String = event.data.get("vector")
    val clusterLabel:String = event.data.get("clusterLabel")
    val hostname:String = event.data.get("hostname")
    
    
    event.text = "This flow was detected by Hogzilla as an anormal activity. In what follows you can see more information.\n"+
                 "Hostname mentioned in HTTP flow: "+hostname+"\n"+
                 "Hogzilla module: HogHTTP, Method: k-means clustering with k="+numberOfClusters+"\n"+
                 "URL for more information: http://ids-hogzilla.org/signature-db/"+"%.0f".format(signature._1.signature_id)+"\n"+""
                 //"Centroids:\n"+centroids+"\n"+
                 //"Vector: "+vector+"\n"+
                 //"(cluster,label nDPI): "+clusterLabel+"\n"
  
    event.signature_id = signature._1.signature_id
                 
    event
  }
  
  
  /**
   * 
   * 
   * 
   */
  def kmeans(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)])
  {
    
	  val features = Array("flow:avg_packet_size",
			                   "flow:packets_without_payload",
                         "flow:avg_inter_time",
			                   "flow:flow_duration",
			                   "flow:max_packet_size",
			                   "flow:bytes",
			                   "flow:packets",
			                   "flow:min_packet_size",
                         "flow:packet_size-0",
                         "flow:inter_time-0",
                         "flow:packet_size-1",
                         "flow:inter_time-1",
                         "flow:packet_size-2",
                         "flow:inter_time-2",
                         "flow:packet_size-3",
                         "flow:inter_time-3",
                         "flow:packet_size-4",
                         "flow:inter_time-4",
		                     "flow:http_method")

    println("Filtering HogRDD...")
    val HttpRDD = HogRDD.
        map { case (id,result) => {
          val map: Map[String,String] = new HashMap[String,String]
              map.put("flow:id",Bytes.toString(id.get).toString())
              HogHBaseRDD.columns.foreach { column => 
                
                val ret = result.getValue(Bytes.toBytes(column.split(":")(0).toString()),Bytes.toBytes(column.split(":")(1).toString()))
                map.put(column, Bytes.toString(ret)) 
        }
          if(map.get("flow:packet_size-1")==null) map.put("flow:packet_size-1","0")
          if(map.get("flow:inter_time-1")==null) map.put("flow:inter_time-1","0")
          if(map.get("flow:packet_size-2")==null) map.put("flow:packet_size-2","0")
          if(map.get("flow:inter_time-2")==null) map.put("flow:inter_time-2","0")
          if(map.get("flow:packet_size-3")==null) map.put("flow:packet_size-3","0")
          if(map.get("flow:inter_time-3")==null) map.put("flow:inter_time-3","0")
          if(map.get("flow:packet_size-4")==null) map.put("flow:packet_size-4","0")
          if(map.get("flow:inter_time-4")==null) map.put("flow:inter_time-4","0")
          if(map.get("flow:http_method")==null) map.put("flow:http_method","0")
          
          val lower_ip = result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("lower_ip"))
          val upper_ip = result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("upper_ip"))
          new HogFlow(map,lower_ip,upper_ip)
        }
    }.filter(x => ( x.get("flow:lower_port").equals("80") || 
                    x.get("flow:upper_port").equals("80") || 
                    x.get("flow:lower_port").equals("81") || 
                    x.get("flow:upper_port").equals("81")
                  ) && x.get("flow:packets").toDouble.>(1)
                    && x.get("flow:id").split('.')(0).toLong.<(System.currentTimeMillis()-6000000)
             ).cache

  println("Counting HogRDD...")
  val RDDtotalSize= HttpRDD.count()
  println("Filtered HogRDD has "+RDDtotalSize+" rows!")
    
  println("Calculating some variables to normalize data...")
  val HttpRDDcount = HttpRDD.map(flow => features.map { feature => flow.get(feature).toDouble }).cache()
  val n = RDDtotalSize
  val numCols = HttpRDDcount.first.length
  val sums = HttpRDDcount.reduce((a,b) => a.zip(b).map(t => t._1 + t._2))
  val sumSquares = HttpRDDcount.fold(
      new Array[Double](numCols)
  )(
      (a,b) => a.zip(b).map(t => t._1 + t._2*t._2)
   )
      
  val stdevs = sumSquares.zip(sums).map{
      case(sumSq,sum) => math.sqrt(n*sumSq - sum*sum)/n
    }
    
  val means = sums.map(_/n)
      
  def normalize(vector: Vector):Vector = {
    val normArray = (vector.toArray,means,stdevs).zipped.map(
        (value,mean,std) =>
          if(std<=0) (value-mean) else (value-mean)/std)
    return Vectors.dense(normArray)
  }
    
  println("Normalizing data...")
    val labelAndData = HttpRDD.map { flow => 
      val vector = Vectors.dense(features.map { feature => flow.get(feature).toDouble })
       ( (flow.get("flow:detected_protocol"), 
           if (flow.get("event:priority_id")!=null && flow.get("event:priority_id").equals("1")) 1 else 0 , 
           flow.get("flow:host_server_name"),flow),
         normalize(vector)
       )
    }
  
    println("Estimating model...")
    val data = labelAndData.values.cache()
    val kmeans = new KMeans()
    kmeans.setK(numberOfClusters)
    val vectorCount = data.count()
    println("Number of vectors: "+vectorCount)
    val model = kmeans.run(data)
    
    println("Predicting points (ie, find cluster for each point)...")
     val clusterLabel = labelAndData.map({
      case (label,datum) =>
        val cluster = model.predict(datum)
        (cluster,label,datum)
    })
    
    println("Generating histogram...")
    val clusterLabelCount = clusterLabel.map({
      case (cluster,label,datum) =>
        val map: Map[(Int,String),(Double,Int)] = new HashMap[(Int,String),(Double,Int)]
        map.put((cluster,label._1),  (label._2.toDouble,1))
        map
    }).reduce((a,b) => { 
      
       b./:(0){
         case (c,((key:(Int,String)),(avg2,count2))) =>
           
                val avg = (a.get(key).get._1*a.get(key).get._2 + b.get(key).get._1*b.get(key).get._2)/
                          (a.get(key).get._2+b.get(key).get._2)
                          
                a.put(key, (avg,a.get(key).get._2+b.get(key).get._2))
           
               0
              }
      /*
      b.keySet().toArray()
      .map { 
        case key: (Int,String) =>  
            if (a.containsKey(key))
            {
              val avg = (a.get(key)._1*a.get(key)._2 + b.get(key)._1*b.get(key)._2)/
                          (a.get(key)._2+b.get(key)._2)
                          
              a.put(key, (avg,a.get(key)._2+b.get(key)._2))
            }else
              a.put(key,b.get(key))
      }*/
      a
    })
    
    println("######################################################################################")
    println("######################################################################################")
    println("######################################################################################")
    println("######################################################################################")
    println("HTTP K-Means Clustering")
    println("Centroids")
    val centroids = ""+model.clusterCenters.mkString(",\n")
    //model.clusterCenters.foreach { center => centroids.concat("\n"+center.toString) }
    
    clusterLabelCount./:(0) 
     { case (z,(key:(Int,String),(avg,count))) =>  
      val cluster = key._1
      val label = key._2
      //val count =clusterLabelCount.get(key).get._2.toString
      //val avg = clusterLabelCount.get(key).get._1.toString
      println(f"Cluster: $cluster%1s\t\tLabel: $label%20s\t\tCount: $count%10s\t\tAvg: $avg%10s")
      0
      }

      val thr=maxAnomalousClusterProportion*RDDtotalSize
      
      println("Selecting cluster to be tainted...")
      val taintedArray = clusterLabelCount.filter({ case (key:(Int,String),(avg,count)) => 
                                                          (count.toDouble < thr 
                                                              && avg.toDouble >= minDirtyProportion )
                          }).map(_._1)
                //.
                //  sortBy ({ case (cluster:Int,label:String) => clusterLabelCount.get((cluster,label))._1.toDouble }).reverse
      
      taintedArray.par.map 
      {
        tainted =>
        
        //val tainted = taintedArray.apply(0)
                  
        println("######################################################################################")
        println("Tainted flows of: "+tainted.toString())
      
        println("Generating events into HBase...")
        clusterLabel.filter({ case (cluster,(group,tagged,hostname,flow),datum) => (cluster,group).equals(tainted) && tagged.equals(0) }).
        foreach{ case (cluster,(group,tagged,hostname,flow),datum) => 
          val event = new HogEvent(flow)
          event.data.put("centroids", centroids)
          event.data.put("vector", datum.toString)
          event.data.put("clusterLabel", "("+cluster.toString()+","+group+")")
          event.data.put("hostname", flow.get("flow:host_server_name")+"/"+flow.get("flow:http_url"))
          kmeansPopulate(event).alert()
        }

   /*   
     (1 to 9).map{ k => 
        println("######################################################################################")
        println(f"Hosts from cluster $k%1s")
        clusterLabel.filter(_._1.equals(k)).foreach{ case (cluster,label,datum) => 
          print(label._3+"|")      
        }
        println("")
     }
*/
      println("######################################################################################")
      println("######################################################################################")
      println("######################################################################################")
      println("######################################################################################")           

   }
      
   if(taintedArray.isEmpty)
   {
        println("No flow matched!")
   }

  }
  
  
  
}