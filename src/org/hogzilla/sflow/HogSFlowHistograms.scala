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
/** 
 *  REFERENCES:
 *   - http://ids-hogzilla.org/xxx/826000101
 */


package org.hogzilla.sflow

import java.net.InetAddress
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.Map
import scala.math.floor
import scala.math.log
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.hogzilla.event.HogEvent
import org.hogzilla.event.HogSignature
import org.hogzilla.hbase.HogHBaseHistogram
import org.hogzilla.hbase.HogHBaseRDD
import org.hogzilla.hbase.HogHBaseReputation
import org.hogzilla.histogram.Histograms
import org.hogzilla.histogram.HogHistogram
import org.hogzilla.util.HogFlow
import org.apache.commons.math3.analysis.function.Min
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.clustering.KMeans
import org.hogzilla.hbase.HogHBaseCluster


/**
 * 
 */
object HogSFlowHistograms {

     
  val signature = HogSignature(3,"HZ: Top talker identified" ,                2,1,826001101,826).saveHBase() //1
                 
  
 
  /**
   * 
   * 
   * 
   */
  def run(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)],spark:SparkContext)
  {
    
   // TopTalkers, SMTP Talkers, XXX: Organize it!
   realRun(HogRDD)
 
  }
  
  
  def populateTopTalker(event:HogEvent):HogEvent =
  {
    val hostname:String = event.data.get("hostname")
    val bytesUp:String = event.data.get("bytesUp")
    val bytesDown:String = event.data.get("bytesDown")
    val threshold:String = event.data.get("threshold")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Large amount of sent data (>"+threshold+")\n"+
                  "IP: "+hostname+"\n"+
                  "Bytes Up: "+bytesUp+"\n"+
                  "Bytes Down: "+bytesDown+"\n"+
                  "Packets: "+numberPkts+"\n"+
                  "Flows"+stringFlows
                    
    event.signature_id = signature.signature_id       
    event
  }
  
 
  def isMyIP(ip:String,myNets:Set[String]):Boolean =
  {
    myNets.map ({ net =>  if( ip.startsWith(net) )
                              { true } 
                          else{false} 
                }).contains(true)
  }
  
  
  
  
  /**
   * 
   * 
   * 
   */
  def realRun(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)])
  {
    
   val myNetsTemp =  new HashSet[String]
      
   val it = HogHBaseRDD.hogzilla_mynets.getScanner(new Scan()).iterator()
   while(it.hasNext())
   {
      myNetsTemp.add(Bytes.toString(it.next().getValue(Bytes.toBytes("net"),Bytes.toBytes("prefix"))))
   }
    
   val myNets:scala.collection.immutable.Set[String] = myNetsTemp.toSet
   
   
  val summary1: RDD[(String,Long,Set[Long],HashMap[String,Double])] 
                      = HogRDD
                        .map ({  case (id,result) => 
                                    
                                      val histogramSize    = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("size"))).toLong
                                      val histogramName    = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
                                      val histMap              = HogHBaseHistogram.mapByResult(result)
                                      
                                      val keys:Set[Long] = histMap.filter({ case (key,value) => key.toDouble < 10000 & value>0.001})
                                                           .keySet
                                                           .map({ case key => key.toDouble.toLong })
                                                           .toSet
                                      //"HIST01-"+myIP
                                      
                                      (histogramName,histogramSize,keys,histMap)
                           })
                           .filter({case (histogramName,histogramSize,keys,histMap) =>
                                         histogramName.startsWith("HIST01") &
                                         histogramSize>20 &
                                         isMyIP(histogramName.subSequence(histogramName.lastIndexOf("-")+1, histogramName.length()).toString,myNets)
                                   })
                           .cache
                           
  val allKeys = summary1
                .map(_._3)
                .reduce(_++_)
                .toList
                .sorted
                
  val vectorSize = allKeys.size
  
  val summary: RDD[(String,Long,Set[Long],Vector)]
              = summary1
                .map({ case (histogramName,histogramSize,keys,histMap) =>
                      val vector = 
                          Vectors.dense({ allKeys.map({ key =>
                            
                                                         if(keys.contains(key))
                                                           histMap.get(key.toString).get*100D
                                                         else
                                                           0D
                                                      }).toArray
                                       })
                      
                      (histogramName,histogramSize,keys,vector)
                }).cache
  
   println("Keys: "+allKeys.mkString(","))
  
   //(5 to 30 by 5).toList.par
   
  val k=10
  
        println("Estimating model, k="+k)
        val kmeans = new KMeans()
        kmeans.setK(k)
        val model = kmeans.run(summary.map(_._4))
        
        println("Centroids("+k+"): \n"+model.clusterCenters.mkString(",\n"))

        val kmeansResult=summary.map({
          case (histogramName,histogramSize,keys,vector) =>
            val cluster = model.predict(vector)
            val centroid = model.clusterCenters(cluster)
            
            val distance=math.sqrt(vector.toArray.zip(centroid.toArray).map({case (p1,p2) => p1-p2}).map(p => p*p).sum)
                       
            (cluster,(distance,histogramName,histogramSize,keys,vector))
        }).cache
        
        val mean    = kmeansResult.map(_._2._1).mean
        val stdDev  = kmeansResult.map(_._2._1).stdev
        val max     = kmeansResult.map(_._2._1).max
        val elementsPerCluster = kmeansResult.countByKey().toList.sortBy(_._1).toMap
   
        println("(Mean,StdDev,Max)("+k+"): "+mean+","+stdDev+","+max+".")
        println("Elements per cluster:\n"+elementsPerCluster.mkString(",\n"))
        
        // Delete saved clusters
        (0 to k by 1).toList.foreach { HogHBaseCluster.deleteCluster(_) }
        
   
            
       val members =
       kmeansResult
       .map({case (cluster,(distance,histogramName,histogramSize,keys,vector)) =>
              (cluster,histogramName.subSequence(histogramName.lastIndexOf("-")+1, histogramName.length()).toString)
        }).cache().toArray()
   
   
        val grouped = kmeansResult.groupByKey()
        grouped
        .foreach({ case ((clusterIdx,iterator)) =>
                    
                    val centroid     = model.clusterCenters(clusterIdx)
                    val centroidMain = allKeys.zip(centroid.toArray)//.filter(_._2>10)
                    val clusterSize  = elementsPerCluster.get(clusterIdx).get
                    
                    if(centroidMain.filter(_._2>10).size>0 & clusterSize > 4)
                    {
                      println("################################################################\n"+
                              "CLUSTER: "+clusterIdx+"\n"+
                              "Centroid:\n"+centroidMain.filter(_._2>10).mkString(",")+"\n"+
                              "clusterSize: "+clusterSize+"\n")
                              
                      HogHBaseCluster.saveCluster(clusterIdx,centroidMain,clusterSize,members.filter(_._1.equals(clusterIdx)).map({_._2}))
                    }
                 })
        
        
        /*
        grouped
        .foreach({ case ((clusterIdx,iterator)) =>
                    
                  val centroid     = model.clusterCenters(clusterIdx)
                  val centroidMain = allKeys.zip(centroid.toArray).filter(_._2>20)
                  val clusterSize = elementsPerCluster.get(clusterIdx).get
                     
                  if(clusterSize>10 & centroidMain.size>0)
                  {
                     val group=iterator
                        .map({ case  (distance,histogramName,histogramSize,keys,vector) =>
                                     val hogAccessHistogram = HogHBaseHistogram
                                                                .getHistogram("HIST02"
                                                                      +histogramName
                                                                       .subSequence(histogramName.lastIndexOf("-"), histogramName.length()))
                               (distance,histogramName,histogramSize,keys,vector,hogAccessHistogram)                                     
                             })
                    
                     
                      val groupHistogram = 
                           group
                           .map({case (distance,histogramName,histogramSize,keys,vector,hogAccessHistogram) => hogAccessHistogram})
                           .reduce({(hogAccessHistogram1,hogAccessHistogram2) =>
                                        Histograms.merge(hogAccessHistogram1,hogAccessHistogram2)
                                  })
                         
                      group
                      .filter({ case (distance,histogramName,histogramSize,keys,vector,hogAccessHistogram) =>
                                   hogAccessHistogram.histSize>20
                              })
                      .map({ case (distance,histogramName,histogramSize,keys,vector,hogAccessHistogram) =>
                            
                            val groupHistogramMinus = Histograms.difference(groupHistogram,hogAccessHistogram)
                            
                            val atypical = Histograms.atypical(groupHistogramMinus.histMap, hogAccessHistogram.histMap)
                            
                            if(atypical.size>0)
                            {
                              println("################################################################\n"+
                                      "CLUSTER: "+clusterIdx+"\n"+
                                      "Centroid:\n"+centroidMain.mkString(",\n")+"\n"+
                                      "HistSize mean: "+(groupHistogram.histSize/clusterSize)+"\n"+
                                      "HistSize:"+hogAccessHistogram.histSize+"\n"+
                                      "Atypicals: "+atypical.mkString(",")+"\n"+
                                      "Histogram: "+hogAccessHistogram.histName+"\n"+
                                      hogAccessHistogram.histMap.mkString(",\n")+"\n"+
                                      "GroupHistogram:\n"+groupHistogram.histMap.mkString(",\n")+"\n")
                            }
                          })
                      
                      } 
                 })
       */          
    
   
   
  }


}