package org.hogzilla.dns

import java.util.HashMap
import java.util.Map
import scala.math.random
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.hogzilla.hbase.HogHBaseRDD
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors


object HogDNS {
  
  def run(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)])
  {
    kmeansBytes(HogRDD)
    
    kmeansAvgPktBytes(HogRDD)
 
  }
  
  def kmeansAvgPktBytes(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)])
  {
 
     
    val DnsRDD = HogRDD.
        map { case (id,result) => {
          val map: Map[String,String] = new HashMap[String,String]
              map.put("flow:id",Bytes.toString(id.get).toString())
              HogHBaseRDD.columns.foreach { column => map.put(column, 
                  Bytes.toString(result.getValue(Bytes.toBytes(column.split(":")(0).toString()),Bytes.toBytes(column.split(":")(1).toString())))) 
        }
        map
        }
    }

    val labelAndData = DnsRDD.filter(_.get("flow:lower_port").equals("53")).map { flow => 
    
    /*val vector = Vectors.dense(flow.get("flow:avg_packet_size").toDouble,flow.get("flow:bytes").toDouble)
    ((flow.get("flow:detected_protocol"),flow.get("flow:bytes")),vector)
   // (flow.get("flow:id"),vector)
    } */
    
     val vector = Vectors.dense(flow.get("flow:avg_packet_size").toDouble)
    ((flow.get("flow:detected_protocol"),flow.get("flow:avg_packet_size")),vector)
   // (flow.get("flow:id"),vector)
    }
    
    val data = labelAndData.values.cache()
    val kmeans = new KMeans()
    val model = kmeans.run(data)
    
    val clusterLabelCount = labelAndData.map({
      case (label,datum) =>
        val cluster = model.predict(datum)
        val map: Map[(Int,String),(Double,Int)] = new HashMap[(Int,String),(Double,Int)]
        map.put((cluster,label._1),  (label._2.toDouble,1))
        map
    }).reduce((a,b) => { 
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
      }
      a
    })
    
    //.countByValue
    
    println("######################################################################################")
    println("DNS K-Means Clustering by Average Packet Size")
    println("Centroids")
    model.clusterCenters.foreach { println }
    
    clusterLabelCount.keySet().toArray().foreach { case key:(Int,String) =>  
      val cluster = key._1
      val label = key._2
      val count =clusterLabelCount.get(key)._2.toString
      val avg = clusterLabelCount.get(key)._1.toString
      println(f"Cluster: $cluster%1s\t\tLabel: $label%20s\t\tCount: $count%10s\t\tAvg: $avg%10s")
      }

    /*clusterLabelCount.toSeq.sorted.foreach {
     
      case ((cluster,label),count) =>
        println(f"Cluster: $cluster%1sLabel: $label%18s Count: $count%8s")
    }*/
    
    //val max = hBaseRDD.map(tuple => tuple._2)
    //          .map(result => Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("packets"))).toLong)
    //          .reduce((a,b) => Math.max(a,b))
              
    //println("Maximo: "+teste)
  //  teste.toSeq.sortBy(_._2).foreach(println)
    //teste.foreach(println)
    println("######################################################################################")           


  }
  
  def kmeansBytes(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)])
  {
 
     
	  val DnsRDD = HogRDD.
			  map { case (id,result) => {
				  val map: Map[String,String] = new HashMap[String,String]
						  map.put("flow:id",Bytes.toString(id.get).toString())
						  HogHBaseRDD.columns.foreach { column => map.put(column, 
								  Bytes.toString(result.getValue(Bytes.toBytes(column.split(":")(0).toString()),Bytes.toBytes(column.split(":")(1).toString())))) 
			  }
			  map
			  }
	  }

   
	  val labelAndData = DnsRDD.filter(_.get("flow:lower_port").equals("53")).map { flow => 
	  
    /*val vector = Vectors.dense(flow.get("flow:avg_packet_size").toDouble,flow.get("flow:bytes").toDouble)
	  ((flow.get("flow:detected_protocol"),flow.get("flow:bytes")),vector)
	 // (flow.get("flow:id"),vector)
	  } */
    
     val vector = Vectors.dense(flow.get("flow:bytes").toDouble)
    ((flow.get("flow:detected_protocol"),flow.get("flow:bytes")),vector)
   // (flow.get("flow:id"),vector)
    }
    
    val data = labelAndData.values.cache()
    val kmeans = new KMeans()
    val model = kmeans.run(data)
    
    val clusterLabelCount = labelAndData.map({
      case (label,datum) =>
        val cluster = model.predict(datum)
        val map: Map[(Int,String),(Double,Int)] = new HashMap[(Int,String),(Double,Int)]
        map.put((cluster,label._1),  (label._2.toDouble,1))
        map
    }).reduce((a,b) => { 
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
      }
      a
    })
    
    //.countByValue
    
    println("######################################################################################")
    println("DNS K-Means Clustering by Flow Total Bytes")
    println("Centroids")
    model.clusterCenters.foreach { println }
    
    clusterLabelCount.keySet().toArray().foreach { case key:(Int,String) =>  
      val cluster = key._1
      val label = key._2
      val count =clusterLabelCount.get(key)._2.toString
      val avg = clusterLabelCount.get(key)._1.toString
      println(f"Cluster: $cluster%1s\t\tLabel: $label%20s\t\tCount: $count%10s\t\tAvg: $avg%10s")
      }

    /*clusterLabelCount.toSeq.sorted.foreach {
     
      case ((cluster,label),count) =>
        println(f"Cluster: $cluster%1sLabel: $label%18s Count: $count%8s")
    }*/
    
    //val max = hBaseRDD.map(tuple => tuple._2)
    //          .map(result => Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("packets"))).toLong)
    //          .reduce((a,b) => Math.max(a,b))
              
    //println("Maximo: "+teste)
  //  teste.toSeq.sortBy(_._2).foreach(println)
    //teste.foreach(println)
    println("######################################################################################")           


  }
  
}