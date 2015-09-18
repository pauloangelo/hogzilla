package org.hogzilla.dns

import java.util.HashMap
import java.util.Map
import scala.math.random
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.hogzilla.hbase.HogHBaseRDD
import org.hogzilla.event.HogEvent

/**
 * 
References: http://www.zytrax.com/books/dns/ch15/
 */
object HogDNS {
  
  def run(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)])
  {
    //kmeansBytes(HogRDD)
    
    kmeans(HogRDD)
 
  }
  
  def kmeans(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)])
  {
 
     
    val DnsRDD = HogRDD.
        map { case (id,result) => {
          val map: Map[String,String] = new HashMap[String,String]
              map.put("flow:id",Bytes.toString(id.get).toString())
              HogHBaseRDD.columns.foreach { column => 
                
                val ret = result.getValue(Bytes.toBytes(column.split(":")(0).toString()),Bytes.toBytes(column.split(":")(1).toString()))
                map.put(column, Bytes.toString(ret)) 
        }
          if(map.get("flow:dns_num_queries")==null) map.put("flow:dns_num_queries","0")
          if(map.get("flow:dns_num_answers")==null) map.put("flow:dns_num_answers","0")
          if(map.get("flow:dns_ret_code")==null) map.put("flow:dns_ret_code","0")
          if(map.get("flow:dns_bad_packet")==null) map.put("flow:dns_bad_packet","0")
          if(map.get("flow:dns_query_type")==null) map.put("flow:dns_query_type","0")
          if(map.get("flow:dns_rsp_type")==null) map.put("flow:dns_rsp_type","0")
       //   if(map.get("flow:packet_size-0")==null) map.put("flow:packet_size-0","0")
         // if(map.get("flow:inter_time-0")==null) map.put("flow:inter_time-0","0")
        map
        }
    }.filter(x => x.get("flow:lower_port").equals("53") && x.get("flow:packets").toDouble.>(1)).cache
      
 val DnsRDDcount = DnsRDD.map(flow => Array( flow.get("flow:avg_packet_size").toDouble,
                                flow.get("flow:packets_without_payload").toDouble,
                                flow.get("flow:avg_inter_time").toDouble,
                                flow.get("flow:flow_duration").toDouble,
                                flow.get("flow:max_packet_size").toDouble,
                                flow.get("flow:flow_duration").toDouble,
                                flow.get("flow:min_packet_size").toDouble,
                                flow.get("flow:packet_size-0").toDouble,
                                flow.get("flow:inter_time-0").toDouble,
                                flow.get("flow:dns_num_queries").toDouble,
                                flow.get("flow:dns_num_answers").toDouble,
                                flow.get("flow:dns_ret_code").toDouble,
                                flow.get("flow:dns_bad_packet").toDouble,
                                flow.get("flow:dns_query_type").toDouble,
                                flow.get("flow:dns_rsp_type").toDouble)).cache
  
  val numCols = DnsRDDcount.first.length
  val n = DnsRDDcount.count()
  val sums = DnsRDDcount.reduce((a,b) => a.zip(b).map(t => t._1 + t._2))
  val sumSquares = DnsRDDcount.fold(
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
    

    val labelAndData = DnsRDD.map { flow => 
     val vector = Vectors.dense(flow.get("flow:avg_packet_size").toDouble,
                                flow.get("flow:packets_without_payload").toDouble,
                                flow.get("flow:avg_inter_time").toDouble,
                                flow.get("flow:flow_duration").toDouble,
                                flow.get("flow:max_packet_size").toDouble,
                                flow.get("flow:flow_duration").toDouble,
                                flow.get("flow:min_packet_size").toDouble,
                                flow.get("flow:packet_size-0").toDouble,
                                flow.get("flow:inter_time-0").toDouble,
                                flow.get("flow:dns_num_queries").toDouble,
                                flow.get("flow:dns_num_answers").toDouble,
                                flow.get("flow:dns_ret_code").toDouble,
                                flow.get("flow:dns_bad_packet").toDouble,
                                flow.get("flow:dns_query_type").toDouble,
                                flow.get("flow:dns_rsp_type").toDouble)
    ((flow.get("flow:detected_protocol"), if (flow.get("event:priority_id")!=null && flow.get("event:priority_id").equals("1")) 1 else 0 , flow.get("flow:host_server_name"),flow),normalize(vector))
   // (flow.get("flow:id"),vector)
    }
    
    val data = labelAndData.values.cache()
    val kmeans = new KMeans()
    kmeans.setK(10)
    val model = kmeans.run(data)
    
     val clusterLabel = labelAndData.map({
      case (label,datum) =>
        val cluster = model.predict(datum)
        (cluster,label,datum)
    })
    
      /*
    val clusterLabelCount = labelAndData.map({
      case (label,datum) =>
        val cluster = model.predict(datum)
        val map: Map[(Int,String),(Double,Int)] = new HashMap[(Int,String),(Double,Int)]
        map.put((cluster,label._1),  (label._2.toDouble,1))
        map
    })*/
    
    val clusterLabelCount = clusterLabel.map({
      case (cluster,label,datum) =>
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
    println("######################################################################################")
    println("######################################################################################")
    println("######################################################################################")
    println("DNS K-Means Clustering")
    println("Centroids")
    model.clusterCenters.foreach { println }
    
    clusterLabelCount.keySet().toArray().foreach { case key:(Int,String) =>  
      val cluster = key._1
      val label = key._2
      val count =clusterLabelCount.get(key)._2.toString
      val avg = clusterLabelCount.get(key)._1.toString
      println(f"Cluster: $cluster%1s\t\tLabel: $label%20s\t\tCount: $count%10s\t\tAvg: $avg%10s")
      }


      val dirty = clusterLabelCount.keySet().toArray().filter({ case (cluster:Int,label:String) => cluster.>(0) }).
                  sortBy ({ case (cluster:Int,label:String) => clusterLabelCount.get((cluster,label))._1.toDouble }).reverse.apply(0)
      
                  
      println("######################################################################################")
      println("Dirty flows of: "+dirty.toString())
      
      clusterLabel.filter({ case (cluster,(group,taited,hostname,flow),datum) => (cluster,group).equals(dirty) }).
      foreach{ case (cluster,(group,taited,hostname,flow),datum) => 
        val event = new HogEvent(flow)
        event.alert()
      }

      
   (1 to 9).map{ k => 
      println("######################################################################################")
      println(f"Hosts from cluster $k%1s")
     clusterLabel.filter(_._1.equals(k)).foreach{ case (cluster,label,datum) => 
        print(label._3+"|")      
      }
      println("")
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
    println("######################################################################################")
    println("######################################################################################")
    println("######################################################################################")           


  }
  
  def x(event:HogEvent):HogEvent =
  {
    event.description = ""
    event
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