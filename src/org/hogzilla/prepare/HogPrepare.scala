package org.hogzilla.prepare

import java.util.HashMap
import java.util.Map
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.hogzilla.hbase.HogHBaseRDD
import org.apache.hadoop.hbase.client.RowMutations
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Scan


object HogPrepare {
  
  def prepare(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)])
  {
    
    // Delete old data from HBase 86400 is one day. You should need even more, depends on your available resources.
    
    println("Cleaning HBase...")
    val timeSuperior = System.currentTimeMillis - 86400000
    val nSplits = 10
    val denseTime = 86400000*2
    val deltaT = denseTime/nSplits
 
    // Parallel
    (0 to nSplits).toList.par.map{ k => 
       
      val scan = new Scan
      
      if(k.equals(0))
        scan.setTimeRange(0, timeSuperior-denseTime)
      else
        scan.setTimeRange(timeSuperior-denseTime + deltaT*(k-1), timeSuperior-denseTime + deltaT*k)
        
      println("TimeRange: "+scan.getTimeRange.toString())  
    
      val scanner = HogHBaseRDD.hogzilla_flows.getScanner(scan).iterator()
    
      while(scanner.hasNext())
      {
        HogHBaseRDD.hogzilla_flows.delete(new Delete(scanner.next().getRow))
      }
    }
    
    
 
    /*
   
   
     //scan.setStartRow(Bytes.toBytes("0"))
      //scan.setStopRow(Bytes.toBytes(time))
       * 
     //THIS CODE HAS BUGS
   
    // TODO HZ: Update flow:inter_time_stddev and flow:packet_size_stddev using "flow:inter_time-%d","flow:packet_size-%d"
   
        
    val prepareRDD = HogRDD.
        map { case (id,result) => {
          val map: Map[String,String] = new HashMap[String,String]
              map.put("flow:id",Bytes.toString(id.get).toString())
              HogHBaseRDD.columns.foreach { column => map.put(column, 
                  Bytes.toString(result.getValue(Bytes.toBytes(column.split(":")(0).toString()),Bytes.toBytes(column.split(":")(1).toString())))) 
        }
        map
        }
    }
    
    prepareRDD.filter(_.get("flow:packet_size_stddev").isEmpty()).map({
      map =>
        val avg=map.get("flow:avg_packet_size").toDouble
        var total:Double =0
        for(i <- 0 to map.get("flow:packets").toInt-1)
        {
          total=total+ (map.get("flow:packet_size-"+i.toString).toDouble-avg) * (map.get("flow:packet_size-"+i.toString).toDouble-avg)
        }
        // TODO HZ: Salve in HBase here
        // ID: map.get("flow:id")
        map.put("flow:packet_size_stddev",total.toString())
        
        val mutation = new RowMutations()
        val put = new Put(Bytes.toBytes(map.get("flow:id")))
        put.add(Bytes.toBytes("flow"), Bytes.toBytes("packet_size_stddev"), Bytes.toBytes(Math.sqrt(total)))
        mutation.add(put)
        HogHBaseRDD.hogzilla_flows.mutateRow(mutation)
    })
   */

  }
  
}