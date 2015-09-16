package org.hogzilla.prepare

import java.util.HashMap
import java.util.Map
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.hogzilla.hbase.HogHBaseRDD
import org.apache.hadoop.hbase.client.RowMutations
import org.apache.hadoop.hbase.client.Put


object HogPrepare {
  
  def prepare(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)])
  {
 
   
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
   

  }
  
}