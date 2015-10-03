package org.hogzilla.initiate

import org.apache.spark._
import org.hogzilla.hbase.HogHBaseRDD
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put


object HogInitiate {
   
  val sensor_description="Hogzilla IDS"
  val sensor_hostname="hoghostname"
   
    
  def initiate(spark: SparkContext)
  {
   
    val get = new Get(Bytes.toBytes("1"))
    
    if(!HogHBaseRDD.hogzilla_sensor.exists(get))
    {
      val put = new Put(Bytes.toBytes("1"))
      put.add(Bytes.toBytes("sensor"), Bytes.toBytes("description"), Bytes.toBytes(sensor_description))
      put.add(Bytes.toBytes("sensor"), Bytes.toBytes("hostname"), Bytes.toBytes(sensor_hostname))
      HogHBaseRDD.hogzilla_sensor.put(put)
    }
    
  }
      
}