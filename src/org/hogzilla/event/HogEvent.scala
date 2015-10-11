package org.hogzilla.event

import java.util.HashMap
import java.util.Map
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.hogzilla.hbase.HogHBaseRDD
import org.hogzilla.util.HogFlow


class HogEvent(flow:HogFlow) 
{
	var sensorid:Int=0
	var signature_id:Double=0
	var priorityid:Int=0
	var text:String=""
	var data:Map[String,String]=new HashMap()

   def alert()
   {
	   val put = new Put(Bytes.toBytes(flow.get("flow:id")))
     put.add(Bytes.toBytes("event"), Bytes.toBytes("note"), Bytes.toBytes(text))
     put.add(Bytes.toBytes("event"), Bytes.toBytes("lower_ip"), flow.lower_ip)
     put.add(Bytes.toBytes("event"), Bytes.toBytes("upper_ip"), flow.upper_ip)
     put.add(Bytes.toBytes("event"), Bytes.toBytes("signature_id"), Bytes.toBytes("%.0f".format(signature_id)))
     put.add(Bytes.toBytes("event"), Bytes.toBytes("time"), Bytes.toBytes(System.currentTimeMillis))
     HogHBaseRDD.hogzilla_events.put(put)

     //println(f"ALERT: $text%100s\n\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
   }
}

