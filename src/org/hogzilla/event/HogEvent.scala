package org.hogzilla.event


import java.util.HashMap
import java.util.Map
import org.apache.hadoop.hbase.client.RowMutations
import org.hogzilla.hbase.HogHBaseRDD
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put


class HogEvent(flow:Map[String,String]) 
{
	var sensorid:Int=0
	//var eventsecond:Int=0
	var signature_id:Double=0
	//var generatorid:Int=0
	var priorityid:Int=0
  var lower_ip:String=""
  var upper_ip:String=""
	var text:String=""
	var data:Map[String,String]=new HashMap()

   def alert()
   {
	   val put = new Put(Bytes.toBytes(flow.get("flow:id")))
     put.add(Bytes.toBytes("event"), Bytes.toBytes("note"), Bytes.toBytes(text))
     put.add(Bytes.toBytes("event"), Bytes.toBytes("lower_ip"), Bytes.toBytes(lower_ip))
     put.add(Bytes.toBytes("event"), Bytes.toBytes("upper_ip"), Bytes.toBytes(upper_ip))
     put.add(Bytes.toBytes("event"), Bytes.toBytes("signature_id"), Bytes.toBytes("%.0f".format(signature_id)))
     HogHBaseRDD.hogzilla_events.put(put)

   }
}

