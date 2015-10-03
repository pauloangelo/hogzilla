package org.hogzilla.event

import org.hogzilla.hbase.HogHBaseRDD
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put



/**
 * @author pa
 */
case class HogSignature(signature_class:Int, signature_name:String, signature_priority:Int, signature_revision:Int, signature_id:Double,signature_group_id:Int) {
  //Example: 3,"HZ: Suspicious DNS flow identified by K-Means clustering",2,1,826000001,826
  
  def saveHBase():HogSignature =
  {
    val get = new Get(Bytes.toBytes("%.0f".format(signature_id)))
    
    if(!HogHBaseRDD.hogzilla_sensor.exists(get))
    {
      val put = new Put(Bytes.toBytes("%.0f".format(signature_id)))
      put.add(Bytes.toBytes("signature"), Bytes.toBytes("id"), Bytes.toBytes("%.0f".format(signature_id)))
      put.add(Bytes.toBytes("signature"), Bytes.toBytes("class"), Bytes.toBytes(signature_class.toString()))
      put.add(Bytes.toBytes("signature"), Bytes.toBytes("name"), Bytes.toBytes(signature_name))
      put.add(Bytes.toBytes("signature"), Bytes.toBytes("priority"), Bytes.toBytes(signature_priority.toString()))
      put.add(Bytes.toBytes("signature"), Bytes.toBytes("revision"), Bytes.toBytes(signature_revision.toString()))
      put.add(Bytes.toBytes("signature"), Bytes.toBytes("group_id"), Bytes.toBytes(signature_group_id.toString()))
      HogHBaseRDD.hogzilla_signatures.put(put)
    }
    
    this
  }
}