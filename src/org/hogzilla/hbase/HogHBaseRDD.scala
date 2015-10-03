package org.hogzilla.hbase


/**
 * @author pa
 */

import scala.math.random
import java.lang.Math
import org.apache.spark._
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.mllib.regression.{LabeledPoint,LinearRegressionModel,LinearRegressionWithSGD}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.client.HTable


object HogHBaseRDD {
  
  val conf = HBaseConfiguration.create()
  val admin = new HBaseAdmin(conf)
  val columns = List("flow:packet_size-0","flow:inter_time-0","flow:lower_ip","flow:lower_ip","flow:lower_ip","flow:upper_ip","flow:lower_name","flow:upper_name","flow:lower_port","flow:upper_port","flow:protocol","flow:vlan_id","flow:last_seen","flow:bytes","flow:packets","flow:flow_duration","flow:first_seen","flow:max_packet_size","flow:min_packet_size","flow:avg_packet_size","flow:payload_bytes","flow:payload_first_size","flow:payload_avg_size","flow:payload_min_size","flow:payload_max_size","flow:packets_without_payload","flow:detection_completed","flow:detected_protocol","flow:host_server_name","flow:inter_time_stddev","flow:packet_size_stddev","flow:avg_inter_time","flow:dns_num_queries","flow:dns_num_answers","flow:dns_ret_code","flow:dns_bad_packet","flow:dns_query_type","flow:dns_query_class","flow:dns_rsp_type","event:sensor_id","event:event_id","event:event_second","event:event_microsecond","event:signature_id","event:generator_id","event:classification_id","event:priority_id") 
  // "flow:inter_time-%d","flow:packet_size-%d"

  val hogzilla_flows = new HTable(conf,"hogzilla_flows")
  val hogzilla_events = new HTable(conf,"hogzilla_events")
  val hogzilla_sensor = new HTable(conf,"hogzilla_sensor")
  val hogzilla_signatures = new HTable(conf,"hogzilla_signatures")

  
  def connect(spark: SparkContext):RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)]=
  {
    val tabela = "hogzilla_flows"
 
    conf.set(TableInputFormat.INPUT_TABLE, tabela)
    // You can limit the SCANNED COLUMNS here
    //conf.set(TableInputFormat.SCAN_COLUMNS, "flow:packets,flow:detected_protocol"),

   
   if (!admin.isTableAvailable(tabela)) {
     println("Table hogzilla_flows does not exist.")
    }
     val hBaseRDD = spark.newAPIHadoopRDD(conf, classOf[TableInputFormat],
                                                classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
                                                classOf[org.apache.hadoop.hbase.client.Result])
    return hBaseRDD;
   
  }
  
    
  def close()
  {
     admin.close()
  }
    
}