/*
* Copyright (C) 2015-2015 Paulo Angelo Alves Resende <pa@pauloangelo.com>
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
import org.hogzilla.util.HogFeature
import scala.collection.mutable.HashSet


object HogHBaseRDD {
  
  val conf = HBaseConfiguration.create()
  val admin = new HBaseAdmin(conf)
  val columns = new HashSet()++
  List(
      new HogFeature("flow:first_seen","u_int64_t",false),
      new HogFeature("flow:bittorent_hash","char",false),
      new HogFeature("flow:info","char",false),
      new HogFeature("flow:host_server_name","char",false),
      new HogFeature("flow:ssh_ssl_client_info","char",false),
      new HogFeature("flow:ssh_ssl_server_info","char",false),
      new HogFeature("flow:src_ip","u_int32_t",false),
      new HogFeature("flow:dst_ip","u_int32_t",false),
      new HogFeature("flow:src_port","u_int16_t",false),
      new HogFeature("flow:dst_port","u_int16_t",false),
      new HogFeature("flow:protocol","char",true,false),
//      new HogFeature("flow:bidirectional","u_int8_t"),
      new HogFeature("flow:src_name","char",false),
      new HogFeature("flow:dst_name","char",false),
      new HogFeature("flow:bytes","u_int64_t"),
      new HogFeature("flow:packets","u_int32_t"),
      new HogFeature("flow:payload_bytes","u_int64_t"),
      new HogFeature("flow:packets_without_payload","u_int32_t"),
      new HogFeature("flow:payload_bytes_first","u_int32_t"),
      new HogFeature("flow:flow_duration","u_int64_t"),
      new HogFeature("flow:flow_use_time","u_int64_t"),
      new HogFeature("flow:flow_idle_time","u_int64_t"),
      new HogFeature("flow:src2dst_pay_bytes","u_int64_t"),
      new HogFeature("flow:dst2src_pay_bytes","u_int64_t"),
      new HogFeature("flow:src2dst_header_bytes","u_int64_t"),
      new HogFeature("flow:dst2src_header_bytes","u_int64_t"),
      new HogFeature("flow:src2dst_packets","u_int32_t"),
      new HogFeature("flow:dst2src_packets","u_int32_t"),
      new HogFeature("flow:src2dst_inter_time_avg","u_int64_t"),
      new HogFeature("flow:src2dst_inter_time_min","u_int64_t"),
      new HogFeature("flow:src2dst_inter_time_max","u_int64_t"),
      new HogFeature("flow:src2dst_inter_time_std","u_int64_t"),
      new HogFeature("flow:dst2src_inter_time_avg","u_int64_t"),
      new HogFeature("flow:dst2src_inter_time_min","u_int64_t"),
      new HogFeature("flow:dst2src_inter_time_max","u_int64_t"),
      new HogFeature("flow:dst2src_inter_time_std","u_int64_t"),
      new HogFeature("flow:src2dst_pay_bytes_avg","u_int64_t"),
      new HogFeature("flow:src2dst_pay_bytes_min","u_int64_t"),
      new HogFeature("flow:src2dst_pay_bytes_max","u_int64_t"),
      new HogFeature("flow:src2dst_pay_bytes_std","u_int64_t"),
      new HogFeature("flow:dst2src_pay_bytes_avg","u_int64_t"),
      new HogFeature("flow:dst2src_pay_bytes_min","u_int64_t"),
      new HogFeature("flow:dst2src_pay_bytes_max","u_int64_t"),
      new HogFeature("flow:dst2src_pay_bytes_std","u_int64_t"),
      new HogFeature("flow:dst2src_pay_bytes_rate","u_int64_t"),
      new HogFeature("flow:src2dst_pay_bytes_rate","u_int64_t"),
      new HogFeature("flow:dst2src_packets_rate","u_int64_t"),
      new HogFeature("flow:src2dst_packets_rate","u_int64_t"),
      new HogFeature("flow:inter_time_avg","u_int64_t"),
      new HogFeature("flow:inter_time_min","u_int64_t"),
      new HogFeature("flow:inter_time_max","u_int64_t"),
      new HogFeature("flow:inter_time_std","u_int64_t"),
      new HogFeature("flow:payload_bytes_avg","u_int64_t"),
      new HogFeature("flow:payload_bytes_std","u_int64_t"),
      new HogFeature("flow:payload_bytes_min","u_int64_t"),
      new HogFeature("flow:payload_bytes_max","u_int64_t"),
      new HogFeature("flow:src2dst_header_bytes_avg","u_int64_t"),
      new HogFeature("flow:src2dst_header_bytes_min","u_int64_t"),
      new HogFeature("flow:src2dst_header_bytes_max","u_int64_t"),
      new HogFeature("flow:src2dst_header_bytes_std","u_int64_t"),
      new HogFeature("flow:dst2src_header_bytes_avg","u_int64_t"),
      new HogFeature("flow:dst2src_header_bytes_min","u_int64_t"),
      new HogFeature("flow:dst2src_header_bytes_max","u_int64_t"),
      new HogFeature("flow:dst2src_header_bytes_std","u_int64_t"),
      new HogFeature("flow:packets_syn","u_int32_t"),
      new HogFeature("flow:packets_ack","u_int32_t"),
      new HogFeature("flow:packets_fin","u_int32_t"),
      new HogFeature("flow:packets_rst","u_int32_t"),
      new HogFeature("flow:packets_psh","u_int32_t"),
      new HogFeature("flow:packets_urg","u_int32_t"),
      new HogFeature("flow:tcp_retransmissions","u_int32_t"),
//      new HogFeature("flow:payload_size_variation","u_int32_t"),
      new HogFeature("flow:C_number_of_contacts","u_int32_t"),
      new HogFeature("flow:C_src2dst_pay_bytes_avg","u_int64_t"),
      new HogFeature("flow:C_src2dst_pay_bytes_min","u_int64_t"),
      new HogFeature("flow:C_src2dst_pay_bytes_max","u_int64_t"),
      new HogFeature("flow:C_src2dst_pay_bytes_std","u_int64_t"),
      new HogFeature("flow:C_src2dst_header_bytes_avg","u_int64_t"),
      new HogFeature("flow:C_src2dst_header_bytes_min","u_int64_t"),
      new HogFeature("flow:C_src2dst_header_bytes_max","u_int64_t"),
      new HogFeature("flow:C_src2dst_header_bytes_std","u_int64_t"),
      new HogFeature("flow:C_src2dst_packets_avg","u_int64_t"),
      new HogFeature("flow:C_src2dst_packets_min","u_int64_t"),
      new HogFeature("flow:C_src2dst_packets_max","u_int64_t"),
      new HogFeature("flow:C_src2dst_packets_std","u_int64_t"),
      new HogFeature("flow:C_dst2src_pay_bytes_avg","u_int64_t"),
      new HogFeature("flow:C_dst2src_pay_bytes_min","u_int64_t"),
      new HogFeature("flow:C_dst2src_pay_bytes_max","u_int64_t"),
      new HogFeature("flow:C_dst2src_pay_bytes_std","u_int64_t"),
      new HogFeature("flow:C_dst2src_header_bytes_avg","u_int64_t"),
      new HogFeature("flow:C_dst2src_header_bytes_min","u_int64_t"),
      new HogFeature("flow:C_dst2src_header_bytes_max","u_int64_t"),
      new HogFeature("flow:C_dst2src_header_bytes_std","u_int64_t"),
      new HogFeature("flow:C_dst2src_packets_avg","u_int64_t"),
      new HogFeature("flow:C_dst2src_packets_min","u_int64_t"),
      new HogFeature("flow:C_dst2src_packets_max","u_int64_t"),
      new HogFeature("flow:C_dst2src_packets_std","u_int64_t"),
      new HogFeature("flow:C_packets_syn_avg","u_int64_t"),
      new HogFeature("flow:C_packets_syn_min","u_int64_t"),
      new HogFeature("flow:C_packets_syn_max","u_int64_t"),
      new HogFeature("flow:C_packets_syn_std","u_int64_t"),
      new HogFeature("flow:C_packets_ack_avg","u_int64_t"),
      new HogFeature("flow:C_packets_ack_min","u_int64_t"),
      new HogFeature("flow:C_packets_ack_max","u_int64_t"),
      new HogFeature("flow:C_packets_ack_std","u_int64_t"),
      new HogFeature("flow:C_packets_fin_avg","u_int64_t"),
      new HogFeature("flow:C_packets_fin_min","u_int64_t"),
      new HogFeature("flow:C_packets_fin_max","u_int64_t"),
      new HogFeature("flow:C_packets_fin_std","u_int64_t"),
      new HogFeature("flow:C_packets_rst_avg","u_int64_t"),
      new HogFeature("flow:C_packets_rst_min","u_int64_t"),
      new HogFeature("flow:C_packets_rst_max","u_int64_t"),
      new HogFeature("flow:C_packets_rst_std","u_int64_t"),
      new HogFeature("flow:C_packets_psh_avg","u_int64_t"),
      new HogFeature("flow:C_packets_psh_min","u_int64_t"),
      new HogFeature("flow:C_packets_psh_max","u_int64_t"),
      new HogFeature("flow:C_packets_psh_std","u_int64_t"),
      new HogFeature("flow:C_packets_urg_avg","u_int64_t"),
      new HogFeature("flow:C_packets_urg_min","u_int64_t"),
      new HogFeature("flow:C_packets_urg_max","u_int64_t"),
      new HogFeature("flow:C_packets_urg_std","u_int64_t"),
      new HogFeature("flow:C_tcp_retransmissions_avg","u_int64_t"),
      new HogFeature("flow:C_tcp_retransmissions_min","u_int64_t"),
      new HogFeature("flow:C_tcp_retransmissions_max","u_int64_t"),
      new HogFeature("flow:C_tcp_retransmissions_std","u_int64_t"),
      new HogFeature("flow:C_dst2src_pay_bytes_rate_avg","u_int64_t"),
      new HogFeature("flow:C_dst2src_pay_bytes_rate_min","u_int64_t"),
      new HogFeature("flow:C_dst2src_pay_bytes_rate_max","u_int64_t"),
      new HogFeature("flow:C_dst2src_pay_bytes_rate_std","u_int64_t"),
      new HogFeature("flow:C_src2dst_pay_bytes_rate_avg","u_int64_t"),
      new HogFeature("flow:C_src2dst_pay_bytes_rate_min","u_int64_t"),
      new HogFeature("flow:C_src2dst_pay_bytes_rate_max","u_int64_t"),
      new HogFeature("flow:C_src2dst_pay_bytes_rate_std","u_int64_t"),
      new HogFeature("flow:C_dst2src_packets_rate_avg","u_int64_t"),
      new HogFeature("flow:C_dst2src_packets_rate_min","u_int64_t"),
      new HogFeature("flow:C_dst2src_packets_rate_max","u_int64_t"),
      new HogFeature("flow:C_dst2src_packets_rate_std","u_int64_t"),
      new HogFeature("flow:C_src2dst_packets_rate_avg","u_int64_t"),
      new HogFeature("flow:C_src2dst_packets_rate_min","u_int64_t"),
      new HogFeature("flow:C_src2dst_packets_rate_max","u_int64_t"),
      new HogFeature("flow:C_src2dst_packets_rate_std","u_int64_t"),
      new HogFeature("flow:C_duration_avg","u_int64_t"),
      new HogFeature("flow:C_duration_min","u_int64_t"),
      new HogFeature("flow:C_duration_max","u_int64_t"),
      new HogFeature("flow:C_duration_std","u_int64_t"),
      new HogFeature("flow:C_idletime_avg","u_int64_t"),
      new HogFeature("flow:C_idletime_min","u_int64_t"),
      new HogFeature("flow:C_idletime_max","u_int64_t"),
      new HogFeature("flow:C_idletime_std","u_int64_t"),
      new HogFeature("flow:response_rel_time","u_int32_t"),
      new HogFeature("flow:detection_completed","u_int8_t"),
      new HogFeature("flow:ndpi_risk",",char",false,false,1),
      new HogFeature("flow:detected_os","char",false),
      new HogFeature("flow:dns_num_queries","u_int32_t"),
      new HogFeature("flow:dns_num_answers","u_int32_t"),
      new HogFeature("flow:dns_reply_code","u_int32_t"),
      new HogFeature("flow:dns_query_type","u_int32_t"),
      new HogFeature("flow:dns_query_class","u_int32_t"),
      new HogFeature("flow:dns_rsp_type","u_int32_t"),
      
      new HogFeature("flow:http_url","char",false),
      new HogFeature("flow:http_content_type","char",true,false),
      new HogFeature("flow:http_method","u_int32_t"),
      new HogFeature("flow:http_num_request_headers","u_int32_t"),
      new HogFeature("flow:http_num_response_headers","u_int32_t"),
      new HogFeature("flow:http_request_version","u_int32_t"),
      new HogFeature("flow:http_response_status_code","u_int32_t"),
      
      
      new HogFeature("event:sensor_id","u_int32_t",false),
      new HogFeature("event:event_id","u_int32_t",false),
      new HogFeature("event:event_second","u_int64_t",false),
      new HogFeature("event:event_microsecond","u_int64_t",false),
      new HogFeature("event:signature_id","u_int64_t",false,false,1),
      new HogFeature("event:generator_id","u_int64_t",false),
      new HogFeature("event:classification_id","u_int32_t",false),
      new HogFeature("event:priority_id","u_int32_t",false)
      )

 
  val columnsSFlow = List("flow:IPprotocol","flow:IPsize","flow:agentID","flow:dstIP","flow:dstMAC","flow:dstPort","flow:ethernetType","flow:inVlan","flow:inputPort","flow:ipTos",
                          "flow:ipTtl","flow:outVlan","flow:outputPort","flow:packetSize","flow:samplingRate","flow:srcIP","flow:srcMAC","flow:srcPort","flow:tcpFlags",
                          "flow:timestamp")
                     
  // "flow:inter_time-%d","flow:packet_size-%d"

  val hogzilla_flows            = new HTable(conf,"hogzilla_flows")
  val hogzilla_sflows           = new HTable(conf,"hogzilla_sflows")
  val hogzilla_events           = new HTable(conf,"hogzilla_events")
  val hogzilla_sensor           = new HTable(conf,"hogzilla_sensor")
  val hogzilla_signatures       = new HTable(conf,"hogzilla_signatures")
  val hogzilla_mynets           = new HTable(conf,"hogzilla_mynets")
  val hogzilla_reputation       = new HTable(conf,"hogzilla_reputation")
  val hogzilla_histograms       = new HTable(conf,"hogzilla_histograms")
  val hogzilla_clusters         = new HTable(conf,"hogzilla_clusters")
  val hogzilla_cluster_members  = new HTable(conf,"hogzilla_cluster_members")
  val hogzilla_inventory        = new HTable(conf,"hogzilla_inventory")
  val hogzilla_authrecords        = new HTable(conf,"hogzilla_authrecords")

  
  def connect(spark: SparkContext):RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)]=
  {
    val table = "hogzilla_flows"
 
    conf.set(TableInputFormat.INPUT_TABLE, table)
    conf.set("zookeeper.session.timeout", "1800000")
    conf.setInt("hbase.client.scanner.timeout.period", 1800000)
    // You can limit the SCANNED COLUMNS here  
    conf.set("hbase.rpc.timeout", "1800000")
    //conf.set(TableInputFormat.SCAN_COLUMNS, "flow:packets,flow:detected_protocol"),

   
   if (!admin.isTableAvailable(table)) {
     println("Table hogzilla_flows does not exist.")
    }
    
     val hBaseRDD = spark.newAPIHadoopRDD(conf, classOf[TableInputFormat],
                                                classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
                                                classOf[org.apache.hadoop.hbase.client.Result])
                                                
     return hBaseRDD
  }
  
  def connectSFlow(spark: SparkContext):RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)]=
  {
    val table = "hogzilla_sflows"
 
    conf.set(TableInputFormat.INPUT_TABLE, table)
    conf.set("zookeeper.session.timeout", "600000")
    conf.setInt("hbase.client.scanner.timeout.period", 600000)
    //conf.set("hbase.rpc.timeout", "1800000")
    // You can limit the SCANNED COLUMNS here  
    //conf.set(TableInputFormat.SCAN_COLUMNS, "flow:packets,flow:detected_protocol"),

   
   if (!admin.isTableAvailable(table)) {
     println("Table hogzilla_sflows does not exist.")
    }
    
     val hBaseRDD = spark.newAPIHadoopRDD(conf, classOf[TableInputFormat],
                                                classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
                                                classOf[org.apache.hadoop.hbase.client.Result])
                                                
     return hBaseRDD
  }
  
  
    
  def connectHistograms(spark: SparkContext):RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)]=
  {
    val table = "hogzilla_histograms"
 
    conf.set(TableInputFormat.INPUT_TABLE, table)
    conf.set("zookeeper.session.timeout", "600000")
    conf.setInt("hbase.client.scanner.timeout.period", 600000)
    //conf.set("hbase.rpc.timeout", "1800000")
    // You can limit the SCANNED COLUMNS here  
    //conf.set(TableInputFormat.SCAN_COLUMNS, "flow:packets,flow:detected_protocol"),

   
   if (!admin.isTableAvailable(table)) {
     println("Table hogzilla_histograms does not exist.")
    }
    
     val hBaseRDD = spark.newAPIHadoopRDD(conf, classOf[TableInputFormat],
                                                classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
                                                classOf[org.apache.hadoop.hbase.client.Result])
                                                
     return hBaseRDD
  }
  
  
  
  def connectAuth(spark: SparkContext):RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)]=
  {
    val table = "hogzilla_authrecords"
 
    conf.set(TableInputFormat.INPUT_TABLE, table)
    conf.set("zookeeper.session.timeout", "600000")
    conf.setInt("hbase.client.scanner.timeout.period", 600000)
    //conf.set("hbase.rpc.timeout", "1800000")
    // You can limit the SCANNED COLUMNS here  
    //conf.set(TableInputFormat.SCAN_COLUMNS, "flow:packets,flow:detected_protocol"),

   
   if (!admin.isTableAvailable(table)) {
     println("Table hogzilla_authrecords does not exist.")
    }
    
     val hBaseRDD = spark.newAPIHadoopRDD(conf, classOf[TableInputFormat],
                                                classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
                                                classOf[org.apache.hadoop.hbase.client.Result])
                                                
     return hBaseRDD
  }
  
  def close()
  {
     admin.close()
  }
    
}