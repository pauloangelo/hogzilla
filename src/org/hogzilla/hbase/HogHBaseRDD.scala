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


object HogHBaseRDD {
  
  val conf = HBaseConfiguration.create()
  val admin = new HBaseAdmin(conf)
  val columns = List("flow:lower_ip","flow:lower_ip","flow:lower_ip","flow:upper_ip","flow:lower_name","flow:upper_name","flow:lower_port","flow:upper_port","flow:protocol","flow:vlan_id","flow:last_seen","flow:bytes","flow:packets","flow:flow_duration","flow:first_seen","flow:max_packet_size","flow:min_packet_size","flow:avg_packet_size","flow:payload_bytes","flow:payload_first_size","flow:payload_avg_size","flow:payload_min_size","flow:payload_max_size","flow:packets_without_payload","flow:detection_completed","flow:detected_protocol","flow:host_server_name","flow:inter_time_stddev","flow:packet_size_stddev","flow:avg_inter_time",
                     "flow:packet_size-0","flow:inter_time-0","flow:packet_size-1","flow:inter_time-1","flow:packet_size-2","flow:inter_time-2","flow:packet_size-3","flow:inter_time-3","flow:packet_size-4","flow:inter_time-4",
                     "flow:detected_os",
                     "flow:dns_num_queries","flow:dns_num_answers","flow:dns_ret_code","flow:dns_bad_packet","flow:dns_query_type","flow:dns_query_class","flow:dns_rsp_type",
                     "event:sensor_id","event:event_id","event:event_second","event:event_microsecond","event:signature_id","event:generator_id","event:classification_id","event:priority_id",
                     "flow:http_method","flow:http_url","flow:http_content_type") 
 
  val columnsSFlow = List("flow:IPprotocol","flow:IPsize","flow:agentID","flow:dstIP","flow:dstMAC","flow:dstPort","flow:ethernetType","flow:inVlan","flow:inputPort","flow:ipTos",
                          "flow:ipTtl","flow:outVlan","flow:outputPort","flow:packetSize","flow:samplingRate","flow:srcIP","flow:srcMAC","flow:srcPort","flow:tcpFlags",
                          "flow:timestamp")
                     
  // "flow:inter_time-%d","flow:packet_size-%d"

  val hogzilla_flows      = new HTable(conf,"hogzilla_flows")
  val hogzilla_sflows     = new HTable(conf,"hogzilla_sflows")
  val hogzilla_events     = new HTable(conf,"hogzilla_events")
  val hogzilla_sensor     = new HTable(conf,"hogzilla_sensor")
  val hogzilla_signatures = new HTable(conf,"hogzilla_signatures")
  val hogzilla_mynets     = new HTable(conf,"hogzilla_mynets")
  val hogzilla_reputation = new HTable(conf,"hogzilla_reputation")
  val hogzilla_histograms = new HTable(conf,"hogzilla_histograms")

  
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
     println("Table hogzilla_sflows does not exist.")
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