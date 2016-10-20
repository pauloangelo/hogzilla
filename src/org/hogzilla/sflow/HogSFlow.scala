/*
* Copyright (C) 2015-2016 Paulo Angelo Alves Resende <pa@pauloangelo.com>
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
/** 
 *  REFERENCES:
 *   - http://ids-hogzilla.org/xxx/826000101
 */


package org.hogzilla.sflow

import java.net.InetAddress
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.Map
import scala.math.floor
import scala.math.log
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.hogzilla.event.HogEvent
import org.hogzilla.event.HogSignature
import org.hogzilla.hbase.HogHBaseHistogram
import org.hogzilla.hbase.HogHBaseRDD
import org.hogzilla.hbase.HogHBaseReputation
import org.hogzilla.histogram.Histograms
import org.hogzilla.histogram.HogHistogram
import org.hogzilla.util.HogFlow
import org.apache.commons.math3.analysis.function.Min
import org.hogzilla.hbase.HogHBaseInventory


/**
 * 
 */
object HogSFlow {

     
  val signature = (HogSignature(3,"HZ: Top talker identified" ,                2,1,826001001,826).saveHBase(),//1
                   HogSignature(3,"HZ: SMTP talker identified",                1,1,826001002,826).saveHBase(),//2
                   HogSignature(3,"HZ: Atypical TCP port used",                2,1,826001003,826).saveHBase(),//3
                   HogSignature(3,"HZ: Atypical alien TCP port used",          2,1,826001004,826).saveHBase(),//4
                   HogSignature(3,"HZ: Atypical number of pairs in the period",2,1,826001005,826).saveHBase(),//5
                   HogSignature(3,"HZ: Atypical amount of data transferred",   2,1,826001006,826).saveHBase(),//6
                   HogSignature(3,"HZ: Alien accessing too much hosts",        3,1,826001007,826).saveHBase(),//7
                   HogSignature(3,"HZ: P2P communication",                     3,1,826001008,826).saveHBase(),//8
                   HogSignature(3,"HZ: UDP amplifier (DDoS)",                  1,1,826001009,826).saveHBase(),//9
                   HogSignature(3,"HZ: Abused SMTP Server",                    1,1,826001010,826).saveHBase(),//10
                   HogSignature(3,"HZ: Media streaming client",                3,1,826001011,826).saveHBase(),//11
                   HogSignature(3,"HZ: DNS Tunnel",                            1,1,826001012,826).saveHBase(),//12
                   HogSignature(3,"HZ: ICMP Tunnel",                           1,1,826001013,826).saveHBase(),//13
                   HogSignature(3,"HZ: Horizontal portscan",                   2,1,826001014,826).saveHBase(),//14
                   HogSignature(3,"HZ: Vertical portscan",                     2,1,826001015,826).saveHBase(),//15
                   HogSignature(3,"HZ: Server under DDoS attack",              1,1,826001016,826).saveHBase())//16
  
  val alienThreshold = 20
  val topTalkersThreshold:Long = 21474836480L // (20*1024*1024*1024 = 20G)
  val SMTPTalkersThreshold:Long = 20971520L // (20*1024*1024 = 20M)
  val atypicalTCPPort:Set[String] = Set("80","443","587","465","993","995")
  val atypicalPairsThresholdMIN = 300
  val atypicalAmountDataThresholdMIN = 5737418240L // (10*1024*1024*1024 = 5G) 
  val p2pPairsThreshold = 5
  val p2pMyPortsThreshold = 4
  val abusedSMTPBytesThreshold = 50000000L // ~50 MB
  val p2pBytes2ndMethodThreshold = 10000000L // ~10 MB
  val p2pPairs2ndMethodThreshold = 10
  val p2pDistinctPorts2ndMethodThreshold = 10
  val mediaClientCommunicationDurationThreshold = 300 // 5min (300s)
  val mediaClientCommunicationDurationMAXThreshold = 7200 // 2h
  val mediaClientPairsThreshold = p2pPairs2ndMethodThreshold
  val mediaClientUploadThreshold = 10000000L // ~10MB
  //val mediaClientDownloadThreshold = 10000000L // ~10MB
  val mediaClientDownloadThreshold = 1000000L // 1MB
  val dnsTunnelThreshold = 50000000L // ~50 MB
  val bigProviderThreshold = 1073741824L // (1*1024*1024*1024 = 1G)
  val icmpTunnelThreshold = 200 // 200b
  val icmpTotalTunnelThreshold = 100000000L // ~100MB
  val hPortScanMinFlowsThreshold = 100
  val vPortScanMinPortsThreshold = 3
  val vPortScanPortIntervalThreshold = 1024 // 1 to 1023
  val ddosMinConnectionsThreshold = 50 // Over this, can be considered
  val ddosMinPairsThreshold = 20
  
  
  
  
  /**
   * 
   * 
   * 
   */
  def run(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)],spark:SparkContext)
  {
   // TopTalkers, SMTP Talkers, XXX: Organize it!
   top(HogRDD,spark)
 
  }
  
  
  def populateTopTalker(event:HogEvent):HogEvent =
  {
    val hostname:String = event.data.get("hostname")
    val bytesUp:String = event.data.get("bytesUp")
    val bytesDown:String = event.data.get("bytesDown")
    val threshold:String = event.data.get("threshold")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Large amount of sent data (>"+threshold+")\n"+
                  "IP: "+hostname+"\n"+
                  "Bytes Up: "+humanBytes(bytesUp)+"\n"+
                  "Bytes Down: "+humanBytes(bytesDown)+"\n"+
                  "Packets: "+numberPkts+"\n"+
                  "Flows"+stringFlows
                    
    event.signature_id = signature._1.signature_id       
    event
  }
  
  def populateSMTPTalker(event:HogEvent):HogEvent =
  {
    val hostname:String = event.data.get("hostname")
    val bytesUp:String = event.data.get("bytesUp")
    val bytesDown:String = event.data.get("bytesDown")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    val connections:String = event.data.get("connections")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: SMTP communication\n"+
                  "IP: "+hostname+"\n"+
                  "Bytes Up: "+humanBytes(bytesUp)+"\n"+
                  "Bytes Down: "+humanBytes(bytesDown)+"\n"+
                  "Packets: "+numberPkts+"\n"+
                  "Connections: "+connections+"\n"+
                  "Flows"+stringFlows
                  
    event.signature_id = signature._2.signature_id       
    event
  }
  
  
  def populateAtypicalTCPPortUsed(event:HogEvent):HogEvent =
  {
    val tcpport:String = event.data.get("tcpport")
    val myIP:String = event.data.get("myIP")
    val bytesUp:String = event.data.get("bytesUp")
    val bytesDown:String = event.data.get("bytesDown")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Atypical TCP port used ("+tcpport+")\n"+
                  "IP: "+myIP+"\n"+
                  "Bytes Up: "+humanBytes(bytesUp)+"\n"+
                  "Bytes Down: "+humanBytes(bytesDown)+"\n"+
                  "Packets: "+numberPkts+"\n"+
                  "Flows"+stringFlows
                  
    event.signature_id = signature._3.signature_id       
    event
  }
  
  def populateAtypicalAlienTCPPortUsed(event:HogEvent):HogEvent =
  {
    
    val tcpport:String = event.data.get("tcpport")
    val myIP:String = event.data.get("myIP")
    val bytesUp:String = event.data.get("bytesUp")
    val bytesDown:String = event.data.get("bytesDown")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Atypical alien TCP port used ("+tcpport+")\n"+
                  "IP: "+myIP+"\n"+
                  "Bytes Up: "+humanBytes(bytesUp)+"\n"+
                  "Bytes Down: "+humanBytes(bytesDown)+"\n"+
                  "Total packets: "+numberPkts+"\n"+
                  "Flows matching the atypical ports"+stringFlows
                  
    event.signature_id = signature._4.signature_id       
    event
  }
  
  
  def populateAtypicalNumberOfPairs(event:HogEvent):HogEvent =
  {
    val numberOfPairs:String = event.data.get("numberOfPairs")
    val myIP:String = event.data.get("myIP")
    val bytesUp:String = event.data.get("bytesUp")
    val bytesDown:String = event.data.get("bytesDown")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    val pairsMean:String = event.data.get("pairsMean")
    val pairsStdev:String = event.data.get("pairsStdev")
    
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Atypical number of pairs in the period ("+numberOfPairs+")\n"+
                  "IP: "+myIP+"\n"+
                  "Bytes Up: "+humanBytes(bytesUp)+"\n"+
                  "Bytes Down: "+humanBytes(bytesDown)+"\n"+
                  "Packets: "+numberPkts+"\n"+
                  "Number of pairs: "+numberOfPairs+"\n"+
                  "Pairs Mean/Stddev (all MyHosts): "+pairsMean+"/"+pairsStdev+"\n"+
                  "Flows"+stringFlows
                  
    event.signature_id = signature._5.signature_id       
    event
  }
  
            
          
  def populateAtypicalAmountData(event:HogEvent):HogEvent =
  {
    val numberOfPairs:String = event.data.get("numberOfPairs")
    val myIP:String = event.data.get("myIP")
    val bytesUp:String = event.data.get("bytesUp")
    val bytesDown:String = event.data.get("bytesDown")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    val dataMean:String = event.data.get("dataMean")
    val dataStdev:String = event.data.get("dataStdev")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Atypical amount of data uploaded ("+bytesUp+" bytes)\n"+
                  "IP: "+myIP+"\n"+
                  "Bytes Up: "+humanBytes(bytesUp)+"\n"+
                  "Bytes Down: "+humanBytes(bytesDown)+"\n"+
                  "Bytes Up Mean/Stddev (all MyHosts): "+humanBytes(dataMean)+"/"+humanBytes(dataStdev)+"\n"+
                  "Packets: "+numberPkts+"\n"+
                  "Number of pairs: "+numberOfPairs+"\n"+
                  "Flows"+stringFlows
                  
    event.signature_id = signature._6.signature_id       
    event
  }  
  
  
  def populateAlienAccessingManyHosts(event:HogEvent):HogEvent =
  {
    val numberOfPairs:String = event.data.get("numberOfPairs")
    val alienIP:String = event.data.get("alienIP")
    val bytesUp:String = event.data.get("bytesUp")
    val bytesDown:String = event.data.get("bytesDown")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Alien accessing too much hosts ("+numberOfPairs+")\n"+
                  "AlienIP: "+alienIP+"\n"+
                  "Bytes Up: "+humanBytes(bytesUp)+"\n"+
                  "Bytes Down: "+humanBytes(bytesDown)+"\n"+
                  "Packets: "+numberPkts+"\n"+
                  "Number of pairs: "+numberOfPairs+"\n"+
                  "Flows"+stringFlows
                  
    event.signature_id = signature._7.signature_id       
    event
  }  
  
  
  def populateP2PCommunication(event:HogEvent):HogEvent =
  {
    val numberOfPairs:String = event.data.get("numberOfPairs")
    val myIP:String = event.data.get("myIP")
    val bytesUp:String = event.data.get("bytesUp")
    val bytesDown:String = event.data.get("bytesDown")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: P2P Communication\n"+
                  "MyIP: "+myIP+"\n"+
                  "Bytes Up: "+humanBytes(bytesUp)+"\n"+
                  "Bytes Down: "+humanBytes(bytesDown)+"\n"+
                  "Packets: "+numberPkts+"\n"+
                  "Number of pairs: "+numberOfPairs+"\n"+
                  "Flows"+stringFlows
                  
    event.signature_id = signature._8.signature_id       
    event
  }  
  
  def populateUDPAmplifier(event:HogEvent):HogEvent =
  {
    val hostname:String = event.data.get("hostname")
    val bytesUp:String = event.data.get("bytesUp")
    val bytesDown:String = event.data.get("bytesDown")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    val connections:String = event.data.get("connections")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Host is sending too many big UDP packets. May be a DDoS.\n"+
                  "IP: "+hostname+"\n"+
                  "Bytes Up: "+humanBytes(bytesUp)+"\n"+
                  "Bytes Down: "+humanBytes(bytesDown)+"\n"+
                  "Packets: "+numberPkts+"\n"+
                  "Connections: "+connections+"\n"+
                  "Flows"+stringFlows
                  
    event.signature_id = signature._9.signature_id       
    event
  }
  
  def populateAbusedSMTP(event:HogEvent):HogEvent =
  {
    val hostname:String = event.data.get("hostname")
    val bytesUp:String = event.data.get("bytesUp")
    val bytesDown:String = event.data.get("bytesDown")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    val connections:String = event.data.get("connections")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Host is receiving too many e-mail submissions. May be an abused SMTP server. \n"+
                  "IP: "+hostname+"\n"+
                  "Bytes Up: "+humanBytes(bytesUp)+"\n"+
                  "Bytes Down: "+humanBytes(bytesDown)+"\n"+
                  "Packets: "+numberPkts+"\n"+
                  "Connections: "+connections+"\n"+
                  "Flows"+stringFlows
                  
    event.signature_id = signature._10.signature_id       
    event
  }
  
 
   
  def populateMediaClient(event:HogEvent):HogEvent =
  {
    val hostname:String = event.data.get("hostname")
    val bytesUp:String = event.data.get("bytesUp")
    val bytesDown:String = event.data.get("bytesDown")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    val connections:String = event.data.get("connections")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Appears to be a media streaming client.\n"+
                  "IP: "+hostname+"\n"+
                  "Bytes Up: "+humanBytes(bytesUp)+"\n"+
                  "Bytes Down: "+humanBytes(bytesDown)+"\n"+
                  "Packets: "+numberPkts+"\n"+
                  "Connections: "+connections+"\n"+
                  "Flows"+stringFlows
            
    event.signature_id = signature._11.signature_id          
    event
  }
  
     
  def populateDNSTunnel(event:HogEvent):HogEvent =
  {
    val hostname:String = event.data.get("hostname")
    val bytesUp:String = event.data.get("bytesUp")
    val bytesDown:String = event.data.get("bytesDown")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    val connections:String = event.data.get("connections")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Host has DNS communication with large amount of data. \n"+
                  "IP: "+hostname+"\n"+
                  "Bytes Up: "+humanBytes(bytesUp)+"\n"+
                  "Bytes Down: "+humanBytes(bytesDown)+"\n"+
                  "Packets: "+numberPkts+"\n"+
                  "Connections: "+connections+"\n"+
                  "Flows"+stringFlows
                  
    event.signature_id = signature._12.signature_id       
    event
  }
  
     
  def populateICMPTunnel(event:HogEvent):HogEvent =
  {
    val hostname:String = event.data.get("hostname")
    val bytesUp:String = event.data.get("bytesUp")
    val bytesDown:String = event.data.get("bytesDown")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    val connections:String = event.data.get("connections")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Host has DNS communication with large amount of data. \n"+
                  "IP: "+hostname+"\n"+
                  "Bytes Up: "+humanBytes(bytesUp)+"\n"+
                  "Bytes Down: "+humanBytes(bytesDown)+"\n"+
                  "Packets: "+numberPkts+"\n"+
                  "Connections: "+connections+"\n"+
                  "Flows"+stringFlows
                  
    event.signature_id = signature._13.signature_id       
    event
  }
  
  
  def populateHorizontalPortScan(event:HogEvent):HogEvent =
  {
    val numberOfFlows:String = event.data.get("numberOfFlows")
    val numberOfFlowsPerPort:String = event.data.get("numberOfFlowsPerPort")
    val myIP:String = event.data.get("myIP")
    val bytesUp:String = event.data.get("bytesUp")
    val bytesDown:String = event.data.get("bytesDown")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    val flowsMean:String = event.data.get("flowsMean")
    val flowsStdev:String = event.data.get("flowsStdev")
    val numberOfFlowsAlienPort:String = event.data.get("numberOfFlowsAlienPort")

    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Horizontal Port Scan \n"+
                  "IP: "+myIP+"\n"+
                  "Number of flows: "+numberOfFlows+"\n"+
                  "Number of flows per AlienPort: "+numberOfFlowsPerPort+"\n"+
                  "Number of flows per distinct AlienIP/AlienPort: "+numberOfFlowsAlienPort+"\n"+
                  "Mean/Stddev of flows per AlienPort (all flows for this IP): "+flowsMean+"/"+flowsStdev+"\n"+
                  "Bytes Up: "+humanBytes(bytesUp)+"\n"+
                  "Bytes Down: "+humanBytes(bytesDown)+"\n"+
                  "Packets: "+numberPkts+"\n"+
                  "Flows"+stringFlows
                  
    event.signature_id = signature._14.signature_id  
    event
  }
  
  
  def populateVerticalPortScan(event:HogEvent):HogEvent =
  {
    val numberOfFlows:String = event.data.get("numberOfFlows")
    val numberOfPorts:String = event.data.get("numberOfPorts")
    val myIP:String = event.data.get("myIP")
    val alienIP:String = event.data.get("alienIP")
    val bytesUp:String = event.data.get("bytesUp")
    val bytesDown:String = event.data.get("bytesDown")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    val portsMean:String = event.data.get("portsMean")
    val portsStdev:String = event.data.get("portsStdev")
    
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Vertical Port Scan \n"+
                  "IP: "+myIP+"\n"+
                  "Scanned AlienIP: "+alienIP+"\n"+
                  "Number of flows: "+numberOfFlows+"\n"+
                  "Number of distinct AlienPorts: "+numberOfPorts+"\n"+
                  "Mean/Stddev of AlienPorts per AlienIP (all flows for this IP): "+portsMean+"/"+portsStdev+"\n"+
                  "Bytes Up: "+humanBytes(bytesUp)+"\n"+
                  "Bytes Down: "+humanBytes(bytesDown)+"\n"+
                  "Packets: "+numberPkts+"\n"+
                  "Flows"+stringFlows
                  
    event.signature_id = signature._15.signature_id  
    event
  }
  
  
  
  def populateDDoSAttack(event:HogEvent):HogEvent =
  {
    val numberOfFlows:String = event.data.get("numberOfFlows")
    val numberOfAttackers:String = event.data.get("numberOfAttackers")
    val myIP:String = event.data.get("myIP")
    val bytesUp:String = event.data.get("bytesUp")
    val bytesDown:String = event.data.get("bytesDown")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    val flowsMean:String = event.data.get("flowsMean")
    val flowsStdev:String = event.data.get("flowsStdev")
    
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Horizontal Port Scan \n"+
                  "IP: "+myIP+"\n"+
                  "Number of Attackers: "+numberOfAttackers+"\n"+
                  "Number of flows: "+numberOfFlows+"\n"+
                  "Mean/Stddev of flows per AlienIP (all flows for this IP): "+flowsMean+"/"+flowsStdev+"\n"+
                  "Bytes Up: "+humanBytes(bytesUp)+"\n"+
                  "Bytes Down: "+humanBytes(bytesDown)+"\n"+
                  "Packets: "+numberPkts+"\n"+
                  "Flows"+stringFlows
                  
    event.signature_id = signature._16.signature_id  
    event
  }
 
  
  def setFlows2String(flowSet:HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)]):String =
  {
     flowSet.toList.sortBy({case (srcIP1,srcPort1,dstIP1,dstPort1,proto1,bytesUP,bytesDOWN,numberPkts1,direction1,beginTime,endTime,sampleRate,status) =>  bytesUP+bytesDOWN })
            .reverse
            ./:("")({ case (c,(srcIP1,srcPort1,dstIP1,dstPort1,proto1,bytesUP,bytesDOWN,numberPkts1,direction1,beginTime,endTime,sampleRate,status)) 
                        => 
                          val statusInd = { if(status>0) "[!]" else ""}
                          if(direction1>0)
                          {
                           c+"\n"+
                           srcIP1+":"+srcPort1+" => "+dstIP1+":"+dstPort1+" "+statusInd+" ("+proto1+", Up: "+humanBytes(bytesUP*sampleRate)+", Down: "+humanBytes(bytesDOWN*sampleRate)+","+numberPkts1+" pkts, duration: "+(endTime-beginTime)+"s, sampling: 1/"+sampleRate+")"
                          }else if(direction1<0)
                          {  
                           c+"\n"+
                           srcIP1+":"+srcPort1+" <= "+dstIP1+":"+dstPort1+" "+statusInd+" ("+proto1+", Down: "+humanBytes(bytesUP*sampleRate)+", Up: "+humanBytes(bytesDOWN*sampleRate)+","+numberPkts1+" pkts, duration: "+(endTime-beginTime)+"s, sampling: 1/"+sampleRate+")"
                          }else
                          {  
                           c+"\n"+
                           srcIP1+":"+srcPort1+" <?> "+dstIP1+":"+dstPort1+" "+statusInd+" ("+proto1+", L-to-R: "+humanBytes(bytesUP*sampleRate)+", R-to-L: "+humanBytes(bytesDOWN*sampleRate)+","+numberPkts1+" pkts, duration: "+(endTime-beginTime)+"s, sampling: 1/"+sampleRate+")"
                          }
                    })
  }
  
  
  def setFlowsICMP2String(flowSet:HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)]):String =
  {
     flowSet
      .toList
      .sortBy({case (srcIP1,srcPort1,dstIP1,dstPort1,proto1,bytesUP,bytesDOWN,numberPkts1,direction1,beginTime,endTime,sampleRate,status) =>  
                        bytesUP+bytesDOWN 
             })
      .reverse 
      ./:("")({ case (c,(srcIP1,icmpType,dstIP1,icmpCode,proto1,bytesUP,bytesDOWN,numberPkts1,direction1,beginTime,endTime,sampleRate,status)) 
                     => 
                     if(direction1>0)
                     {
                       c+"\n"+
                       srcIP1+" -> "+dstIP1+" ("+proto1+", Type/Code: "+icmpType+"/"+icmpCode+", L-to-R: "+humanBytes(bytesUP*sampleRate)+", R-to-L: "+humanBytes(bytesDOWN*sampleRate)+","+numberPkts1+" pkts, duration: "+(endTime-beginTime)+"s, sampling: 1/"+sampleRate+")"
                     }else if(direction1<0)
                     { 
                       c+"\n"+
                       srcIP1+" <- "+dstIP1+" ("+proto1+", Type/Code: "+icmpType+"/"+icmpCode+", L-to-R: "+humanBytes(bytesUP*sampleRate)+", R-to-L: "+humanBytes(bytesDOWN*sampleRate)+","+numberPkts1+" pkts, duration: "+(endTime-beginTime)+"s, sampling: 1/"+sampleRate+")"
                     }else
                     {
                       c+"\n"+
                       srcIP1+" <?> "+dstIP1+" ("+proto1+", Type/Code: "+icmpType+"/"+icmpCode+", L-to-R: "+humanBytes(bytesUP*sampleRate)+", R-to-L: "+humanBytes(bytesDOWN*sampleRate)+","+numberPkts1+" pkts, duration: "+(endTime-beginTime)+"s, sampling: 1/"+sampleRate+")"
                     }
                     
             })
  }
  
  def formatIPtoBytes(ip:String):Array[Byte] =
  {
    // Eca! Snorby doesn't support IPv6 yet. See https://github.com/Snorby/snorby/issues/65
    if(ip.contains(":"))
      InetAddress.getByName("255.255.6.6").getAddress
    else  
      InetAddress.getByName(ip).getAddress
  }
 

  def isMyIP(ip:String,myNets:Set[String]):Boolean =
  {
    myNets.map ({ net =>  if( ip.startsWith(net) )
                              { true } 
                          else{false} 
                }).contains(true)
  }
  
  def ipSignificantNetwork(ip:String):String =
  {
    if(ip.contains("."))
      return ip.substring(0,ip.lastIndexOf("."))
    
    if(ip.contains(":"))
      return ip.substring(0,ip.lastIndexOf(":"))
      
    ip
  }
  
   def humanBytes(b:Any):String =
   {
    val bytes=b.toString().toLong
    val unit = 1024L
    if (bytes < unit) return bytes + " B";
    val exp = (log(bytes) / log(unit)).toInt;
    val pre = "KMGTPE".charAt(exp-1)
    "%.1f%sB".format(bytes / math.pow(unit, exp), pre);
  }
  
  
  /**
   * 
   * 
   * 
   */
  def top(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)],spark:SparkContext)
  {
    
   val myNetsTemp =  new HashSet[String]
      
   val it = HogHBaseRDD.hogzilla_mynets.getScanner(new Scan()).iterator()
   while(it.hasNext())
   {
      myNetsTemp.add(Bytes.toString(it.next().getValue(Bytes.toBytes("net"),Bytes.toBytes("prefix"))))
   }
    
   val myNets:scala.collection.immutable.Set[String] = myNetsTemp.toSet
    
   println("Filtering SflowRDD...")
   println("My networks")
   myNets.foreach { println(_) }
   
  /*
  * 
  * SFlow Summary
  * 
  */   
    
  //Directions
  val UNKNOWN   = 0  
  val LEFTRIGHT = 1
  val RIGHTLEFT = -1
  val OCCURRED = 1
      
  val sflowSummary1: PairRDDFunctions[(String,String,String,String,String), (Long,Long,Long,Int,Long,Long,Long,Int)] 
                      = HogRDD
                        .map ({  case (id,result) => 
                                     /* Performance
                                      val map: Map[String,String] = new HashMap[String,String]
                                      map.put("flow:id",Bytes.toString(id.get).toString())
                                      HogHBaseRDD.columnsSFlow.foreach { column => 
                                      val ret = result.getValue(Bytes.toBytes(column.split(":")(0).toString()),Bytes.toBytes(column.split(":")(1).toString()))
                                      map.put(column, Bytes.toString(ret)) 
                                      }
                                      */
                                      val srcIP       = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("srcIP")))
                                      val srcPort     = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("srcPort")))
                                      val dstIP       = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("dstIP")))
                                      val dstPort     = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("dstPort")))
                                      val packetSize  = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("packetSize"))).toLong
                                      val tcpFlags    = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("tcpFlags")))
                                      val IPprotocol  = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("IPprotocol")))
                                      val timestamp   = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("timestamp"))).toLong
                                      val sampleRate   = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("samplingRate"))).toLong

                                      var direction = UNKNOWN
                                      var protoName="UDP" // We filter below TCP or UDP
                                      var status = UNKNOWN
                                      
                                      if(IPprotocol.equals("6")) // If is TCP
                                      {
                                        protoName="TCP"
                                        if(tcpFlags.equals("0x02")) // Is a SYN pkt
                                          direction = LEFTRIGHT
                                        if(tcpFlags.equals("0x12")) // Is a SYN-ACK pkt
                                        {
                                          direction = RIGHTLEFT
                                          status = OCCURRED
                                        }
                                        
                                        if(tcpFlags.equals("0x18")) // Is a PSH-ACK pkt
                                        {
                                          status = OCCURRED
                                        }
                                        
                                        if(tcpFlags.equals("0x10") & isMyIP(srcIP,myNets)) // Is a ACK pkt originated by a MyHost 
                                        {
                                          status = OCCURRED
                                        }
                                          
                                        // Suppose that ports < 1024 would not used for clients
                                        if(direction==UNKNOWN)
                                        {
                                          if(dstPort.toInt<1024)
                                            direction = LEFTRIGHT
                                            
                                          if(srcPort.toInt<1024)
                                            direction = RIGHTLEFT
                                        }
                                      }
                                      
                               if(!isMyIP(srcIP,myNets))
                               {
                                 ((  dstIP,
                                       dstPort,
                                       srcIP,
                                       srcPort, 
                                       protoName ), 
                                    (0L, packetSize, 1L,-direction,timestamp,timestamp,IPprotocol,sampleRate,status)
                                   )
                               }else
                               {
                                  ((  srcIP,
                                       srcPort,
                                       dstIP,
                                       dstPort, 
                                       protoName ), 
                                    (packetSize, 0L, 1L, direction,timestamp,timestamp,IPprotocol,sampleRate,status)
                                   )
                                   
                               } 
                           })
                           .filter({case ((myIP,myPort,alienIP,alienPort, proto ),(bytesUP,bytesDown,numberOfPkts,direction,beginTime,endTime,iPprotocolNumber,sampleRate,status))
                                             =>  iPprotocolNumber.equals("6") || iPprotocolNumber.equals("17") // TCP or UDP
                                  })
                           .map({case ((myIP,myPort,alienIP,alienPort, proto ),(bytesUP,bytesDown,numberOfPkts,direction,beginTime,endTime,iPprotocol,sampleRate,status))
                                    =>((myIP,myPort,alienIP,alienPort, proto ),(bytesUP,bytesDown,numberOfPkts,direction,beginTime,endTime,sampleRate,status))
                                })


    // (srcIP, srcPort, dstIP, dstPort, totalBytes, numberOfPkts)
  val sflowSummary = 
      sflowSummary1
      .reduceByKey({ case ((bytesUpA,bytesDownA,pktsA,directionA,beginTimeA,endTimeA,sampleRateA,statusA),(bytesUpB,bytesDownB,pktsB,directionB,beginTimeB,endTimeB,sampleRateB,statusB)) => 
                           (bytesUpA+bytesUpB,bytesDownA+bytesDownB,pktsA+pktsB,directionA+directionB,beginTimeA.min(beginTimeB),endTimeA.max(endTimeB),(sampleRateA+sampleRateB)/2,statusA+statusB)
                  })
      .cache
      
      
  val sflowSummaryICMP1: PairRDDFunctions[(String,String,String,String,String), (Long,Long,Long,Int,Long,Long,Long)] 
                      = HogRDD
                        .map ({  case (id,result) => 
                                     
                                      val srcIP       = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("srcIP")))
                                      val icmpType    = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("srcPort")))
                                      val dstIP       = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("dstIP")))
                                      val icmpCode    = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("dstPort")))
                                      val packetSize  = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("packetSize"))).toLong
                                      val IPprotocol  = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("IPprotocol")))
                                      val timestamp   = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("timestamp"))).toLong
                                      val sampleRate   = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("samplingRate"))).toLong

                                      var protoName="ICMPv6" 
                                      
                                      if(IPprotocol.equals("1")) // If is ICMP
                                      {
                                        protoName="ICMP"
                                      }
                                      
                               if(!isMyIP(srcIP,myNets))
                               {
                                 ((  dstIP,
                                       icmpType,
                                       srcIP,
                                       icmpCode, 
                                       protoName ), 
                                    (0L, packetSize, 1L,-1,timestamp,timestamp,IPprotocol,sampleRate)
                                   )
                               }else
                               {
                                  ((  srcIP,
                                       icmpType,
                                       dstIP,
                                       icmpCode, 
                                       protoName ), 
                                    (packetSize, 0L, 1L, 1,timestamp,timestamp,IPprotocol,sampleRate)
                                   )
                                   
                               } 
                           })
                           .filter({case ((myIP,icmpType,alienIP,icmpCode, proto ),(bytesUP,bytesDown,numberOfPkts,direction,beginTime,endTime,iPprotocolNumber,sampleRate))
                                             =>  iPprotocolNumber.equals("1") || iPprotocolNumber.equals("58") // ICMP or ICMPv6
                                  })
                           .map({case ((myIP,icmpType,alienIP,icmpCode, proto ),(bytesUP,bytesDown,numberOfPkts,direction,beginTime,endTime,iPprotocol,sampleRate))
                                    =>((myIP,icmpType,alienIP,icmpCode, proto ),(bytesUP,bytesDown,numberOfPkts,direction,beginTime,endTime,sampleRate))
                                })


  val sflowSummaryICMP = 
      sflowSummaryICMP1
      .reduceByKey({ case ((bytesUpA,bytesDownA,pktsA,directionA,beginTimeA,endTimeA,sampleRateA),(bytesUpB,bytesDownB,pktsB,directionB,beginTimeB,endTimeB,sampleRateB)) => 
                           (bytesUpA+bytesUpB,bytesDownA+bytesDownB,pktsA+pktsB,directionA+directionB,beginTimeA.min(beginTimeB),endTimeA.max(endTimeB),(sampleRateA+sampleRateB)/2)
                  })
      .cache
         
/* DEBUG
 sflowSummary
  .take(1000)
  .foreach({ case ((myIP,myPort,alienIP,alienPort, proto ),(bytesUP,bytesDown,numberOfPkts,direction,beginTime,endTime)) => 
                  println("MyIP:   \""+myIP+"\"    - "+isMyIP(myIP))
                  println("AlienIP:\""+alienIP+"\" - "+isMyIP(alienIP))
                  val flowSet = new HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long)]
                  flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytesUP,bytesDown,numberOfPkts,direction,beginTime,endTime))
                  println(setFlows2String(flowSet))
           })
  return
 */
      
  println("Counting sflowSummary...")
  val RDDtotalSize= sflowSummary.count()
  println("Filtered sflowSummary has "+RDDtotalSize+" rows!")
  
  if(RDDtotalSize==0)
    return
  
 /*
  * 
  * Top Talkers
  * 
  */
    
  /*
   *
   * Atypical Amount of data does basically the same but with less FP.
   * 
   *  
  val whiteTopTalkers = HogHBaseReputation.getReputationList("TTalker","whitelist")
  
  println("")
  println("Top Talkers (bytes):")
  
  val topTalkerCollection: PairRDDFunctions[String, (Long,Long,Long,HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)],Long)] = 
  sflowSummary
     .filter({case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) =>
                  !isMyIP(alienIP, myNets)
             })
     .map({
                  case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) =>
                       val flowSet:HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)] = new HashSet()
                       flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status))
                       (myIP,(bytesUp,bytesDown,numberPkts,flowSet,sampleRate,status))
                    })
  
  
  topTalkerCollection
  .reduceByKey({
                case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,sampleRateB)) =>
                     (bytesUpA+bytesUpB, bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, (sampleRateA+sampleRateB)/2)
              })
  .filter({
    case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,sampleRate)) =>
    bytesUp*sampleRate > topTalkersThreshold
         })
  .filter(tp => {  !whiteTopTalkers.map { net => if( tp._1.startsWith(net) )
                                            { true } else {false} 
                                        }.contains(true) 
          })
  .foreach{   case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,sampleRate)) => 
                    println("("+myIP+","+bytesUp+")" ) 
                    val flowMap: Map[String,String] = new HashMap[String,String]
                    flowMap.put("flow:id",System.currentTimeMillis.toString)
                    val event = new HogEvent(new HogFlow(flowMap,formatIPtoBytes(myIP),
                                                                 InetAddress.getByName("255.255.255.255").getAddress))
                    event.data.put("hostname", myIP)
                    event.data.put("bytesUp", (bytesUp*sampleRate).toString)
                    event.data.put("bytesDown", (bytesDown*sampleRate).toString)
                    event.data.put("numberPkts", numberPkts.toString)
                    event.data.put("threshold", humanBytes(topTalkersThreshold))
                    event.data.put("stringFlows", setFlows2String(flowSet))
                    
                    populateTopTalker(event).alert()
           }
      
   */
    
 /*
  * 
  * SMTP Talkers
  * 
  */
  val whiteSMTPTalkers =  HogHBaseReputation.getReputationList("MX","whitelist")

  println("")
  println("SMTP Talkers:")
  println("(SRC IP, DST IP, Bytes, Qtd Flows)")
  
  
   val SMTPTalkersCollection: PairRDDFunctions[String, (Long,Long,Long,HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)],Long,Long)] = 
    sflowSummary
    .filter({case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) 
                  =>  alienPort.equals("25") &  
                      numberPkts>3 &
                      !isMyIP(alienIP,myNets) // Exclude internal communication
           })
    .map({
           case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) =>
                val flowSet:HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)] = new HashSet()
                flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status))
                (myIP,(bytesUp,bytesDown,numberPkts,flowSet,1L,sampleRate))
        })
  
  
  SMTPTalkersCollection
  .reduceByKey({
          case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,connectionsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,connectionsB,sampleRateB)) =>
               (bytesUpA+bytesUpB,bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, connectionsA+connectionsB,(sampleRateA+sampleRateB)/2)
              })
  .sortBy({ 
              case   (myIP,(bytesUp,bytesDown,numberPkts,flowSet,connections,sampleRate)) =>    bytesUp  
          }, false, 15
         )
  .filter({ case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,connections,sampleRate)) => 
                   {  
                     /*!whiteSMTPTalkers.map { net => if( myIP.startsWith(net) )
                                                      { true } else{false} 
                                           }.contains(true) &
                     */
                     
                     connections > 1 & // Consider just MyIPs that generated more than 2 SMTP connections
                     (bytesUp+bytesDown)*sampleRate > SMTPTalkersThreshold &
                     numberPkts > 20 &
                     { val savedLastHogHistogram=HogHBaseHistogram.getHistogram("HIST01-"+myIP)
                       !Histograms.isTypicalEvent(savedLastHogHistogram.histMap, "25")// Exclude SMTP servers
                     } &
                     { val savedLastHogHistogram2=HogHBaseHistogram.getHistogram("HIST02-"+myIP)
                       !Histograms.isTypicalEvent(savedLastHogHistogram2.histMap, "25")// Exclude if send before
                     }
                   }
          })
  .take(100)
  .foreach{ case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,connections,sampleRate)) => 
                    println("("+myIP+","+bytesUp+")" ) 
                    val flowMap: Map[String,String] = new HashMap[String,String]
                    flowMap.put("flow:id",System.currentTimeMillis.toString)
                    val event = new HogEvent(new HogFlow(flowMap,myIP,"255.255.255.255"))
                    
                    event.data.put("hostname", myIP)
                    event.data.put("bytesUp", (bytesUp*sampleRate).toString)
                    event.data.put("bytesDown", (bytesDown*sampleRate).toString)
                    event.data.put("numberPkts", numberPkts.toString)
                    event.data.put("connections", connections.toString)
                    event.data.put("stringFlows", setFlows2String(flowSet))
                    
                    populateSMTPTalker(event).alert()
           }
  

 /*
  * FTP, etc.. Talkers
  *   
  */
     
  println("")
  println("FTP Talker")
  
  val ftpTalkersCollection:PairRDDFunctions[(String,String), 
                                            (Long,Long,Long,HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)],Long,Long)] = 
  sflowSummary
  .filter({case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) 
                  =>  proto.equals("TCP") &
                      ( myPort.equals("21") |
                        alienPort.equals("21") ) 
         })
  .map({
      case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) =>
         val flowSet:HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)] = new HashSet()
         flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status))
        ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,1L,sampleRate))
      })
  
  val ftpTalkers =
  ftpTalkersCollection
  .reduceByKey({
        case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,numberOfflowsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,numberOfflowsB,sampleRateB)) =>
             (bytesUpA+bytesUpB,bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB,(sampleRateA+sampleRateB)/2)
              })
  .map({
         case ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,sampleRate)) =>
              println("FTP Communication "+myIP+ " <?> "+alienIP)
              (myIP,alienIP)
      }).toArray().toSet
  
      
      
  println("")
  println("FTP Servers")
  
  val ftpServers = HogHBaseHistogram.getIPListHIST01(spark,"21")
  
      
      
 /*
  * P2P Communication
  *   
  */
  
  println("")
  println("P2P Communication")
  
  val p2pTalkersCollection:PairRDDFunctions[(String,String), 
                                            (Long,Long,Long,HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)],Long,Long)] = 
  sflowSummary
  .filter({case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) 
                  =>  myPort.toInt > 10000 &
                      alienPort.toInt > 10000 &
                      numberPkts > 1
         })
  .map({
      case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) =>
         val flowSet:HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)] = new HashSet()
         flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status))
        ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,1L,sampleRate))
      })
  
  val p2pTalkers1st =
  p2pTalkersCollection
  .reduceByKey({
        case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,numberOfflowsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,numberOfflowsB,sampleRateB)) =>
             (bytesUpA+bytesUpB,bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB,(sampleRateA+sampleRateB)/2)
              })
  .filter({
         case ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,sampleRate)) =>
               !isMyIP(alienIP,myNets) & // Avoid internal communication
               !ftpTalkers.contains((myIP,alienIP)) // Avoid FTP communication
          })
  .map({
         case ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,sampleRate)) =>
              (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,1L,sampleRate))
      })
  .reduceByKey({
          case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,numberOfflowsA,pairsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,numberOfflowsB,pairsB,sampleRateB)) =>
               (bytesUpA+bytesUpB,bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB, pairsA+pairsB,(sampleRateA+sampleRateB)/2)
              })
  .filter({ case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,pairs,sampleRate)) =>
                 pairs > p2pPairsThreshold &
                 flowSet
                  .map({case (myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status) => myPort})
                  .toList.distinct.size > p2pMyPortsThreshold &
                !ftpServers.contains(myIP)
         })
  .map({ 
      case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numberOfPairs,sampleRate)) =>
         //p2pTalkers.add(myIP)
         
         println("MyIP: "+myIP+ " - P2P Communication, number of pairs: "+numberOfPairs)
                            
         val flowMap: Map[String,String] = new HashMap[String,String]
         flowMap.put("flow:id",System.currentTimeMillis.toString)
         val event = new HogEvent(new HogFlow(flowMap,myIP,"255.255.255.255"))
         event.data.put("numberOfPairs",numberOfPairs.toString)
         event.data.put("myIP", myIP)
         event.data.put("bytesUp", (bytesUp*sampleRate).toString)
         event.data.put("bytesDown", (bytesDown*sampleRate).toString)
         event.data.put("numberPkts", numberPkts.toString)
         event.data.put("stringFlows", setFlows2String(flowSet))
                           
         populateP2PCommunication(event).alert()
         
         myIP
     }).toArray().toSet
  
    
    
    
  println("P2P Communication - 2nd method")
  val p2pTalkers2ndMethodCollection:PairRDDFunctions[(String,String), (Long,Long,Long,HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)],Long,Long)] = 
    sflowSummary
    .filter({case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) 
                  =>  proto.equals("UDP")     &
                      myPort.toInt < 10000    &
                      myPort.toInt > 1000     &
                      alienPort.toInt < 10000 &
                      alienPort.toInt > 1000  &
                      numberPkts > 1
           })
    .map({
      case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) =>
         val flowSet:HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)] = new HashSet()
         flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status))
        ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,1L,sampleRate))
        })
  
  val p2pTalkers2nd = p2pTalkers2ndMethodCollection
  .reduceByKey({
         case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,numberOfflowsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,numberOfflowsB,sampleRateB)) =>
              (bytesUpA+bytesUpB,bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB,(sampleRateA+sampleRateB)/2)
              })
  .filter({
         case ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,sampleRate)) =>
               !isMyIP(alienIP,myNets) &
               !p2pTalkers1st.contains(myIP)
          })
  .map({
         case ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,sampleRate)) =>
              (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,1L,sampleRate))
      })
  .reduceByKey({
         case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,numberOfflowsA,pairsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,numberOfflowsB,pairsB,sampleRateB)) =>
              (bytesUpA+bytesUpB,bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB, pairsA+pairsB,(sampleRateA+sampleRateB)/2)
              })
  .filter({ case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,pairs,sampleRate)) =>
                 pairs > p2pPairs2ndMethodThreshold &
                 flowSet.map(_._4).toList.distinct.size > p2pDistinctPorts2ndMethodThreshold &
                 bytesDown+bytesUp > p2pBytes2ndMethodThreshold &
                !ftpServers.contains(myIP)
          })
    .map({
      case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numberOfPairs,sampleRate)) =>
      
         println("MyIP: "+myIP+ " - P2P Communication 2nd method, number of pairs: "+numberOfPairs)
                            
         val flowMap: Map[String,String] = new HashMap[String,String]
         flowMap.put("flow:id",System.currentTimeMillis.toString)
         val event = new HogEvent(new HogFlow(flowMap,myIP,"255.255.255.255"))
         event.data.put("numberOfPairs",numberOfPairs.toString)
         event.data.put("myIP", myIP)
         event.data.put("bytesUp", (bytesUp*sampleRate).toString)
         event.data.put("bytesDown", (bytesDown*sampleRate).toString)
         event.data.put("numberPkts", numberPkts.toString)
         event.data.put("stringFlows", setFlows2String(flowSet))
                           
         populateP2PCommunication(event).alert()
         
         myIP
    }).toArray.toSet
    
    val p2pTalkers = p2pTalkers1st ++ p2pTalkers2nd


  println("Media streaming clients")
  val mediaClientCollection:PairRDDFunctions[(String,String), (Long,Long,Long,HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)],
                                            Long,Long,Long,Long)] = 
    sflowSummary
    .filter({case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) 
                  =>  proto.equals("TCP")      &
                      myPort.toInt > 1000      &
                      alienPort.toInt < 10000  &
                      alienPort.toInt > 1000   &
                      numberPkts > 1           &
                      !myPort.equals("1194")   &
                      !alienPort.equals("1194") 
           })
    .map({
      case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) =>
           val flowSet:HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)] = new HashSet()
           flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status))
           ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,1L,beginTime,endTime,sampleRate))
        })
  
  val mediaStreamingClients = 
  mediaClientCollection
  .reduceByKey({
      case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,numberOfflowsA,beginTimeA,endTimeA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,numberOfflowsB,beginTimeB,endTimeB,sampleRateB)) =>
           (bytesUpA+bytesUpB,bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB,beginTimeA.min(beginTimeB),endTimeA.max(endTimeB),(sampleRateA+sampleRateB)/2)
              })
  .filter({
           case ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,beginTime,endTime,sampleRate)) =>
                !isMyIP(alienIP,myNets)  &
                !p2pTalkers.contains(myIP) &
                (endTime-beginTime) > mediaClientCommunicationDurationThreshold &
                (endTime-beginTime) < mediaClientCommunicationDurationMAXThreshold 
          })
  .map({
          case ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,beginTime,endTime,sampleRate)) =>
               (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,1L,sampleRate))
      })
  .reduceByKey({
          case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,numberOfflowsA,pairsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,numberOfflowsB,pairsB,sampleRateB)) =>
               (bytesUpA+bytesUpB,bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB, pairsA+pairsB,(sampleRateA+sampleRateB)/2)
              })
  .filter({ case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,pairs,sampleRate)) =>
                 pairs     < mediaClientPairsThreshold  &
                 bytesUp*sampleRate   < mediaClientUploadThreshold &
                 bytesDown*sampleRate >= mediaClientDownloadThreshold
          })
    .map({ 
      case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numberOfPairs,sampleRate)) =>
      
         println("MyIP: "+myIP+ " - Media streaming client, number of pairs: "+numberOfPairs)
                            
         val flowMap: Map[String,String] = new HashMap[String,String]
         flowMap.put("flow:id",System.currentTimeMillis.toString)
         val event = new HogEvent(new HogFlow(flowMap,myIP,"255.255.255.255"))
         event.data.put("numberOfPairs",numberOfPairs.toString)
         event.data.put("myIP", myIP)
         event.data.put("bytesUp",   (bytesUp*sampleRate).toString)
         event.data.put("bytesDown", (bytesDown*sampleRate).toString)
         event.data.put("numberPkts", numberPkts.toString)
         event.data.put("connections", flowSet.size.toString)
         event.data.put("stringFlows", setFlows2String(flowSet)) 
                           
         populateMediaClient(event).alert()
         
         myIP
    }).toArray.toSet

    
  
 
 /*
  * 
  * Port Histogram - Atypical TCP port used
  * 
  * 
  */
  

  println("")
  println("Atypical TCP port used")
  
 val proxyServers = HogHBaseReputation.getReputationList("ProxyServer", "whitelist")

          
 val atypicalTCPCollection: PairRDDFunctions[String, (Long,Long,Long,HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)],
                                             Map[String,Double],Long,Long)] = 
    sflowSummary
    .filter({case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) 
                  =>  //direction  < 0 & // Algorithm implemented below to dig this information in another interesting form
                      !ftpTalkers.contains((myIP,alienIP)) &
                      //( numberPkts > 1  ) & XXX: Test 
                      //bytesUp > 0 &
                      //bytesDown > 0 &
                      status > 0 // PSH-ACK or SYN-ACK flags or ACK from MyHost 
           })
    .map({
      case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) =>
         val flowSet:HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)] = new HashSet()
         flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status))
         
         val histogram: Map[String,Double] = new HashMap()
         histogram.put(myPort,1D)
         
        (myIP,(bytesUp,bytesDown,numberPkts,flowSet,histogram,1L,sampleRate))
        
        })
  
  
     atypicalTCPCollection
     .reduceByKey({
       case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,histogramA,numberOfFlowsA,sampleRateA),
             (bytesUpB,bytesDownB,numberPktsB,flowSetB,histogramB,numberOfFlowsB,sampleRateB)) =>
      
               histogramB./:(0){case  (c,(key,qtdH))=> val qtdH2 = {if(histogramA.get(key).isEmpty) 0D else histogramA.get(key).get }
                                                        histogramA.put(key,  qtdH2 + qtdH) 
                                                        0
                                 }
               (bytesUpA+bytesUpB, bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, histogramA, numberOfFlowsA+numberOfFlowsB,(sampleRateA+sampleRateB)/2)
            })
     .map({ case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,histogram,numberOfFlows,sampleRate)) =>
    
                            (myIP,(bytesUp,bytesDown,numberPkts,flowSet,histogram.map({ case (port,qtdC) => (port,qtdC/numberOfFlows.toDouble) }),numberOfFlows,sampleRate))
          })
    .filter({case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,histogram,numberOfFlows,sampleRate)) =>
                   !p2pTalkers.contains(myIP)  // Avoid P2P talkers
           })
    .foreach{case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,histogram1,numberOfFlows,sampleRate)) => 
      
                 // Remove ports used to connect as client, and not to serve
                    val newHistogram = 
                    histogram1.filter({
                        case (port,weight) =>
                          
                          if(proxyServers.contains(myIP)) // Proxies have a different approach
                          {
                            if( flowSet
                                 .filter({case (myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status) =>
                                             myPort.equals(port) &
                                             direction < 0
                                        }).size >0 )
                            {
                              true
                            }else
                            {
                              false
                            }
                          }else
                          {
                              val alienPorts =
                              flowSet
                                .filter({
                                        case (myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status) =>
                                             myPort.equals(port)
                                       })
                                .map({
                                      case (myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status) =>
                                           (alienPort,1L)
                                    })
                                .groupBy(_._1)
                                .map({
                                      case (group,traversable) =>
                                           traversable.reduce({(a,b) => (a._1,a._2+b._2)})
                                   })
                                .map({
                                      case (alienPort,qtd) =>
                                           alienPort
                                   })
                                
                               val qtdAlienPorts = alienPorts.size
    
                                
                               val numberOfFlowsUsingThisMyPort =
                                 flowSet.filter({case (myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status) =>
                                   myPort.equals(port)               
                                 }).size
                              
                               if( ( qtdAlienPorts >3 ) &
                                   ( (qtdAlienPorts.toDouble/numberOfFlowsUsingThisMyPort.toDouble) > 0.2  )  
                                 )
                                 
                                 if(  alienPorts.filter({ p => p.toLong < 1024 }).size >2 &
                                    ((qtdAlienPorts.toDouble/alienPorts.filter({ p => p.toLong < 1024 }).size.toDouble) > 0.5  )
                                   ) // Proxy detected
                                 {
                                   // Save Proxy
                                   HogHBaseReputation.saveReputationList("ProxyServer", "whitelist", myIP)
                                   false
                                 }else
                                 {
                                   true
                                 }
                               else
                                 false
                          }
                    })  
                    
                    
                    val hogHistogram=HogHBaseHistogram.getHistogram("HIST01-"+myIP)
                    
                    if(hogHistogram.histSize < 100)
                    {
                      //println("IP: "+dstIP+ "  (N:"+qtd+",S:"+hogHistogram.histSize+") - Learn More!")
                      HogHBaseHistogram.saveHistogram(Histograms.merge(hogHistogram, new HogHistogram("",numberOfFlows,newHistogram)))
                    }else
                    {
                          //val KBDistance = Histograms.KullbackLiebler(hogHistogram.histMap, map)
                          val atypical   = Histograms.atypical(hogHistogram.histMap, newHistogram)
                                                     .filter { port =>   !atypicalTCPPort.contains(port) & // Exclude highly common FP
                                                                       ( !Histograms.isTypicalEvent(hogHistogram.histMap,"21") |
                                                                         port.toInt < 1024
                                                                        ) // Avoid FTP servers
                                                              } 

                          if(atypical.size>0)
                          {
                            println("Source IP: "+myIP+ "  (N:"+numberOfFlows+",S:"+hogHistogram.histSize+") - Atypical (open) source ports: "+atypical.mkString(","))
                            
                            
                            val flowMap: Map[String,String] = new HashMap[String,String]
                            flowMap.put("flow:id",System.currentTimeMillis.toString)
                            val event = new HogEvent(new HogFlow(flowMap,myIP,"255.255.255.255"))
                            event.data.put("myIP", myIP)
                            event.data.put("tcpport", atypical.mkString(","))
                            event.data.put("bytesUp", (bytesUp*sampleRate).toString)
                            event.data.put("bytesDown", (bytesDown*sampleRate).toString)
                            event.data.put("numberPkts", numberPkts.toString)
                            event.data.put("stringFlows", setFlows2String(flowSet.filter({p => atypical.contains(p._2)})))
                    
                            populateAtypicalTCPPortUsed(event).alert()
                          }
                      HogHBaseHistogram.saveHistogram(Histograms.merge(hogHistogram, new HogHistogram("",numberOfFlows,newHistogram)))
                    }
             }
  
    
    
 /*
  * 
  * Port Histogram - Atypical alien TCP port used
  * 
  * 
  */
  

  println("")
  println("Atypical alien TCP port used")
  
 val atypicalAlienTCPCollection: PairRDDFunctions[String, (Long,Long,Long,HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)],
                                                  Map[String,Double],Long,Long)] = 
    sflowSummary
    .filter({case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) 
                  =>  //numberPkts > 1           & //XXX: Test
                      alienPort.toLong < 10000 &
                      direction > -1           &
                      myPort.toLong > 1024     &
                      !myPort.equals("8080")   &
                      !isMyIP(alienIP,myNets)  &
                      !ftpTalkers.contains((myIP,alienIP)) &// Avoid FTP communication
                      proto.equals("TCP") &
                      status > 0
           })         
    .map({
      case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) =>
         val flowSet:HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)] = new HashSet()
         flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status))
         
         val histogram: Map[String,Double] = new HashMap()
         histogram.put(alienPort,1D)
         
        (myIP,(bytesUp,bytesDown,numberPkts,flowSet,histogram,1L,sampleRate))
        
        })
  
  
     atypicalAlienTCPCollection
     .reduceByKey({
       case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,histogramA,numberOfFlowsA,sampleRateA),
             (bytesUpB,bytesDownB,numberPktsB,flowSetB,histogramB,numberOfFlowsB,sampleRateB)) =>
      
               histogramB./:(0){case  (c,(key,qtdH))=> val qtdH2 = {if(histogramA.get(key).isEmpty) 0D else histogramA.get(key).get }
                                                        histogramA.put(key,  qtdH2 + qtdH) 
                                                        0
                                 }
               (bytesUpA+bytesUpB, bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, histogramA, numberOfFlowsA+numberOfFlowsB,(sampleRateA+sampleRateB)/2)
            })
     .map({ case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,histogram,numberOfFlows,sampleRate)) =>
                 (myIP,(bytesUp,bytesDown,numberPkts,flowSet,
                      histogram.map({ case (port,qtdC) => (port,qtdC/numberOfFlows.toDouble) }),
                      numberOfFlows,sampleRate))
          })
    .filter({case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,histogram,numberOfFlows,sampleRate)) =>
                   !p2pTalkers.contains(myIP) & // Avoid P2P talkers
                   !mediaStreamingClients.contains(myIP)  // Avoid media streaming clients
           })
    .foreach{case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,histogram1,numberOfFlows,sampleRate)) => 
      
                    // Remove alienPorts used to connect by Aliens as clients
                    val alienClientPortsList=
                      flowSet.map({
                        case (myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status) =>
                          (myPort,Set(alienPort),1L)
                      })
                      .groupBy(_._1)
                      .map({
                        case (group,traversable) =>
                         traversable.reduce({(a,b) => (a._1,a._2++b._2,a._3+b._3)})
                      })
                      .filter({case (myPort,alienPorts,qtd) => qtd>1})
                      .map(_._2)
                      
                    val alienClientPorts = 
                    {
                      if(alienClientPortsList.size>0)
                        alienClientPortsList.reduce({(a,b) => a++b})
                      else
                        Set("")
                    }
                      
      
                    val newHistogram = 
                    histogram1
                    .filter({
                        case (port,weight) =>
                          
                          if(alienClientPorts.contains(port))  
                                 false
                              else
                                 true
                    })  
                    
                    
                    val savedHogHistogram=HogHBaseHistogram.getHistogram("HIST02-"+myIP)
                    
                    if(savedHogHistogram.histSize < 1000)
                    {
                      //println("IP: "+dstIP+ "  (N:"+qtd+",S:"+hogHistogram.histSize+") - Learn More!")
                      HogHBaseHistogram.saveHistogram(Histograms.merge(savedHogHistogram, new HogHistogram("",numberOfFlows,newHistogram)))
                    }else
                    {
                         val savedLastHogHistogram=HogHBaseHistogram.getHistogram("HIST02.1-"+myIP)
                         
                         if(savedLastHogHistogram.histSize > 0)
                         {
                           
                           //val KBDistance = Histograms.KullbackLiebler(hogHistogram.histMap, map)
                           val atypical   = Histograms.atypical(savedHogHistogram.histMap, newHistogram)
                           val typical   = Histograms.typical(savedLastHogHistogram.histMap, newHistogram)

                            val newAtypical = 
                            atypical.filter({ atypicalAlienPort =>
                                               {
                                                  typical.contains(atypicalAlienPort)
                                                } &
                                                {
                                                  flowSet.filter(p => p._4.equals(atypicalAlienPort))
                                                  .map({ case (myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status) => 
                                                             var savedAlienHistogram = new HogHistogram("",0,new HashMap[String,Double])
                                                             if(isMyIP(alienIP,myNets))
                                                             {
                                                               savedAlienHistogram = HogHBaseHistogram.getHistogram("HIST01-"+alienIP)
                                                             }                                                             
                                                             else
                                                             {                                                              
                                                               savedAlienHistogram = HogHBaseHistogram.getHistogram("HIST05-"+ipSignificantNetwork(alienIP))
                                                             }
                                                             
                                                             val histogramAlien      = new HashMap[String,Double]
                                                             histogramAlien.put(alienPort, 1D)
                                                             val atypicalAlien   = Histograms.atypical(savedAlienHistogram.histMap, histogramAlien)
                                                             if(atypicalAlien.size>0)
                                                                 true // Indeed, an atypical access
                                                             else
                                                                 false // No! The Alien was accessed before by someone else. It's not an atypical flow.
                                                           }).contains(true)
                                                }
                                            })
                            
                            if(newAtypical.size>0)
                            {
                            println("MyIP: "+myIP+ "  (N:"+numberOfFlows+",S:"+savedHogHistogram.histSize+") - Atypical alien ports: "+newAtypical.mkString(","))
                            
                            /*
                            println("Saved:")
                            hogHistogram.histMap./:(0){case  (c,(key,qtd))=>
                            println(key+": "+ qtd)
                            0
                            } 
                            println("Now:")
                            map./:(0){case  (c,(key,qtd))=>
                            println(key+": "+ qtd)
                            0
                            } 
                            * 
                            */
                            
                            val flowMap: Map[String,String] = new HashMap[String,String]
                            flowMap.put("flow:id",System.currentTimeMillis.toString)
                            val event = new HogEvent(new HogFlow(flowMap,myIP,"255.255.255.255"))
                            event.data.put("myIP", myIP)
                            event.data.put("tcpport", newAtypical.mkString(","))
                            event.data.put("bytesUp", (bytesUp*sampleRate).toString)
                            event.data.put("bytesDown", (bytesDown*sampleRate).toString)
                            event.data.put("numberPkts", numberPkts.toString)
                            event.data.put("stringFlows", setFlows2String(flowSet.filter({p => newAtypical.contains(p._4)})))
                    
                            populateAtypicalAlienTCPPortUsed(event).alert()
                          }
                           
                          HogHBaseHistogram.saveHistogram(Histograms.merge(savedHogHistogram, savedLastHogHistogram)) 

                         }
                      
                          
                      HogHBaseHistogram.saveHistogram(new HogHistogram("HIST02.1-"+myIP,numberOfFlows,newHistogram))
                    }
                    
             }
 
    
      
 /*
  * 
  * Atypical number of pairs in the period
  * 
  * 
  */
    
    
  println("")
  println("Atypical number of pairs in the period")
 
  val atypicalNumberPairsCollection: PairRDDFunctions[(String,String), (Long,Long,Long,HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)],Long,Long)] = 
    sflowSummary
    .filter({case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) 
                  =>  numberPkts > 1
           })
    .map({
      case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) =>
         val flowSet:HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)] = new HashSet()
         flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status))
        ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,1L,sampleRate))
        })
  
  val atypicalNumberPairsCollectionFinal=
  atypicalNumberPairsCollection
  .reduceByKey({
    case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,numberOfflowsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,numberOfflowsB,sampleRateB)) =>
      (bytesUpA+bytesUpB,bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB, (sampleRateA+sampleRateB)/2)
  })
  .map({
     case ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,sampleRate)) =>
    
       (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,1L,sampleRate))
  })
  .reduceByKey({
    case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,numberOfflowsA,pairsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,numberOfflowsB,pairsB,sampleRateB)) =>
      (bytesUpA+bytesUpB,bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB, pairsA+pairsB,(sampleRateA+sampleRateB)/2)
  })
  .filter{case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numberOfPairs,sampleRate)) =>
                   !p2pTalkers.contains(myIP)// Avoid P2P talkers
           }.cache
  
  val pairsStats = 
  atypicalNumberPairsCollectionFinal
  .map({case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numberOfPairs,sampleRate)) =>
    numberOfPairs
      }).stats()
  
  
  atypicalNumberPairsCollectionFinal
  .filter{case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numberOfPairs,sampleRate)) =>
                   numberOfPairs > atypicalPairsThresholdMIN
           }
  .foreach{case  (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numberOfPairs,sampleRate)) => 
                    val savedHistogram=HogHBaseHistogram.getHistogram("HIST03-"+myIP)
                    
                    val histogram = new HashMap[String,Double]
                    val key = floor(log(numberOfPairs.*(1000)+1D)).toString
                    histogram.put(key, 1D)
                    
                    if(savedHistogram.histSize< 10)
                    {
                      //println("MyIP: "+myIP+ "  (N:1,S:"+hogHistogram.histSize+") - Learn More!")
                      HogHBaseHistogram.saveHistogram(Histograms.merge(savedHistogram, new HogHistogram("",1L,histogram)))
                    }else
                    {
                          //val KBDistance = Histograms.KullbackLiebler(hogHistogram.histMap, map)
                          val atypical   = Histograms.atypical(savedHistogram.histMap, histogram)

                          if(atypical.size>0 & savedHistogram.histMap.filter({case (key,value) => value > 0.001D}).size <5)
                          {
                            println("MyIP: "+myIP+ "  (N:1,S:"+savedHistogram.histSize+") - Atypical number of pairs in the period: "+numberOfPairs)
                            
                            val flowMap: Map[String,String] = new HashMap[String,String]
                            flowMap.put("flow:id",System.currentTimeMillis.toString)
                            val event = new HogEvent(new HogFlow(flowMap,myIP,"255.255.255.255"))
                            event.data.put("numberOfPairs",numberOfPairs.toString)
                            event.data.put("myIP", myIP)
                            event.data.put("bytesUp",   (bytesUp*sampleRate).toString)
                            event.data.put("bytesDown", (bytesDown*sampleRate).toString)
                            event.data.put("numberPkts", numberPkts.toString)
                            event.data.put("stringFlows", setFlows2String(flowSet))
                            event.data.put("pairsMean", pairsStats.mean.round.toString)
                            event.data.put("pairsStdev", pairsStats.stdev.round.toString)
                            
                            populateAtypicalNumberOfPairs(event).alert()
                          }
                          
                          HogHBaseHistogram.saveHistogram(Histograms.merge(savedHistogram, new HogHistogram("",1L,histogram)))
                    }
             }
  
    
       
 /*
  * 
  * Atypical amount of data transfered
  * 
  */
   
  println("")
  println("Atypical amount of data transfered")
  
  val bigProviderNets = HogHBaseReputation.getReputationList("BigProvider", "whitelist")
  
  val atypicalAmountDataCollection: PairRDDFunctions[(String,String), (Long,Long,Long,HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)]
                                                      ,Long,Long)] = 
    sflowSummary
    .filter({case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) 
                  =>  //alienPort.toLong < 1204  &&
                      direction > -1       &
                      myPort.toLong > 1024 &
                      !myPort.equals("8080") &
                      !isMyIP(alienIP,myNets) & // Exclude internal communication
                      !isMyIP(alienIP,bigProviderNets) // Exclude bigProviders
           })
    .map({
          case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) =>
               val flowSet:HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)] = new HashSet()
               flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status))
               ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,1L,sampleRate))
        })
  
  val atypicalAmountDataCollectionFinal =
  atypicalAmountDataCollection
  .reduceByKey({
                case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,numberOfflowsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,numberOfflowsB,sampleRateB)) =>
                     (bytesUpA+bytesUpB,bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB,(sampleRateA+sampleRateB)/2)
              })
  .map({
     case ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,sampleRate)) =>
          (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,1L,sampleRate))
  })
  .reduceByKey({
    case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,numberOfflowsA,pairsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,numberOfflowsB,pairsB,sampleRateB)) =>
         (bytesUpA+bytesUpB,bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB, pairsA+pairsB,(sampleRateA+sampleRateB)/2)
  })
  .filter{case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numberOfPairs,sampleRate)) =>
                   !p2pTalkers.contains(myIP) & // Avoid P2P talkers
                   !mediaStreamingClients.contains(myIP)  // Avoid media streaming clients
         }.cache
  
  val dataStats = 
  atypicalAmountDataCollectionFinal
  .map({case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numberOfPairs,sampleRate)) =>
        bytesUp
      }).stats()
  
  
  atypicalAmountDataCollectionFinal
  .filter{case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numberOfPairs,sampleRate)) =>
               bytesUp*sampleRate > atypicalAmountDataThresholdMIN
         }
  .foreach{case  (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numberOfPairs,sampleRate)) => 
                    
                    val savedHistogram=HogHBaseHistogram.getHistogram("HIST04-"+myIP)
                    
                    val histogram = new HashMap[String,Double]
                    val key = floor(log(bytesUp.*(0.0001)+1D)).toString
                    histogram.put(key, 1D)
                    
                    if(savedHistogram.histSize< 30 )
                    {
                      //println("MyIP: "+myIP+ "  (N:1,S:"+hogHistogram.histSize+") - Learn More!")
                      HogHBaseHistogram.saveHistogram(Histograms.merge(savedHistogram, new HogHistogram("",1L,histogram)))
                    }else
                    {
                      

                          //val KBDistance = Histograms.KullbackLiebler(hogHistogram.histMap, map)
                          val atypical   = Histograms.atypical(savedHistogram.histMap, histogram)

                          if(atypical.size>0 & savedHistogram.histMap.filter({case (key,value) => value > 0.001D}).size <5)
                          {
                            println("MyIP: "+myIP+ "  (N:1,S:"+savedHistogram.histSize+") - Atypical amount of sent bytes: "+bytesUp)
                            
                            val flowMap: Map[String,String] = new HashMap[String,String]
                            flowMap.put("flow:id",System.currentTimeMillis.toString)
                            val event = new HogEvent(new HogFlow(flowMap,myIP,"255.255.255.255"))
                            event.data.put("numberOfPairs",numberOfPairs.toString)
                            event.data.put("myIP", myIP)
                            event.data.put("bytesUp",   (bytesUp*sampleRate).toString)
                            event.data.put("bytesDown", (bytesDown*sampleRate).toString)
                            event.data.put("numberPkts", numberPkts.toString)
                            event.data.put("stringFlows", setFlows2String(flowSet))
                            event.data.put("dataMean", dataStats.mean.round.toString)
                            event.data.put("dataStdev", dataStats.stdev.round.toString)
                            
                            populateAtypicalAmountData(event).alert()
                          }
                          
                          HogHBaseHistogram.saveHistogram(Histograms.merge(savedHistogram, new HogHistogram("",1L,histogram)))
                    }
             }
  
  
 
   
 /*
  * 
  * Port Histogram - Atypical TCP port used
  * 
  * 
  */

  println("")
  println("Atypical TCP port used by Alien Network")
          
 val atypicalAlienReverseTCPCollection: PairRDDFunctions[String, (Long,Long,Long,HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)],
                                                        Map[String,Double],Long,Long)] = 
    sflowSummary
    .filter({case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) 
                  => // numberPkts  >1   & // XXX: Test
                      myPort.toLong >1024 &
                      alienPort.toLong <10000 &
                      proto.equals("TCP") &
                      !isMyIP(alienIP,myNets) &  // Flow InternalIP <-> ExternalIP
                      !p2pTalkers.contains(myIP) & // Avoid P2P communication
                      !ftpTalkers.contains((myIP,alienIP)) 
           })
    .map({
      case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) =>
         val flowSet:HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)] = new HashSet()
         flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status))
         
         val histogram: Map[String,Double] = new HashMap()
         histogram.put(alienPort,1D)
         
        (ipSignificantNetwork(alienIP),(bytesUp,bytesDown,numberPkts,flowSet,histogram,1L,sampleRate))
        
        })
  
  
     atypicalAlienReverseTCPCollection
     .reduceByKey({
       case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,histogramA,numberOfFlowsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,histogramB,numberOfFlowsB,sampleRateB)) =>
      
               histogramB./:(0){case  (c,(key,qtdH))=> val qtdH2 = {if(histogramA.get(key).isEmpty) 0D else histogramA.get(key).get }
                                                        histogramA.put(key,  qtdH2 + qtdH) 
                                                        0
                                 }
               (bytesUpA+bytesUpB, bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, histogramA, numberOfFlowsA+numberOfFlowsB,(sampleRateA+sampleRateB)/2)
            })
     .map({ case (alienNetwork,(bytesUp,bytesDown,numberPkts,flowSet,histogram,numberOfFlows,sampleRate)) =>
                 (alienNetwork,(bytesUp,bytesDown,numberPkts,flowSet,
                     histogram.map({ case (port,qtdC) => (port,qtdC/numberOfFlows.toDouble) }),
                     numberOfFlows,sampleRate))
          })
    .foreach{case (alienNetwork,(bytesUp,bytesDown,numberPkts,flowSet,histogram,numberOfFlows,sampleRate)) => 
                    
                    //  Ports
                    val savedHogHistogram=HogHBaseHistogram.getHistogram("HIST05-"+alienNetwork)
                    
                    // Bytes
                    if({
                          flowSet
                          .map({case (myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status) => myIP})
                          .toList.distinct.size>4
                    }) // Consider just if there are more than 4 distinct MyHosts as pairs 
                    {
                    
                    val histogramBytes = new HashMap[String,Double]
                    flowSet
                       .filter({case (myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status) =>
                                myPort.toInt > 1023
                            })
                       .map({case (myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status) => 
                                     (floor(log((bytesUp*sampleRate).*(0.0001)+1D)).toString,1D)
                            })
                       .toMap
                       .groupBy(_._1)
                       .map({
                        case (group,traversable) =>
                              traversable.reduce({(a,b) => (a._1,a._2+b._2)})
                           })
                       .map({case (key,value) => histogramBytes.put(key,value)})
                      
                      val savedHogHistogramBytes=HogHBaseHistogram.getHistogram("HIST06-"+alienNetwork)
                      HogHBaseHistogram.saveHistogram(Histograms.merge(savedHogHistogramBytes, new HogHistogram("",numberOfFlows,histogramBytes)))
                      
                      val maxBytesUp=
                        flowSet
                       .map({case (myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status) => 
                                     bytesUp*sampleRate
                            }).max
                       if(maxBytesUp>bigProviderThreshold)
                       {
                          HogHBaseReputation.saveReputationList("BigProvider", "whitelist", alienNetwork)
                       }
                           
                    }
                    // Bytes End
                    
                    
                    
                    if(savedHogHistogram.histSize < 1000)
                    {
                      //println("IP: "+dstIP+ "  (N:"+qtd+",S:"+hogHistogram.histSize+") - Learn More!")
                      HogHBaseHistogram.saveHistogram(Histograms.merge(savedHogHistogram, new HogHistogram("",numberOfFlows,histogram)))
                    }else
                    {
                          //val KBDistance = Histograms.KullbackLiebler(hogHistogram.histMap, map)
                          val atypical   = Histograms.atypical(savedHogHistogram.histMap, histogram)

                          if(atypical.size>0)
                          {
                            println("Alien Network: "+alienNetwork+ "  (N:"+numberOfFlows+",S:"+savedHogHistogram.histSize+") - Atypical alien network port used: "+atypical.mkString(","))
                            
                            /*
                            val flowMap: Map[String,String] = new HashMap[String,String]
                            flowMap.put("flow:id",System.currentTimeMillis.toString)
                            val event = new HogEvent(new HogFlow(flowMap,formatIPtoBytes(alienNetwork+".0"),
                                                                         InetAddress.getByName("255.255.255.255").getAddress))
                            event.data.put("myIP", myIP)
                            event.data.put("tcpport", atypical.mkString(","))
                            event.data.put("bytesUp", bytesUp.toString)
                            event.data.put("bytesDown", bytesDown.toString)
                            event.data.put("numberPkts", numberPkts.toString)
                            event.data.put("stringFlows", setFlows2String(flowSet.filter({p => atypical.contains(p._2)})))
                    
                            populateAtypicalTCPPortUsed(event).alert()
                            */
                          }
                      HogHBaseHistogram.saveHistogram(Histograms.merge(savedHogHistogram, new HogHistogram("",numberOfFlows,histogram)))
                    }
             }
  
  
       
 /*
  * 
  * Alien accessing too much hosts
  * 
  */
  
    
  println("")
  println("Aliens accessing more than "+alienThreshold+" hosts")
 
  
   val alienTooManyPairsCollection: PairRDDFunctions[(String,String), (Long,Long,Long,HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)],
                                                     Long,Long)] = 
    sflowSummary
    .filter({ case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) 
                 => direction < 0  &
                    !isMyIP(alienIP,myNets)
            })
    .map({
      case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) =>
         val flowSet:HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)] = new HashSet()
         flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status))
        ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,1L,sampleRate))
        })
  
  alienTooManyPairsCollection
  .reduceByKey({
       case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,numberOfflowsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,numberOfflowsB,sampleRateB)) =>
            (bytesUpA+bytesUpB, bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB, (sampleRateA+sampleRateB)/2)
              })
  .map({
       case ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,sampleRate)) =>
            (alienIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,1L,sampleRate))
      })
  .reduceByKey({
       case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,numberOfflowsA,pairsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,numberOfflowsB,pairsB,sampleRateB)) =>
            (bytesUpA+bytesUpB, bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB, pairsA+pairsB, (sampleRateA+sampleRateB)/2)
  })
  .foreach{case  (alienIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numberOfPairs,sampleRate)) => 
                    if(numberOfPairs > alienThreshold && !alienIP.equals("0.0.0.0") )
                    {                     
                            println("AlienIP: "+alienIP+ " more than "+alienThreshold+" pairs in the period: "+numberOfPairs)
                         
                            val flowMap: Map[String,String] = new HashMap[String,String]
                            flowMap.put("flow:id",System.currentTimeMillis.toString)
                            val event = new HogEvent(new HogFlow(flowMap,alienIP,"255.255.255.255"))
                            
                            event.data.put("numberOfPairs",numberOfPairs.toString)
                            event.data.put("alienIP", alienIP)
                            event.data.put("bytesUp",   (bytesUp*sampleRate).toString)
                            event.data.put("bytesDown", (bytesDown*sampleRate).toString)
                            event.data.put("numberPkts", numberPkts.toString)
                            event.data.put("stringFlows", setFlows2String(flowSet))
                            
                            populateAlienAccessingManyHosts(event).alert()
                    }
           }
  
  /*
   * 
   * UDP amplifier (DDoS)
   * 
   */
  println("")
  println("UDP amplifier (DDoS)")
  
   val udpAmplifierCollection: PairRDDFunctions[String, 
                                                (Long,Long,Long,HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)],
                                                Long,Long)] = sflowSummary
    .filter({case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) 
                  => (  myPort.equals("19")   |
                        myPort.equals("53")   |
                        myPort.equals("123")  |
                        myPort.equals("1900")
                      ) &
                      proto.equals("UDP")      &
                      bytesUp/numberPkts >250   &
                      !isMyIP(alienIP,myNets)                      
           })
    .map({
          case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) =>
               val flowSet:HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)] = new HashSet()
               flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status))
               (myIP,(bytesUp,bytesDown,numberPkts,flowSet,1L,sampleRate))
        })
  
  
  udpAmplifierCollection
    .reduceByKey({
                   case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,connectionsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,connectionsB,sampleRateB)) =>
                        (bytesUpA+bytesUpB,bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, connectionsA+connectionsB,(sampleRateA+sampleRateB)/2)
                })
    .filter({ case  (myIP,(bytesUp,bytesDown,numberPkts,flowSet,connections,sampleRate)) => 
                    numberPkts>400
           })
    .sortBy({ 
              case   (myIP,(bytesUp,bytesDown,numberPkts,flowSet,connections,sampleRate)) =>    bytesUp  
            }, false, 15
           )
  .foreach{ case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,connections,sampleRate)) => 
                    println("("+myIP+","+bytesUp+")" ) 
                    val flowMap: Map[String,String] = new HashMap[String,String]
                    flowMap.put("flow:id",System.currentTimeMillis.toString)
                    val event = new HogEvent(new HogFlow(flowMap,myIP,"255.255.255.255"))
                    
                    event.data.put("hostname", myIP)
                    event.data.put("bytesUp", (bytesUp*sampleRate).toString)
                    event.data.put("bytesDown", (bytesDown*sampleRate).toString)
                    event.data.put("numberPkts", numberPkts.toString)
                    event.data.put("connections", connections.toString)
                    event.data.put("stringFlows", setFlows2String(flowSet))
                    
                    populateUDPAmplifier(event).alert()
           }
   
  
  /*
   * 
   *  Abused SMTP Server
   * 
   */
   
  println("")
  println("Abused SMTP Server")
   val abusedSMTPCollection: PairRDDFunctions[(String, String), (Long,Long,Long,HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)],Long,Long)] = sflowSummary
    .filter({case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) 
                  =>  
                      ( myPort.equals("465") |
                        myPort.equals("587")
                      ) &
                      proto.equals("TCP")  &
                      !isMyIP(alienIP,myNets) 
           })
    .map({
          case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) =>
               val flowSet:HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)] = new HashSet()
               flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status))
               ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,1L,sampleRate))
        })
  
  
  abusedSMTPCollection
    .reduceByKey({
                   case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,connectionsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,connectionsB,sampleRateB)) =>
                        (bytesUpA+bytesUpB,bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, connectionsA+connectionsB,(sampleRateA+sampleRateB)/2)
                })
    .filter({ case  ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,connections,sampleRate)) => 
                    connections>50 &
                    bytesDown*sampleRate > abusedSMTPBytesThreshold
           })
    .sortBy({ 
              case   ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,connections,sampleRate)) =>    bytesDown  
            }, false, 15
           )
  .take(100)
  .foreach{ case ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,connections,sampleRate)) => 
                    println("("+myIP+","+alienIP+","+bytesUp+")" ) 
                    val flowMap: Map[String,String] = new HashMap[String,String]
                    flowMap.put("flow:id",System.currentTimeMillis.toString)
                    val event = new HogEvent(new HogFlow(flowMap,myIP,alienIP))
                    
                    event.data.put("hostname", myIP)
                    event.data.put("bytesUp",   (bytesUp*sampleRate).toString)
                    event.data.put("bytesDown", (bytesDown*sampleRate).toString)
                    event.data.put("numberPkts", numberPkts.toString)
                    event.data.put("connections", connections.toString)
                    event.data.put("stringFlows", setFlows2String(flowSet))
                    
                    populateAbusedSMTP(event).alert()
           }
   
 
   /*
   * 
   *  DNS tunnels
   * 
   */
   
  println("")
  println("DNS tunnels")
  val dnsTunnelCollection: PairRDDFunctions[String, (Long,Long,Long,HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)],Long,Long)] =
  sflowSummary
  .filter({case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) 
                  =>  
                      alienPort.equals("53") &
                      proto.equals("UDP")  &
                      (bytesUp+bytesDown)*sampleRate > dnsTunnelThreshold &
                      !isMyIP(alienIP,myNets) 
           })
    .map({
          case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) =>
               val flowSet:HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)] = new HashSet()
               flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status))
               (myIP,(bytesUp,bytesDown,numberPkts,flowSet,1L,sampleRate))
        })
  
  
  dnsTunnelCollection
    .reduceByKey({
                   case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,connectionsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,connectionsB,sampleRateB)) =>
                        (bytesUpA+bytesUpB,bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, connectionsA+connectionsB,(sampleRateA+sampleRateB)/2)
                })
    .sortBy({ 
              case   (myIP,(bytesUp,bytesDown,numberPkts,flowSet,connections,sampleRate)) =>    bytesUp+bytesDown  
            }, false, 15
           )
  .take(30)
  .foreach{ case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,connections,sampleRate)) => 
                    println("("+myIP+","+bytesUp+")" ) 
                    val flowMap: Map[String,String] = new HashMap[String,String]
                    flowMap.put("flow:id",System.currentTimeMillis.toString)
                    val event = new HogEvent(new HogFlow(flowMap,myIP,"255.255.255.255"))
                    
                    event.data.put("hostname", myIP)
                    event.data.put("bytesUp",   (bytesUp*sampleRate).toString)
                    event.data.put("bytesDown", (bytesDown*sampleRate).toString)
                    event.data.put("numberPkts", numberPkts.toString)
                    event.data.put("connections", connections.toString)
                    event.data.put("stringFlows", setFlows2String(flowSet.filter({p => p._4.equals("53")}))) // 4: alienPort
                    
                    populateDNSTunnel(event).alert()
           }
  
  
  
  
  
   /*
   * 
   *  ICMP tunnels
   * 
   */
   
  println("")
  println("ICMP tunnels")
  val icmpTunnelCollection: PairRDDFunctions[String, (Long,Long,Long,HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)],Long,Long)] =
  sflowSummaryICMP
  .filter({case ((srcIP,icmpType,dstIP,icmpCode, proto ),(bytesUp,bytesDown,numberOfPkts,direction,beginTime,endTime,sampleRate)) 
                  =>  
                      (bytesUp+bytesDown)/numberOfPkts > icmpTunnelThreshold
           })
    .map({
          case ((srcIP,icmpType,dstIP,icmpCode, proto ),(bytesUp,bytesDown,numberOfPkts,direction,beginTime,endTime,sampleRate)) =>
               val flowSet:HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)] = new HashSet()
               flowSet.add((srcIP,icmpType,dstIP,icmpCode,proto,bytesUp,bytesDown,numberOfPkts,direction,beginTime,endTime,sampleRate,0))
               (srcIP,(bytesUp,bytesDown,numberOfPkts,flowSet,1L,sampleRate))
        })
  
  
  icmpTunnelCollection
    .reduceByKey({
                   case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,connectionsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,connectionsB,sampleRateB)) =>
                        (bytesUpA+bytesUpB,bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, connectionsA+connectionsB,(sampleRateA+sampleRateB)/2)
                })
    .filter({
      case   (srcIP,(bytesUp,bytesDown,numberPkts,flowSet,connections,sampleRate)) =>
         (bytesUp+bytesDown)*sampleRate > icmpTotalTunnelThreshold
    })
    .sortBy({ 
              case   (srcIP,(bytesUp,bytesDown,numberPkts,flowSet,connections,sampleRate)) =>    bytesUp+bytesDown  
            }, false, 15
           )
  .take(30)
  .foreach{ case (srcIP,(bytesUp,bytesDown,numberPkts,flowSet,connections,sampleRate)) => 
                    println("("+srcIP+","+bytesUp+")" ) 
                    val flowMap: Map[String,String] = new HashMap[String,String]
                    flowMap.put("flow:id",System.currentTimeMillis.toString)
                    val event = new HogEvent(new HogFlow(flowMap,srcIP,"255.255.255.255"))
                    
                    event.data.put("hostname", srcIP)
                    event.data.put("bytesUp",   (bytesUp*sampleRate).toString)
                    event.data.put("bytesDown", (bytesDown*sampleRate).toString)
                    event.data.put("numberPkts", numberPkts.toString)
                    event.data.put("connections", connections.toString)
                    event.data.put("stringFlows", setFlowsICMP2String(flowSet)) 
                    
                    populateICMPTunnel(event).alert()
           }
  
  
  
 /*
  * 
  * Horizontal PortScans
  * 
  * 
  */
  //val hPortScanMinFlowsThreshold = 300
  
    
  println("")
  println("Horizontal portscan")
 
  val hPortScanCollection: PairRDDFunctions[(String,String,String), (Long,Long,Long,HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)],Long,Long,Long)] = 
    sflowSummary
    .filter({case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) 
                  => !alienPort.equals("80")  &
                     !alienPort.equals("443") & // Avoid common ports
                     numberPkts < 5 // few pkts per flow
           })
    .map({
      case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) =>
         val flowSet:HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)] = new HashSet()
         flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status))
        ((myIP,alienIP,alienPort),(bytesUp,bytesDown,numberPkts,flowSet,1L,1L,sampleRate))
        })
  
  val hPortScanCollectionFinal=
  hPortScanCollection
  .reduceByKey({
    case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,numberOfflowsA,numberOffPairsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,numberOfflowsB,numberOffPairsB,sampleRateB)) =>
      (bytesUpA+bytesUpB,bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB,1L, (sampleRateA+sampleRateB)/2)
  })
  .map({
     case ((myIP,alienIP,alienPort),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numberOffPairsPort,sampleRate)) =>
        ((myIP,alienPort),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numberOffPairsPort,sampleRate))
  })
  .reduceByKey({
    case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,numberOfflowsA,numberOffPairsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,numberOfflowsB,numberOffPairsB,sampleRateB)) =>
      (bytesUpA+bytesUpB,bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB,numberOffPairsA+numberOffPairsB, (sampleRateA+sampleRateB)/2)
  })
  .cache
  
  val hPortScanStats = 
  hPortScanCollectionFinal
  .map({case ((myIP,alienPort),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numberOffPairsPort,sampleRate)) =>
        numberOffPairsPort
      }).stats()
  
  
  hPortScanCollectionFinal
  .filter({case ((myIP,alienPort),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numberOfPairsPort,sampleRate)) =>
          numberOfPairsPort > hPortScanMinFlowsThreshold
    })
  .map({
     case ((myIP,alienPort),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numberOfPairsPort,sampleRate)) =>
    
         val histogram: Map[String,Double] = new HashMap()
         histogram.put(alienPort,numberOfPairsPort)
         
       (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numberOfPairsPort,histogram,sampleRate))
  })
  .reduceByKey({
    case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,numberOfflowsA,numberOfPairsPortA,histogramA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,numberOfflowsB,numberOfPairsPortB,histogramB,sampleRateB)) =>
         
      histogramB./:(0){case  (c,(key,qtdH))=> val qtdH2 = {if(histogramA.get(key).isEmpty) 0D else histogramA.get(key).get }
                                                        histogramA.put(key,  qtdH2 + qtdH) 
                                                        0
                                 }
      (bytesUpA+bytesUpB,bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB,numberOfPairsPortA+numberOfPairsPortB, histogramA,(sampleRateA+sampleRateB)/2)
  })
  .filter{case (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numberOfPairsPort,numPorts,sampleRate)) =>
                   !p2pTalkers.contains(myIP)// Avoid P2P talkers
           }
  .foreach{case  (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numberOfPairsPort,histogram,sampleRate)) => 
    
                    val savedHistogram=HogHBaseHistogram.getHistogram("HIST07-"+myIP)
                    
                   
                    if(savedHistogram.histSize< 100)
                    {
                      HogHBaseHistogram.saveHistogram(Histograms.merge(savedHistogram, new HogHistogram("",numberOfPairsPort,histogram)))
                    }else
                    {
                          val atypical   = histogram.filter({ case (port,numPairsPort) =>
                                                  
                                                  if(savedHistogram.histMap.get(port).isEmpty)
                                                  {
                                                    true // This MyIP never accessed so much distinct Aliens in the same port
                                                  }else
                                                  {
                                                    if(savedHistogram.histMap.get(port).get.toLong < numPairsPort)
                                                      true // This MyIP never accessed so much distinct Aliens in the same port
                                                    else
                                                      false // Is typical
                                                  }
                                            })
                            
                           // Histograms.atypical(savedHistogram.histMap, histogram)

                          if(atypical.size>0)
                          {
                            println("MyIP: "+myIP+ "  (N:1,S:"+savedHistogram.histSize+") - Horizontal PortScan: "+numberOfflows+" Ports: "+atypical.toString)
                            
                            val flowMap: Map[String,String] = new HashMap[String,String]
                            flowMap.put("flow:id",System.currentTimeMillis.toString)
                            val event = new HogEvent(new HogFlow(flowMap,myIP,"255.255.255.255"))
                            event.data.put("numberOfFlows",numberOfflows.toString)
                            event.data.put("numberOfFlowsAlienPort",numberOfPairsPort.toString)
                            event.data.put("numberOfFlowsPerPort",atypical.map({case (port,number) => port+"="+number}).mkString("[",", ","]"))
                            event.data.put("myIP", myIP)
                            event.data.put("bytesUp",   (bytesUp*sampleRate).toString)
                            event.data.put("bytesDown", (bytesDown*sampleRate).toString)
                            event.data.put("numberPkts", numberPkts.toString)
                            event.data.put("stringFlows", setFlows2String(flowSet.filter({p => atypical.keySet.contains(p._4)})))
                            event.data.put("flowsMean", hPortScanStats.mean.round.toString)
                            event.data.put("flowsStdev", hPortScanStats.stdev.round.toString)
                            
                            populateHorizontalPortScan(event).alert()
                          }
                          
                          HogHBaseHistogram.saveHistogram(Histograms.merge(savedHistogram, new HogHistogram("",numberOfflows,histogram)))
                    }
             }
  
  
  
 /*
  * 
  * Horizontal PortScans
  * 
  * 
  */
  //val vPortScanMinPortsThreshold = 3
  
    
  println("")
  println("Vertical portscan")
 
  val vPortScanCollection: PairRDDFunctions[(String,String), (Long,Long,Long,HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)],Long,Long)] = 
    sflowSummary
    .filter({case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) 
                  => alienPort.toLong < vPortScanPortIntervalThreshold  &
                     numberPkts < 5 // few pkts per flow
           })
    .map({
      case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) =>
         val flowSet:HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)] = new HashSet()
         flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status))
        ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,1L,sampleRate))
        })
  
  val vPortScanCollectionFinal=
  vPortScanCollection
  .reduceByKey({
    case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,numberOfflowsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,numberOfflowsB,sampleRateB)) =>
      (bytesUpA+bytesUpB,bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB, (sampleRateA+sampleRateB)/2)
  })
  .map({case ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,sampleRate)) =>
        
        ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,
            flowSet
            .map({ case (myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status) => alienPort})
            .toArray
            .distinct
            .size,
          sampleRate))
      })
  .cache
  
  val vPortScanStats = 
  vPortScanCollectionFinal
  .map({case ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numPorts,sampleRate)) =>
        numPorts
      }).stats()
  
  
  vPortScanCollectionFinal
  .filter({case ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numPorts,sampleRate)) =>
        numPorts > vPortScanMinPortsThreshold
    })
  .filter{case ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numPorts,sampleRate)) =>
                   !p2pTalkers.contains(myIP)// Avoid P2P talkers
           }
  .foreach{case  ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,numPorts,sampleRate)) => 
    
                    val savedHistogram=HogHBaseHistogram.getHistogram("HIST08-"+myIP)
                    
                    val histogram = new HashMap[String,Double]
                    val key = numPorts.toString
                    histogram.put(key, 1D)
                    
                    if(savedHistogram.histSize< 10)
                    {
                      HogHBaseHistogram.saveHistogram(Histograms.merge(savedHistogram, new HogHistogram("",1L,histogram)))
                    }else
                    {

                          if(savedHistogram
                              .histMap
                              .filter({case (numberOfPorts,weight) =>
                                            numberOfPorts.toLong >= numPorts &
                                            weight > Histograms.atypicalThreshold
                                }).size  == 0
                            )
                          {
                            println("MyIP: "+myIP+ "  (N:1,S:"+savedHistogram.histSize+") - Vertical PortScan: "+numberOfflows+" numPorts: "+numPorts.toString)
                            
                            val flowMap: Map[String,String] = new HashMap[String,String]
                            flowMap.put("flow:id",System.currentTimeMillis.toString)
                            val event = new HogEvent(new HogFlow(flowMap,myIP,alienIP))
                            event.data.put("numberOfFlows",numberOfflows.toString)
                            event.data.put("numberOfPorts",numPorts.toString)
                            event.data.put("myIP", myIP)
                            event.data.put("alienIP", alienIP)
                            event.data.put("bytesUp",   (bytesUp*sampleRate).toString)
                            event.data.put("bytesDown", (bytesDown*sampleRate).toString)
                            event.data.put("numberPkts", numberPkts.toString)
                            event.data.put("stringFlows", setFlows2String(flowSet))
                            event.data.put("portsMean", vPortScanStats.mean.round.toString)
                            event.data.put("portsStdev", vPortScanStats.stdev.round.toString)
                            
                            populateVerticalPortScan(event).alert()
                          }
                          
                          HogHBaseHistogram.saveHistogram(Histograms.merge(savedHistogram, new HogHistogram("",1L,histogram)))
                    }
             }
  
  
  
  
  
  
 /*
  * 
  * Server under DDoS attack
  * 
  * 
  */

    
  println("")
  println("Server under DDoS attack")
 
  val ddosCollection: PairRDDFunctions[(String,String), (Long,Long,Long,HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)],Long,Long)] = 
    sflowSummary
    .filter({case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) 
                  => !isMyIP(alienIP, myNets)
           })
    .map({
      case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) =>
         val flowSet:HashSet[(String,String,String,String,String,Long,Long,Long,Int,Long,Long,Long,Int)] = new HashSet()
         flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status))
        ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,1L,sampleRate))
        })
  
  val ddosCollectionFinal=
  ddosCollection
  .reduceByKey({
    case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,numberOfflowsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,numberOfflowsB,sampleRateB)) =>
      (bytesUpA+bytesUpB,bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB, (sampleRateA+sampleRateB)/2)
  })
  .cache
  
  val ddosStats = 
  ddosCollectionFinal
  .map({case ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,sampleRate)) =>
        numberOfflows
      }).stats()
  
  
  ddosCollectionFinal
  .filter({case ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,sampleRate)) =>
        numberOfflows > ddosMinConnectionsThreshold &
        !p2pTalkers.contains(myIP) &// Avoid P2P talkers
        {
          
          val orderedFlowSet=
          flowSet
          .map({case (myIP,myPort,alienIP,alienPort,proto,bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status) => beginTime})
          .toIndexedSeq
          .sortBy { x => x }
              
          //TODO: Review. You should count the number of equals beginTime and discover why its generating error.
          if(orderedFlowSet.size>6)
          {  
              val flowSetMean=
              (orderedFlowSet.slice(1, orderedFlowSet.size)
              .zip(orderedFlowSet.slice(0, orderedFlowSet.size-1))
              .map({case (a,b) => a-b})
              .toSeq
              .sortBy { x => x }
              .slice(0,orderedFlowSet.size-4)
              .sum)/(orderedFlowSet.size-4)
              
               if(flowSetMean<60)
                true
               else
                 false
          }else
          {false}
        }
    })
  .map({case ((myIP,alienIP),(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,sampleRate)) =>
           (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,1L,sampleRate))
  })
  .reduceByKey({
    case ((bytesUpA,bytesDownA,numberPktsA,flowSetA,numberOfflowsA,pairsA,sampleRateA),(bytesUpB,bytesDownB,numberPktsB,flowSetB,numberOfflowsB,pairsB,sampleRateB)) =>
      (bytesUpA+bytesUpB,bytesDownA+bytesDownB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB,pairsA+pairsB,(sampleRateA+sampleRateB)/2)
  })
  .filter{case  (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,pairs,sampleRate)) => pairs > ddosMinPairsThreshold }
  .foreach{case  (myIP,(bytesUp,bytesDown,numberPkts,flowSet,numberOfflows,pairs,sampleRate)) => 
    
              
                            println("MyIP: "+myIP+ " - DDoS Attack: "+numberOfflows+" Pairs: "+pairs.toString)
                            
                            val flowMap: Map[String,String] = new HashMap[String,String]
                            flowMap.put("flow:id",System.currentTimeMillis.toString)
                            val event = new HogEvent(new HogFlow(flowMap,myIP,"255.255.255.255"))
                            event.data.put("numberOfFlows",numberOfflows.toString)
                            event.data.put("numberOfAttackers",pairs.toString)
                            event.data.put("myIP", myIP)
                            event.data.put("bytesUp",   (bytesUp*sampleRate).toString)
                            event.data.put("bytesDown", (bytesDown*sampleRate).toString)
                            event.data.put("numberPkts", numberPkts.toString)
                            event.data.put("stringFlows", setFlows2String(flowSet))
                            event.data.put("flowsMean", ddosStats.mean.round.toString)
                            event.data.put("flowsStdev", ddosStats.stdev.round.toString)
                            
                            // TODO: 
                           //populateDDoSAttack(event).alert()
                           
                  
          }
  
 /*
  * 
  * Find Operating Systems
  * 
  */
   
  println("")
  println("Find Operating Systems")
  
  val osRepos:scala.collection.immutable.Map[String,String] = 
       HogHBaseReputation.getReputationList("OSRepo", "windows")
                         .map({case ip => (ip -> "Windows")}).toMap++
       HogHBaseReputation.getReputationList("OSRepo", "linux")
                         .map({case ip => (ip -> "Linux OS")}).toMap++
       HogHBaseReputation.getReputationList("OSRepo", "android")
                         .map({case ip => (ip -> "Android")}).toMap++
       HogHBaseReputation.getReputationList("OSRepo", "apple")
                         .map({case ip => (ip -> "IOS")}).toMap++
       HogHBaseReputation.getReputationList("OSRepo", "freebsd")
                         .map({case ip => (ip -> "FreeBSD")}).toMap
                         
  val osKeySet:scala.collection.immutable.Set[String] = osRepos.keySet
  
  
  val osCollection: PairRDDFunctions[String, Set[String]] = 
    sflowSummary
    .filter({case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) 
                  => osKeySet.contains(alienIP)
           })
    .map({
      case ((myIP,myPort,alienIP,alienPort,proto),(bytesUp,bytesDown,numberPkts,direction,beginTime,endTime,sampleRate,status)) =>
            (myIP,Set(osRepos.get(alienIP).get))
        })
  
  val osCollectionFinal=
  osCollection
  .reduceByKey({
    case (osA,osB) =>
      (osA++osB)
  })
  .cache
  
  osCollectionFinal
  .foreach{case  (myIP,opSystems) => 
              opSystems.toArray.distinct
              .foreach { opSys =>
                             HogHBaseInventory.saveInventory(myIP, opSys)
                        }                
           }
  
  
  // END
  }
}
