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


/**
 * 
 */
object HogSFlow {

  val signature = (HogSignature(3,"HZ: Top talker identified" ,2,1,826001001,826).saveHBase(),
                   HogSignature(3,"HZ: SMTP talker identified",1,1,826001002,826).saveHBase(),
                   HogSignature(3,"HZ: Atypical TCP port used",2,1,826001003,826).saveHBase(),
                   HogSignature(3,"HZ: Atypical alien TCP port used",2,1,826001004,826).saveHBase(),
                   HogSignature(3,"HZ: Atypical number of pairs in the period",2,1,826001005,826).saveHBase(),
                   HogSignature(3,"HZ: Atypical amount of data transfered",2,1,826001006,826).saveHBase(),
                   HogSignature(3,"HZ: Alien accessing too much hosts",3,1,826001007,826).saveHBase())
      
  
  /**
   * 
   * 
   * 
   */
  def run(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)],spark:SparkContext)
  {
    
   // TopTalkers, SMTP Talkers, XXX: Organize it!
   top(HogRDD)
 
  }
  
  
  def populateTopTalker(event:HogEvent):HogEvent =
  {
    val hostname:String = event.data.get("hostname")
    val bytes:String = event.data.get("bytes")
    val threshold:String = event.data.get("threshold")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Large amount of sent data (>"+threshold+")\n"+
                  "IP: "+hostname+"\n"+
                  "Bytes: "+bytes+"\n"+
                  "Packets: "+numberPkts+"\n"+
                  "Flows"+stringFlows
                    
    event.signature_id = signature._1.signature_id       
    event
  }
  
  def populateSMTPTalker(event:HogEvent):HogEvent =
  {
    val hostname:String = event.data.get("hostname")
    val bytes:String = event.data.get("bytes")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: SMTP communication\n"+
                  "IP: "+hostname+"\n"+
                  "Bytes: "+bytes+"\n"+
                  "Packets: "+numberPkts+"\n"+
                  "Flows"+stringFlows
                  
    event.signature_id = signature._2.signature_id       
    event
  }
  
  
  def populateAtypicalTCPPortUsed(event:HogEvent):HogEvent =
  {
    val tcpport:String = event.data.get("tcpport")
    val myIP:String = event.data.get("myIP")
    val bytes:String = event.data.get("bytes")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Atypical TCP/UDP port used ("+tcpport+")\n"+
                  "IP: "+myIP+"\n"+
                  "Bytes: "+bytes+"\n"+
                  "Packets: "+numberPkts+"\n"+
                  "Flows"+stringFlows
                  
    event.signature_id = signature._3.signature_id       
    event
  }
  
  def populateAtypicalAlienTCPPortUsed(event:HogEvent):HogEvent =
  {
    
    val tcpport:String = event.data.get("tcpport")
    val myIP:String = event.data.get("myIP")
    val bytes:String = event.data.get("bytes")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Atypical alien TCP/UDP port used ("+tcpport+")\n"+
                  "IP: "+myIP+"\n"+
                  "Total bytes: "+bytes+"\n"+
                  "Total packets: "+numberPkts+"\n"+
                  "Flows matching the atypical ports"+stringFlows
                  
    event.signature_id = signature._4.signature_id       
    event
  }
  
  
  def populateAtypicalNumberOfPairs(event:HogEvent):HogEvent =
  {
    val numberOfPairs:String = event.data.get("numberOfPairs")
    val myIP:String = event.data.get("myIP")
    val bytes:String = event.data.get("bytes")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Atypical number of pairs in the period ("+numberOfPairs+")\n"+
                  "IP: "+myIP+"\n"+
                  "Bytes: "+bytes+"\n"+
                  "Packets: "+numberPkts+"\n"+
                  "Number of pairs: "+numberOfPairs+"\n"+
                  "Flows"+stringFlows
                  
    event.signature_id = signature._5.signature_id       
    event
  }
                          
  def populateAtypicalAmountData(event:HogEvent):HogEvent =
  {
    val numberOfPairs:String = event.data.get("numberOfPairs")
    val myIP:String = event.data.get("myIP")
    val bytes:String = event.data.get("bytes")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Atypical amount of data transfered ("+bytes+" bytes)\n"+
                  "IP: "+myIP+"\n"+
                  "Bytes: "+bytes+"\n"+
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
    val bytes:String = event.data.get("bytes")
    val numberPkts:String = event.data.get("numberPkts")
    val stringFlows:String = event.data.get("stringFlows")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Alien accessing too much hosts ("+numberOfPairs+")\n"+
                  "AlienIP: "+alienIP+"\n"+
                  "Bytes: "+bytes+"\n"+
                  "Packets: "+numberPkts+"\n"+
                  "Number of pairs: "+numberOfPairs+"\n"+
                  "Flows"+stringFlows
                  
    event.signature_id = signature._7.signature_id       
    event
  }  
  
  
  def setFlows2String(flowSet:HashSet[(String,String,String,String,String,Long,Long,Int)]):String =
  {
     flowSet.toList.sortBy(_._5)
            .reverse
            ./:("")({ case (c,(srcIP1,srcPort1,dstIP1,dstPort1,proto1,packetsSize1,numberPkts1,direction1)) 
                        => 
                          if(direction1>0)
                          {
                           c+"\n"+
                           srcIP1+":"+srcPort1+" => "+dstIP1+":"+dstPort1+"  ("+proto1+", "+packetsSize1+" bytes,"+numberPkts1+" pkts)"
                          }else if(direction1<0)
                          {  
                           c+"\n"+
                           srcIP1+":"+srcPort1+" <= "+dstIP1+":"+dstPort1+"  ("+proto1+", "+packetsSize1+" bytes,"+numberPkts1+" pkts)"
                          }else
                          {  
                           c+"\n"+
                           srcIP1+":"+srcPort1+" <?> "+dstIP1+":"+dstPort1+"  ("+proto1+", "+packetsSize1+" bytes,"+numberPkts1+" pkts)"
                          }
                    })
  }
  
  
  /**
   * 
   * 
   * 
   */
  def top(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)])
  {
    
    println("Filtering SflowRDD...")
   

   val myNets =  new HashSet[String]
   val it = HogHBaseRDD.hogzilla_mynets.getScanner(new Scan()).iterator()
   while(it.hasNext())
   {
      myNets.add(Bytes.toString(it.next().getValue(Bytes.toBytes("net"),Bytes.toBytes("prefix"))))
   }
  
    
    
  /*
  * 
  * SFlow Summary
  * 
  */   
    
  //Directions
  val UNKNOWN   = 0  
  val LEFTRIGHT = 1
  val RIGHTLEFT = -1
      
  // (srcIP, srcPort, dstIP, dstPort, bytes, 1)
  val sflowSummary1: PairRDDFunctions[(String,String,String,String,String), (Long,Long,Int)] 
                      = HogRDD.map ({  case (id,result) => 
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
                                      val IPprotocol    = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("IPprotocol")))

                                      var direction = UNKNOWN
                                      var protoName="UDP" // We filter below TCP or UDP
                                      
                                      if(IPprotocol.equals("6")) // If is TCP
                                      {
                                        protoName="TCP"
                                        if(tcpFlags.equals("0x02")) // Is a SYN pkt
                                          direction = LEFTRIGHT
                                        if(tcpFlags.equals("0x12")) // Is a SYN-ACK pkt
                                          direction = RIGHTLEFT
                                      }
                                      
                               if(myNets.map { net =>  if( srcIP.startsWith(net) )
                                                          { true } else{false} 
                                              }.contains(true))
                               {
                                   ((  srcIP,
                                       srcPort,
                                       dstIP,
                                       dstPort, 
                                       protoName ), 
                                    (packetSize, 1L, direction,IPprotocol)
                                   )
                               }else
                               {
                                   ((  dstIP,
                                       dstPort,
                                       srcIP,
                                       srcPort, 
                                       protoName ), 
                                    (packetSize, 1L,-direction,IPprotocol)
                                   )
                               } 
                           })
                           .filter({case ((dstIP,dstPort,srcIP,srcPort, proto ),(packetSize,numberOfPkts,direction,iPprotocolNumber))
                                             =>  iPprotocolNumber.equals("6") || iPprotocolNumber.equals("17") // TCP or UDP
                                  })
                           .map({case ((dstIP,dstPort,srcIP,srcPort, proto ),(packetSize,numberOfPkts,direction,iPprotocol))
                                       =>((dstIP,dstPort,srcIP,srcPort, proto ),(packetSize,numberOfPkts,direction))
                                })
                             
                           
    
    // (srcIP, srcPort, dstIP, dstPort, totalBytes, numberOfPkts)
   val sflowSummary = 
     sflowSummary1
     .reduceByKey({ case ((bytesA,pktsA,directionA),(bytesB,pktsB,directionB)) => (bytesA+bytesB,pktsA+pktsB,directionA+directionB)})
     .cache

   
  println("Counting sflowSummary...")
  val RDDtotalSize= sflowSummary.count()
  println("Filtered sflowSummary has "+RDDtotalSize+" rows!")
  
 /*
  * 
  * Top Talkers
  * 
  */
    
  val whiteTopTalkers = HogHBaseReputation.getReputationList("TTalker","whitelist")
  val topTalkersThreshold:Long = 21474836480L // (20*1024*1024*1024 = 20G)
  
  println("")
  println("Top Talkers (bytes):")
  println("(MyIP, Bytes)")
  
  val topTalkerCollection: PairRDDFunctions[String, (Long,Long,HashSet[(String,String,String,String,String,Long,Long,Int)])] = 
    sflowSummary.map({
    case ((myIP,myPort,alienIP,alienPort,proto),(bytes,numberPkts,direction)) =>
       val flowSet:HashSet[(String,String,String,String,String,Long,Long,Int)] = new HashSet()
       flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytes,numberPkts,direction))
      (myIP,(bytes,numberPkts,flowSet))
  }).cache
  
  
  topTalkerCollection.reduceByKey({
    case ((bytesA,numberPktsA,flowSetA),(bytesB,numberPktsB,flowSetB)) =>
      (bytesA+bytesB, numberPktsA+numberPktsB, flowSetA++flowSetB)
  })
  .filter({
    case (myIP,(bytes,numberPkts,flowSet)) =>
    bytes > topTalkersThreshold
  })
  .sortBy({ 
    case (myIP,(bytes,numberPkts,flowSet)) =>    bytes  }, false, 15
   )
   .take(5000+whiteTopTalkers.size)
   .filter(tp => {  !whiteTopTalkers.map { net => if( tp._1.startsWith(net) )
                                            { true } else{false} 
                                    }.contains(true) 
          })
   .take(100)
  .foreach{ case (myIP,(bytes,numberPkts,flowSet)) => 
                    println("("+myIP+","+bytes+")" ) 
                    val flowMap: Map[String,String] = new HashMap[String,String]
                    flowMap.put("flow:id",System.currentTimeMillis.toString)
                    val event = new HogEvent(new HogFlow(flowMap,InetAddress.getByName(myIP).getAddress,
                                                                 InetAddress.getByName("255.255.255.255").getAddress))
                    event.data.put("hostname", myIP)
                    event.data.put("bytes", bytes.toString)
                    event.data.put("numberPkts", numberPkts.toString)
                    event.data.put("threshold", topTalkersThreshold.toString)
                    event.data.put("stringFlows", setFlows2String(flowSet))
                    
                    populateTopTalker(event).alert()
             }
  

  
    
 /*
  * 
  * SMTP Talkers
  * 
  */
    
  
  val whiteSMTPTalkers =  HogHBaseReputation.getReputationList("MX","whitelist")

  println("")
  println("SMTP Talkers:")
  println("(SRC IP, DST IP, Bytes, Qtd Flows)")
  
  
   val SMTPTalkersCollection: PairRDDFunctions[String, (Long,Long,HashSet[(String,String,String,String,String,Long,Long,Int)])] = sflowSummary
    .filter({case ((myIP,myPort,alienIP,alienPort,proto),(bytes,numberPkts,direction)) 
                  => (  alienPort.equals("25") &&  numberPkts>2 ) &&
                      !myNets.map { net =>  if( alienIP.startsWith(net) )  // Exclude internal communication
                                                          { true } else{false} 
                                              }.contains(true) 
                      
                      //! ( myPort.equals("25") && direction < 0 && numberPkts.equals(1L)) // Dismiss portscan. ie, SYN_pkt Alien->MyIP:25
           })
    .map({
      case ((myIP,myPort,alienIP,alienPort,proto),(bytes,numberPkts,direction)) =>
         val flowSet:HashSet[(String,String,String,String,String,Long,Long,Int)] = new HashSet()
         flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytes,numberPkts,direction))
        (myIP,(bytes,numberPkts,flowSet))
        }).cache
  
  
  SMTPTalkersCollection.reduceByKey({
    case ((bytesA,numberPktsA,flowSetA),(bytesB,numberPktsB,flowSetB)) =>
      (bytesA+bytesB, numberPktsA+numberPktsB, flowSetA++flowSetB)
  })
  .sortBy({ 
    case (myIP,(bytes,numberPkts,flowSet)) =>    bytes  }, false, 15
   )
   .take(5000+whiteSMTPTalkers.size)
   .filter(tp => {  !whiteSMTPTalkers.map { net => if( tp._1.startsWith(net) )
                                            { true } else{false} 
                                    }.contains(true) 
          })
   .take(100)
  .foreach{ case (myIP,(bytes,numberPkts,flowSet)) => 
                    println("("+myIP+","+bytes+")" ) 
                    val flowMap: Map[String,String] = new HashMap[String,String]
                    flowMap.put("flow:id",System.currentTimeMillis.toString)
                    val event = new HogEvent(new HogFlow(flowMap,InetAddress.getByName(myIP).getAddress,
                                                                 InetAddress.getByName("255.255.255.255").getAddress))
                    
                    event.data.put("hostname", myIP)
                    event.data.put("bytes", bytes.toString)
                    event.data.put("numberPkts", numberPkts.toString)
                    event.data.put("stringFlows", setFlows2String(flowSet))
                    
                    populateSMTPTalker(event).alert()
             }
  
  
 
 /*
  * 
  * Port Histogram - Atypical TCP port used
  * 
  * 
  */
  

  println("")
  println("Atypical TCP/UDP port used")
          
 val atypicalTCPCollection: PairRDDFunctions[String, (Long,Long,HashSet[(String,String,String,String,String,Long,Long,Int)],Map[String,Double],Long)] = 
    sflowSummary
    .filter({case ((myIP,myPort,alienIP,alienPort,proto),(bytes,numberPkts,direction)) 
                  =>  direction < 0 && numberPkts  >1
           })
    .map({
      case ((myIP,myPort,alienIP,alienPort,proto),(bytes,numberPkts,direction)) =>
         val flowSet:HashSet[(String,String,String,String,String,Long,Long,Int)] = new HashSet()
         flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytes,numberPkts,direction))
         
         val histogram: Map[String,Double] = new HashMap()
         histogram.put(myPort,1D)
         
        (myIP,(bytes,numberPkts,flowSet,histogram,1L))
        
        })
  
  
  val atypicalTCPCollectionFinal = 
     atypicalTCPCollection
     .reduceByKey({
       case ((bytesA,numberPktsA,flowSetA,histogramA,numberOfFlowsA),(bytesB,numberPktsB,flowSetB,histogramB,numberOfFlowsB)) =>
      
               histogramB./:(0){case  (c,(key,qtdH))=> val qtdH2 = {if(histogramA.get(key).isEmpty) 0D else histogramA.get(key).get }
                                                        histogramA.put(key,  qtdH2 + qtdH) 
                                                        0
                                 }
               (bytesA+bytesB, numberPktsA+numberPktsB, flowSetA++flowSetB, histogramA, numberOfFlowsA+numberOfFlowsB)
            })
     .map({ case (myIP,(bytes,numberPkts,flowSet,histogram,numberOfFlows)) =>
    
                            (myIP,(bytes,numberPkts,flowSet,histogram.map({ case (port,qtdC) => (port,qtdC/numberOfFlows.toDouble) }),numberOfFlows))
          }).cache
  
  
  atypicalTCPCollectionFinal   
    .foreach{case (myIP,(packetsSize,numberPkts,flowSet,histogram,numberOfFlows)) => 
                    
                    val hogHistogram=HogHBaseHistogram.getHistogram("HIST01-"+myIP)
                    
                    
                    if(hogHistogram.histSize < 1000)
                    {
                      //println("IP: "+dstIP+ "  (N:"+qtd+",S:"+hogHistogram.histSize+") - Learn More!")
                      HogHBaseHistogram.saveHistogram(Histograms.merge(hogHistogram, new HogHistogram("",numberOfFlows,histogram)))
                    }else
                    {
                          //val KBDistance = Histograms.KullbackLiebler(hogHistogram.histMap, map)
                          val atypical   = Histograms.atypical(hogHistogram.histMap, histogram)

                          if(atypical.size>0)
                          {
                            println("Source IP: "+myIP+ "  (N:"+numberOfFlows+",S:"+hogHistogram.histSize+") - Atypical (open) source ports: "+atypical.mkString(","))
                            
                            
                            val flowMap: Map[String,String] = new HashMap[String,String]
                            flowMap.put("flow:id",System.currentTimeMillis.toString)
                            val event = new HogEvent(new HogFlow(flowMap,InetAddress.getByName(myIP).getAddress,
                                                                         InetAddress.getByName("255.255.255.255").getAddress))
                            event.data.put("myIP", myIP)
                            event.data.put("tcpport", atypical.mkString(","))
                            event.data.put("bytes", packetsSize.toString)
                            event.data.put("numberPkts", numberPkts.toString)
                            event.data.put("stringFlows", setFlows2String(flowSet.filter({p => atypical.contains(p._2)})))
                    
                            populateAtypicalTCPPortUsed(event).alert()
                          }
                      HogHBaseHistogram.saveHistogram(Histograms.merge(hogHistogram, new HogHistogram("",numberOfFlows,histogram)))
                    }
                    
             }
  
    
    
 /*
  * 
  * Port Histogram - Atypical alien TCP port used
  * 
  * 
  */
  

  println("")
  println("Atypical alien TCP/UDP port used")
  
 val atypicalAlienTCPCollection: PairRDDFunctions[String, (Long,Long,HashSet[(String,String,String,String,String,Long,Long,Int)],Map[String,Double],Long)] = 
    sflowSummary
    .filter({case ((myIP,myPort,alienIP,alienPort,proto),(bytes,numberPkts,direction)) 
                  =>  alienPort.toLong < 10000  &&
                      direction > -1 &&
                      myPort.toLong>1024
           })
    .map({
      case ((myIP,myPort,alienIP,alienPort,proto),(bytes,numberPkts,direction)) =>
         val flowSet:HashSet[(String,String,String,String,String,Long,Long,Int)] = new HashSet()
         flowSet.add((myIP,myPort,alienIP,alienPort,proto,bytes,numberPkts,direction))
         
         val histogram: Map[String,Double] = new HashMap()
         histogram.put(alienPort,1D)
         
        (myIP,(bytes,numberPkts,flowSet,histogram,1L))
        
        })
  
  
  val atypicalAlienTCPCollectionFinal = 
     atypicalAlienTCPCollection
     .reduceByKey({
       case ((bytesA,numberPktsA,flowSetA,histogramA,numberOfFlowsA),(bytesB,numberPktsB,flowSetB,histogramB,numberOfFlowsB)) =>
      
               histogramB./:(0){case  (c,(key,qtdH))=> val qtdH2 = {if(histogramA.get(key).isEmpty) 0D else histogramA.get(key).get }
                                                        histogramA.put(key,  qtdH2 + qtdH) 
                                                        0
                                 }
               (bytesA+bytesB, numberPktsA+numberPktsB, flowSetA++flowSetB, histogramA, numberOfFlowsA+numberOfFlowsB)
            })
     .map({ case (myIP,(bytes,numberPkts,flowSet,histogram,numberOfFlows)) =>
    
                            (myIP,(bytes,numberPkts,flowSet,histogram.map({ case (port,qtdC) => (port,qtdC/numberOfFlows.toDouble) }),numberOfFlows))
          }).cache
  
  
  atypicalAlienTCPCollectionFinal   
    .foreach{case (myIP,(packetsSize,numberPkts,flowSet,histogram,numberOfFlows)) => 
                    
                    val hogHistogram=HogHBaseHistogram.getHistogram("HIST02-"+myIP)
                    
                    
                    if(hogHistogram.histSize < 1000)
                    {
                      //println("IP: "+dstIP+ "  (N:"+qtd+",S:"+hogHistogram.histSize+") - Learn More!")
                      HogHBaseHistogram.saveHistogram(Histograms.merge(hogHistogram, new HogHistogram("",numberOfFlows,histogram)))
                    }else
                    {
                          //val KBDistance = Histograms.KullbackLiebler(hogHistogram.histMap, map)
                          val atypical   = Histograms.atypical(hogHistogram.histMap, histogram)

                          if(atypical.size>0)
                          {
                            println("MyIP: "+myIP+ "  (N:"+numberOfFlows+",S:"+hogHistogram.histSize+") - Atypical alien ports: "+atypical.mkString(","))
                            
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
                            val event = new HogEvent(new HogFlow(flowMap,InetAddress.getByName(myIP).getAddress,
                                                                         InetAddress.getByName("255.255.255.255").getAddress))
                            event.data.put("myIP", myIP)
                            event.data.put("tcpport", atypical.mkString(","))
                            event.data.put("bytes", packetsSize.toString)
                            event.data.put("numberPkts", numberPkts.toString)
                            event.data.put("stringFlows", setFlows2String(flowSet.filter({p => atypical.contains(p._4)})))
                    
                            populateAtypicalAlienTCPPortUsed(event).alert()
                          }
                      HogHBaseHistogram.saveHistogram(Histograms.merge(hogHistogram, new HogHistogram("",numberOfFlows,histogram)))
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
 
  val atypicalNumberPairsCollection: PairRDDFunctions[(String,String), (Long,Long,HashSet[(String,String,String,String,String,Long,Long,Int)],Long)] = 
    sflowSummary
    .map({
      case ((myIP,myPort,alienIP,alienPort,proto),(packetsSize,numberPkts,direction)) =>
         val flowSet:HashSet[(String,String,String,String,String,Long,Long,Int)] = new HashSet()
         flowSet.add((myIP,myPort,alienIP,alienPort,proto,packetsSize,numberPkts,direction))
        ((myIP,alienIP),(packetsSize,numberPkts,flowSet,1L))
        })
  
  val atypicalNumberPairsCollectionFinal =
  atypicalNumberPairsCollection
  .reduceByKey({
    case ((packetsSizeA,numberPktsA,flowSetA,numberOfflowsA),(packetsSizeB,numberPktsB,flowSetB,numberOfflowsB)) =>
      (packetsSizeA+packetsSizeB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB)
  })
  .map({
     case ((myIP,alienIP),(packetsSize,numberPkts,flowSet,numberOfflows)) =>
    
       (myIP,(packetsSize,numberPkts,flowSet,numberOfflows,1L))
  })
  .reduceByKey({
    case ((packetsSizeA,numberPktsA,flowSetA,numberOfflowsA,pairsA),(packetsSizeB,numberPktsB,flowSetB,numberOfflowsB,pairsB)) =>
      (packetsSizeA+packetsSizeB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB, pairsA+pairsB)
  }).cache
  
  
  
  atypicalNumberPairsCollectionFinal
  .foreach{case  (myIP,(packetsSize,numberPkts,flowSet,numberOfflows,numberOfPairs)) => 
                    
                    val savedHistogram=HogHBaseHistogram.getHistogram("HIST03-"+myIP)
                    
                    val histogram = new HashMap[String,Double]
                    val key = floor(log(numberOfPairs.*(1000))).toString
                    histogram.put(key, 1D)
                    
                    if(savedHistogram.histSize< 20)
                    {
                      //println("MyIP: "+myIP+ "  (N:1,S:"+hogHistogram.histSize+") - Learn More!")
                      HogHBaseHistogram.saveHistogram(Histograms.merge(savedHistogram, new HogHistogram("",1L,histogram)))
                    }else
                    {
                          //val KBDistance = Histograms.KullbackLiebler(hogHistogram.histMap, map)
                          val atypical   = Histograms.atypical(savedHistogram.histMap, histogram)

                          if(atypical.size>0 )
                          {
                            println("MyIP: "+myIP+ "  (N:1,S:"+savedHistogram.histSize+") - Atypical number of pairs in the period: "+numberOfPairs)
                            
                            val flowMap: Map[String,String] = new HashMap[String,String]
                            flowMap.put("flow:id",System.currentTimeMillis.toString)
                            val event = new HogEvent(new HogFlow(flowMap,InetAddress.getByName(myIP).getAddress,
                                                                         InetAddress.getByName("255.255.255.255").getAddress))
                            event.data.put("numberOfPairs",numberOfPairs.toString)
                            event.data.put("myIP", myIP)
                            event.data.put("bytes", packetsSize.toString)
                            event.data.put("numberPkts", numberPkts.toString)
                            event.data.put("stringFlows", setFlows2String(flowSet))
                            
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
  
  atypicalNumberPairsCollectionFinal
  .foreach{case  (myIP,(bytes,numberPkts,flowSet,numberOfflows,numberOfPairs)) => 
                    
                    val savedHistogram=HogHBaseHistogram.getHistogram("HIST04-"+myIP)
                    
                    val histogram = new HashMap[String,Double]
                    val key = floor(log(bytes.*(0.0001))).toString
                    histogram.put(key, 1D)
                    
                    if(savedHistogram.histSize< 20)
                    {
                      //println("MyIP: "+myIP+ "  (N:1,S:"+hogHistogram.histSize+") - Learn More!")
                      HogHBaseHistogram.saveHistogram(Histograms.merge(savedHistogram, new HogHistogram("",1L,histogram)))
                    }else
                    {
                          //val KBDistance = Histograms.KullbackLiebler(hogHistogram.histMap, map)
                          val atypical   = Histograms.atypical(savedHistogram.histMap, histogram)

                          if(atypical.size>0 )
                          {
                             println("MyIP: "+myIP+ "  (N:1,S:"+savedHistogram.histSize+") - Atypical amount of transfered bytes: "+bytes)
                            
                            val flowMap: Map[String,String] = new HashMap[String,String]
                            flowMap.put("flow:id",System.currentTimeMillis.toString)
                            val event = new HogEvent(new HogFlow(flowMap,InetAddress.getByName(myIP).getAddress,
                                                                         InetAddress.getByName("255.255.255.255").getAddress))
                            event.data.put("numberOfPairs",numberOfPairs.toString)
                            event.data.put("myIP", myIP)
                            event.data.put("bytes", bytes.toString)
                            event.data.put("numberPkts", numberPkts.toString)
                            event.data.put("stringFlows", setFlows2String(flowSet))
                            
                            populateAtypicalAmountData(event).alert()
                          }
                          
                          HogHBaseHistogram.saveHistogram(Histograms.merge(savedHistogram, new HogHistogram("",1L,histogram)))
                    }
             }
  
  
  
       
 /*
  * 
  * Alien accessing too much hosts
  * 
  */
  
    
  println("")
  val alienThreshold = 20
  println("Aliens accessing more than "+alienThreshold+" hosts")
 
  
   val alienTooManyPairsCollection: PairRDDFunctions[(String,String), (Long,Long,HashSet[(String,String,String,String,String,Long,Long,Int)],Long)] = 
    sflowSummary
    .filter({ case ((myIP,myPort,alienIP,alienPort,proto),(packetsSize,numberPkts,direction)) 
                 => direction < 0  &&
                     ! myNets.map { net =>  if( alienIP.startsWith(net) )
                                                          { true } else{false} 
                                              }.contains(true)
            })
    .map({
      case ((myIP,myPort,alienIP,alienPort,proto),(packetsSize,numberPkts,direction)) =>
         val flowSet:HashSet[(String,String,String,String,String,Long,Long,Int)] = new HashSet()
         flowSet.add((myIP,myPort,alienIP,alienPort,proto,packetsSize,numberPkts,direction))
        ((myIP,alienIP),(packetsSize,numberPkts,flowSet,1L))
        })
  
  val alienTooManyPairsCollectionFinal =
  alienTooManyPairsCollection
  .reduceByKey({
    case ((packetsSizeA,numberPktsA,flowSetA,numberOfflowsA),(packetsSizeB,numberPktsB,flowSetB,numberOfflowsB)) =>
      (packetsSizeA+packetsSizeB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB)
  })
  .map({
     case ((myIP,alienIP),(packetsSize,numberPkts,flowSet,numberOfflows)) =>
    
       (alienIP,(packetsSize,numberPkts,flowSet,numberOfflows,1L))
  })
  .reduceByKey({
    case ((packetsSizeA,numberPktsA,flowSetA,numberOfflowsA,pairsA),(packetsSizeB,numberPktsB,flowSetB,numberOfflowsB,pairsB)) =>
      (packetsSizeA+packetsSizeB, numberPktsA+numberPktsB, flowSetA++flowSetB, numberOfflowsA+numberOfflowsB, pairsA+pairsB)
  }).cache
  
  
  
  alienTooManyPairsCollectionFinal
  .foreach{case  (alienIP,(packetsSize,numberPkts,flowSet,numberOfflows,numberOfPairs)) => 
                    
                    if(numberOfPairs > alienThreshold && !alienIP.equals("0.0.0.0") )
                    {                     
                            println("AlienIP: "+alienIP+ " more than "+alienThreshold+" pairs in the period: "+numberOfPairs)
                         
                            val flowMap: Map[String,String] = new HashMap[String,String]
                            flowMap.put("flow:id",System.currentTimeMillis.toString)
                            val event = new HogEvent(new HogFlow(flowMap,InetAddress.getByName(alienIP).getAddress,
                                                                         InetAddress.getByName("255.255.255.255").getAddress))
                            
                            event.data.put("numberOfPairs",numberOfPairs.toString)
                            event.data.put("alienIP", alienIP)
                            event.data.put("bytes", packetsSize.toString)
                            event.data.put("numberPkts", numberPkts.toString)
                            event.data.put("stringFlows", setFlows2String(flowSet))
                            
                            populateAlienAccessingManyHosts(event).alert()
                    }
             }
  

  }
  
  
  
}