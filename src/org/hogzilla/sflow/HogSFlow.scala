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
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Large amount of sent data (>"+threshold+")\n"+
                  "IP: "+hostname+"\n"+
                  "Bytes: "+bytes+"\n"
  
    event.signature_id = signature._1.signature_id       
    event
  }
  
  def populateSMTPTalker(event:HogEvent):HogEvent =
  {
    val src:String = event.data.get("src")
    val dst:String = event.data.get("dst")
    val bytes:String = event.data.get("bytes")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: SMTP communication\n"+
                  " "+src+" <-> "+dst+"  ("+bytes+" bytes)\n"
                  
    event.signature_id = signature._2.signature_id       
    event
  }
  
  
  def populateAtypicalTCPPortUsed(event:HogEvent):HogEvent =
  {
    val src:String = event.data.get("src")
    val tcpport:String = event.data.get("tcpport")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Atypical TCP port used ("+tcpport+")\n"+
                  " "+src+":"+tcpport+" <-> Alien or Host  \n"
                  
    event.signature_id = signature._3.signature_id       
    event
  }
  
  def populateAtypicalAlienTCPPortUsed(event:HogEvent):HogEvent =
  {
    val dst:String = event.data.get("dst")
    val tcpport:String = event.data.get("tcpport")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Atypical alien TCP port used ("+tcpport+")\n"+
                  " "+dst+" <-> Alien_or_Host:"+tcpport+"  \n"
                  
    event.signature_id = signature._4.signature_id       
    event
  }
  
  
  
  def populateAtypicalNumberOfPairs(event:HogEvent):HogEvent =
  {
    val dst:String = event.data.get("dst")
    val numberOfPairs:String = event.data.get("numberOfPairs")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Atypical number of pairs in the period ("+numberOfPairs+")\n"+
                  " "+dst+" <-> "+numberOfPairs+" distincts other hosts  \n"
                  
    event.signature_id = signature._5.signature_id       
    event
  }
                          
  def populateAtypicalAmountData(event:HogEvent):HogEvent =
  {
    val src:String = event.data.get("src")
    val bytes:String = event.data.get("bytes")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Atypical amount of data transfered ("+bytes+" bytes)\n"+
                  " "+src+" <-> other hosts ("+bytes+" bytes)  \n"
                  
    event.signature_id = signature._6.signature_id       
    event
  }  
  
  
  def populateAlienAccessingManyHosts(event:HogEvent):HogEvent =
  {
    val src:String = event.data.get("src")
    val numberOfPairs:String = event.data.get("numberOfPairs")
    
    event.text = "This IP was detected by Hogzilla performing an abnormal activity. In what follows, you can see more information.\n"+
                  "Abnormal behaviour: Alien accessing too much hosts ("+numberOfPairs+")\n"+
                  " "+src+" <-> "+numberOfPairs+" distinct hosts \n"
                  
    event.signature_id = signature._7.signature_id       
    event
  }  
  
  
  
  
  /**
   * 
   * 
   * 
   */
  def top(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)])
  {


    println("Filtering SflowRDD...")
    val SflowRDD = HogRDD.
        map { case (id,result) => {
          val map: Map[String,String] = new HashMap[String,String]
              map.put("flow:id",Bytes.toString(id.get).toString())
              HogHBaseRDD.columnsSFlow.foreach { column => 
                
                val ret = result.getValue(Bytes.toBytes(column.split(":")(0).toString()),Bytes.toBytes(column.split(":")(1).toString()))
                map.put(column, Bytes.toString(ret)) 
        }
                   
        val srcIP = result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("srcIP"))
        val dstIP = result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("dstIP"))
         // if(Bytes.toString(srcIP) < Bytes.toString(dstIP) )
         new HogFlow(map,srcIP,dstIP)
        //  else
          //  new HogFlow(map,dstIP,srcIP)
        }
    }.cache

  println("Counting SflowRDD...")
  val RDDtotalSize= SflowRDD.count()
  println("Filtered SflowRDD has "+RDDtotalSize+" rows!")

   val myNets =  new HashSet[String]
    val it = HogHBaseRDD.hogzilla_mynets.getScanner(new Scan()).iterator()
    while(it.hasNext())
    {
      myNets.add(Bytes.toString(it.next().getValue(Bytes.toBytes("net"),Bytes.toBytes("prefix"))))
    }
  
    
  
 /*
  * 
  * Top Talkers
  * 
  */
    
  val whiteTopTalkers = HogHBaseReputation.getReputationList("TTalker","whitelist")
  val topTalkersThreshold:Long = 21474836480L // (20*1024*1024*1024 = 20G)
  
  println("")
  println("Top Talkers (bytes):")
  println("(LowerIP <-> UpperIP, Bytes)")
  val g1: PairRDDFunctions[String, Long] = 
    SflowRDD.map( hogflow => (Bytes.toString(hogflow.lower_ip),hogflow.map.get("flow:packetSize").get.toLong) )
  
  g1.reduceByKey(_+_).sortBy(_._2, false, 10)
    .filter(tp => {  myNets.map { net => if( tp._1.startsWith(net) )
                                            { true } else{false} 
                                    }.contains(true) & (tp._2 > topTalkersThreshold )
          })
    .take(2000+whiteTopTalkers.size)
    .filter(tp => {  !whiteTopTalkers.map { net => if( tp._1.startsWith(net) )
                                            { true } else{false} 
                                    }.contains(true) 
          })
    .take(10)
    .foreach{ case (talker,bytes) => 
                    println("("+talker+","+bytes+")" ) 
                    val flowMap: Map[String,String] = new HashMap[String,String]
                    flowMap.put("flow:id",System.currentTimeMillis.toString)
                    val event = new HogEvent(new HogFlow(flowMap,InetAddress.getByName(talker).getAddress,
                                                                 InetAddress.getByName("255.255.255.255").getAddress))
                    event.data.put("hostname", talker)
                    event.data.put("bytes", bytes.toString)
                    event.data.put("threshold", topTalkersThreshold.toString)
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
  val g2: PairRDDFunctions[(String,String), (Long,Long)] 
           = SflowRDD.filter { flow => flow.map.get("flow:dstPort").equals("25") || flow.map.get("flow:srcPort").equals("25") }
                     .map { hogflow => 
                               ((Bytes.toString(hogflow.lower_ip),Bytes.toString(hogflow.upper_ip)),
                                (hogflow.map.get("flow:packetSize").get.toLong,1L)) 
                           }
  g2.reduceByKey{case ((a1,b1),(a2,b2)) => (a1+a2,b1+b2)}
    .sortBy({case ((src,dst),(bytes,qtFlows)) => bytes} , false, 10)
    .take(1000+whiteSMTPTalkers.size)
    .filter{case ((a1,b1),(a2,b2)) => ! (whiteSMTPTalkers.contains(a1) || whiteSMTPTalkers.contains(b1)) }
    .take(30)
    .foreach{case ((src,dst),(bytes,qtFlows)) => println("("+src+","+dst+","+bytes+","+qtFlows+")")
                    val flowMap: Map[String,String] = new HashMap[String,String]
                    flowMap.put("flow:id",System.currentTimeMillis.toString)
                    val event = new HogEvent(new HogFlow(flowMap,InetAddress.getByName(src).getAddress,
                                                                 InetAddress.getByName(dst).getAddress))
                    event.data.put("src", src)
                    event.data.put("dst", dst)
                    event.data.put("bytes", bytes.toString)
                    populateSMTPTalker(event).alert()
             }
             
  
 
 /*
  * 
  * Port Histogram - Atypical TCP port used
  * 
  * 
  * 
  */
  

  println("")
  println("Atypical TCP port used")
          
      
   val g3: PairRDDFunctions[String, (Map[String,Double], Long)] 
            = SflowRDD.filter { flow => flow.map.get("flow:tcpFlags").get.equals("0x12") }
                      .filter(flow => {  myNets.map { net =>
                                                         if( flow.map.get("flow:srcIP").get.startsWith(net) )
                                                          { true } else{false} 
                                                    }.contains(true)
                      }).map { flow => 
                               val map:Map[String,Double]=new HashMap[String,Double]
                               map.put(flow.map.get("flow:srcPort").get, 1D)                        
                       
                               (flow.map.get("flow:srcIP").get,  (map,1L)  ) 
                           }
            
   //println("Filtered RDD: "+g3.countByKey())
   
   val g3b = g3.reduceByKey{case ((map1,qtd1),(map2,qtd2)) => 
                       map2./:(0){case  (c,(key,qtdH))=> val qtdH2 = {if(map1.get(key).isEmpty) 0 else map1.get(key).get }
                                                        map1.put(key,  qtdH2 + qtdH) 
                                  0
                                 }
                      (map1,qtd1+qtd2)
                 }
    .map({ case (srcIP,(map,qtd)) =>
                            (srcIP,(map.map({ case (port,qtdC) => (port,qtdC/qtd.toDouble) }),qtd))
         })
         
    println("Mapped RDD1: "+g3b.count())
   
    g3b.foreach{case (srcIP,(map,qtd)) => 
                    
                    val hogHistogram=HogHBaseHistogram.getHistogram("HIST01-"+srcIP)
                    
                    
                    if(hogHistogram.histSize< 1000)
                    {
                      // Learn more!
                      //println("IP: "+srcIP+ "  (N:"+qtd+",S:"+hogHistogram.histSize+") - Learn More!")
                      HogHBaseHistogram.saveHistogram(Histograms.merge(hogHistogram, new HogHistogram("",qtd,map)))

                    }else
                    {

                    	    //val KBDistance = Histograms.KullbackLiebler(hogHistogram.histMap, map)
                    			val atypical   = Histograms.atypical(hogHistogram.histMap, map)
                          
                    			//println("KB: "+KBDistance)
                    			//println("ATypical: "+atypical)
                    			if(atypical.size > 0)
                    			{
                            println("Source IP: "+srcIP+ "  (N:"+qtd+",S:"+hogHistogram.histSize+") - Atypical (open) source ports: "+atypical.mkString(","))
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
                            */
                            
                            val flowMap: Map[String,String] = new HashMap[String,String]
                            flowMap.put("flow:id",System.currentTimeMillis.toString)
                            val event = new HogEvent(new HogFlow(flowMap,InetAddress.getByName(srcIP).getAddress,
                                                                         InetAddress.getByName("255.255.255.255").getAddress))
                            event.data.put("src", srcIP)
                            event.data.put("tcpport", atypical.mkString(","))
                            populateAtypicalTCPPortUsed(event).alert()
                    			}
                          
                          HogHBaseHistogram.saveHistogram(Histograms.merge(hogHistogram, new HogHistogram("",qtd,map)))
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
          
      //.filter { flow => flow.map.get("flow:tcpFlags").get.equals("0x12") }
   val g4: PairRDDFunctions[String, (Map[String,Double], Long)] 
                      = SflowRDD
                      .filter(flow => {  myNets.map { net =>
                                                         if( flow.map.get("flow:dstIP").get.startsWith(net) )
                                                          { true } else{false} 
                                                    }.contains(true) & (flow.map.get("flow:srcPort").get.toLong < 10000)
                      }).map { flow => 
                               val map:Map[String,Double]=new HashMap[String,Double]
                               map.put(flow.map.get("flow:srcPort").get, 1D)                        
                       
                               (flow.map.get("flow:dstIP").get, (map,1L) ) 
                           }
            
   //println("Filtered RDD: "+g3.countByKey())
   
   val g4b = g4.reduceByKey{case ((map1,qtd1),(map2,qtd2)) => 
                       map2./:(0){case  (c,(key,qtdH))=> val qtdH2 = {if(map1.get(key).isEmpty) 0 else map1.get(key).get }
                                                        map1.put(key,  qtdH2 + qtdH) 
                                  0
                                 }
                      (map1,qtd1+qtd2)
                 }
    .map({ case (dstIP,(map,qtd)) =>
                            (dstIP,(map.map({ case (port,qtdC) => (port,qtdC/qtd.toDouble) }),qtd))
         })
         
    println("Mapped RDD2: "+g4b.count())
   
    g4b.foreach{case (dstIP,(map,qtd)) => 
                    
                    val hogHistogram=HogHBaseHistogram.getHistogram("HIST02-"+dstIP)
                    
                    
                    if(hogHistogram.histSize< 1000)
                    {
                      // Learn more!
                      //println("IP: "+dstIP+ "  (N:"+qtd+",S:"+hogHistogram.histSize+") - Learn More!")
                      HogHBaseHistogram.saveHistogram(Histograms.merge(hogHistogram, new HogHistogram("",qtd,map)))

                    }else
                    {

                          //val KBDistance = Histograms.KullbackLiebler(hogHistogram.histMap, map)
                          val atypical   = Histograms.atypical(hogHistogram.histMap, map)

                          if(atypical.size>0 )
                          {
                            
                             println("Destination IP: "+dstIP+ "  (N:"+qtd+",S:"+hogHistogram.histSize+") - Atypical source ports: "+atypical.mkString(","))
                            
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
                            val event = new HogEvent(new HogFlow(flowMap,InetAddress.getByName(dstIP).getAddress,
                                                                         InetAddress.getByName("255.255.255.255").getAddress))
                            event.data.put("dst", dstIP)
                            event.data.put("tcpport", atypical.mkString(","))
                            populateAtypicalAlienTCPPortUsed(event).alert()
                          }
                      HogHBaseHistogram.saveHistogram(Histograms.merge(hogHistogram, new HogHistogram("",qtd,map)))

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
          
      //.filter { flow => flow.map.get("flow:tcpFlags").get.equals("0x12") }
   val g5: PairRDDFunctions[(String,String), Long] 
                      = SflowRDD.map ({ flow => 
                                
                               if(myNets.map { net =>  if( flow.map.get("flow:srcIP").get.startsWith(net) )
                                                          { true } else{false} 
                                              }.contains(true))
                               {
                                   ((flow.map.get("flow:srcIP").get,flow.map.get("flow:dstIP").get), 1L )
                               }else
                               {
                                   ((flow.map.get("flow:dstIP").get,flow.map.get("flow:srcIP").get), 1L )
                               } 
                           })
            
   //println("Filtered RDD: "+g3.countByKey())
   
   val g5b = g5.reduceByKey({ case (qtda,qtdb) => 0L})
                                              .map({case ((myIP,alienIP),qtd) => (myIP,1L)})
                                              .countByKey()
                                              //.reduceByKey({ case (qtda,qtdb) => qtda+qtdb})
     
    g5b.foreach{case (myIP,qtdCon) => 
                    
                    val hogHistogram=HogHBaseHistogram.getHistogram("HIST03-"+myIP)
                    
                    val map = new HashMap[String,Double]
                    val key = floor(log(qtdCon.*(1000))).toString
                    map.put(key, 1D)
                    
                    if(hogHistogram.histSize< 20)
                    {
                      // Learn more!
                      //println("MyIP: "+myIP+ "  (N:1,S:"+hogHistogram.histSize+") - Learn More!")
                      HogHBaseHistogram.saveHistogram(Histograms.merge(hogHistogram, new HogHistogram("",1L,map)))

                    }else
                    {

                          //val KBDistance = Histograms.KullbackLiebler(hogHistogram.histMap, map)
                          val atypical   = Histograms.atypical(hogHistogram.histMap, map)

                           
                          if(atypical.size>0 )
                          {
                            println("MyIP: "+myIP+ "  (N:1,S:"+hogHistogram.histSize+") - Atypical number of pairs in the period: "+qtdCon)
                           
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
                            event.data.put("dst", myIP)
                            event.data.put("numberOfPairs",qtdCon.toString)
                            populateAtypicalNumberOfPairs(event).alert()
                          }
                      HogHBaseHistogram.saveHistogram(Histograms.merge(hogHistogram, new HogHistogram("",1L,map)))

                    }
                    
             }
    
    
       
 /*
  * 
  * Atypical amount of data transfered
  * 
  */
  
    
  println("")
  println("Atypical amount of data transfered")
          
      //.filter { flow => flow.map.get("flow:tcpFlags").get.equals("0x12") }
   val g6: PairRDDFunctions[(String,String), Long] 
                      = SflowRDD.map ({ flow => 
                                
                               if(myNets.map { net =>  if( flow.map.get("flow:srcIP").get.startsWith(net) )
                                                          { true } else{false} 
                                              }.contains(true))
                               {
                                   ((flow.map.get("flow:srcIP").get,flow.map.get("flow:dstIP").get), flow.map.get("flow:packetSize").get.toLong )
                               }else
                               {
                                   ((flow.map.get("flow:dstIP").get,flow.map.get("flow:srcIP").get), flow.map.get("flow:packetSize").get.toLong  )
                               } 
                           })
            
   //println("Filtered RDD: "+g3.countByKey())
   
   val g6b = g6.reduceByKey({ case (sizea,sizeb) => sizea+sizeb})
                                              .map({case ((myIP,alienIP),size) => (myIP,size)})
                                              .reduceByKey({ case (sizea,sizeb) => sizea+sizeb})
     
    g6b.foreach{case (myIP,bytes) => 
                    
                    val hogHistogram=HogHBaseHistogram.getHistogram("HIST04-"+myIP)
                    
                    val map = new HashMap[String,Double]
                    val key = floor(log(bytes.*(0.0001))).toString
                    map.put(key, 1D)
                    
                    if(hogHistogram.histSize< 20)
                    {
                      // Learn more!
                      //println("MyIP: "+myIP+ "  (N:1,S:"+hogHistogram.histSize+") - Learn More!")
                      HogHBaseHistogram.saveHistogram(Histograms.merge(hogHistogram, new HogHistogram("",1L,map)))

                    }else
                    {

                          //val KBDistance = Histograms.KullbackLiebler(hogHistogram.histMap, map)
                          val atypical   = Histograms.atypical(hogHistogram.histMap, map)

                          
                          if(atypical.size>0 )
                          {
                             println("MyIP: "+myIP+ "  (N:1,S:"+hogHistogram.histSize+") - Atypical amount of transfered bytes: "+bytes)
                            
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
                            event.data.put("src", myIP)
                            event.data.put("tcpport", bytes.toString())
                            populateAtypicalAmountData(event).alert()
                          }
                      HogHBaseHistogram.saveHistogram(Histograms.merge(hogHistogram, new HogHistogram("",1L,map)))

                    }
                    
             }
    
    
       
 /*
  * 
  * Alien accessing too much hosts
  * 
  */
  
    
  println("")
  val alienThreshold = 2
  println("Aliens accessing more than "+alienThreshold+" hosts")
          
      //
   val g7: PairRDDFunctions[(String,String), Long] 
                      = SflowRDD.filter { flow => flow.map.get("flow:tcpFlags").get.equals("0x02") &
                                                  !myNets.map { net =>  if( flow.map.get("flow:srcIP").get.startsWith(net) )
                                                                             { true } else{false} 
                                                              }.contains(true)
                                        }
                           .map ({ flow => 
                               ((flow.map.get("flow:srcIP").get,flow.map.get("flow:dstIP").get), 1L )
                           })
            
   //println("Filtered RDD: "+g3.countByKey())
   
   val g7b = g7.reduceByKey({ case (qtda,qtdb) => 0L})
                                              .map({case ((alienIP,myIP),qtd) => (alienIP,1L)})
                                              .countByKey()
                                              //.reduceByKey({ case (qtda,qtdb) => qtda+qtdb})
     
    g7b.foreach{case (alienIP,qtdCon) => 
                    if(qtdCon > alienThreshold & !alienIP.equals("0.0.0.0") )
                    {                     
                            println("AlienIP: "+alienIP+ " more than "+alienThreshold+" pairs in the period: "+qtdCon)
                         
                            val flowMap: Map[String,String] = new HashMap[String,String]
                            flowMap.put("flow:id",System.currentTimeMillis.toString)
                            val event = new HogEvent(new HogFlow(flowMap,InetAddress.getByName(alienIP).getAddress,
                                                                         InetAddress.getByName("255.255.255.255").getAddress))
                            event.data.put("src", alienIP)
                            event.data.put("numberOfPairs", qtdCon.toString())
                            populateAlienAccessingManyHosts(event).alert()
                    }
                }
               
    
    
       
  
    /*
     * 
                    val flowMap: Map[String,String] = new HashMap[String,String]
                    flowMap.put("flow:id",System.currentTimeMillis.toString)
                    val event = new HogEvent(new HogFlow(flowMap,InetAddress.getByName(src).getAddress,
                                                                 InetAddress.getByName(dst).getAddress))
                    event.data.put("src", src)
                    event.data.put("dst", dst)
                    event.data.put("bytes", bytes.toString)
                    populateSMTPTalker(event).alert()
      
      
                      val it = map2.keySet().iterator()
                      while(it.hasNext())
                      {
                        val key = it.next()
                        if(map1.containsKey(key))
                        {
                          //map1.put(key, (map1.get(key)*qtd1+map2.get(key)*qtd2)/(qtd1+qtd2))
                          map1.put(key, map1.get(key)+map2.get(key))
                        }else
                        {
                          map1.put(key, map2.get(key))
                        }
                      }
     */
     

  }
  
  
  
}