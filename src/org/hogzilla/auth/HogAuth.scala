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


package org.hogzilla.auth

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
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.clustering.KMeans
import org.hogzilla.hbase.HogHBaseCluster
import org.hogzilla.cluster.HogClusterMember


/**
 * 
 */
object HogAuth {

                                                                                          
  val signature = (HogSignature(3,"HZ/Auth: Atypical access location" ,                2,1,826001201,826).saveHBase(), //1
                   HogSignature(3,"HZ/Auth: Atypical access user-agent" ,              2,1,826001202,826).saveHBase(), //2
                   HogSignature(3,"HZ/Auth: Atypical access system" ,                  2,1,826001203,826).saveHBase()) //3
                
  
 
  /**
   * 
   * 
   * 
   */
  def run(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)],spark:SparkContext)
  {
    
   //  XXX: Organize it!
   realRun(HogRDD,spark)
 
  }
  
 
  def isMyIP(ip:String,myNets:Set[String]):Boolean =
  {
    myNets.map ({ net =>  if( ip.startsWith(net) )
                              { true } 
                          else{false} 
                }).contains(true)
  }
  
   
  def populateAtypicalAccessLocation(event:HogEvent):HogEvent =
  {
                
    val userName:String = event.data.get("userName")
    val atypicalCities:String = event.data.get("atypicalCities")
    val accessLogs:String = event.data.get("accessLogs")
    
    event.title = f"HZ/Auth: Atypical access location ($atypicalCities)"
    
    event.text =  "The user accessed from locations which are not usual for his profile.\n"+
                  "Username: "+userName+"\n"+
                  "Atypical Cities: "+atypicalCities+"\n"+
                  "Atypical access logs:\n"+accessLogs
                  
    event.signature_id = signature._1.signature_id       
    event
  }
  
  

  def authTupleToString(hashSet:HashSet[(Double,String,String,String,String,String,Int,String,String,String,String,String,String)]):String =
	  {
		  hashSet.toList.sortBy({case (generatedTime, agent, service, clientReverse, clientIP, authMethod, 
                                  loginFailed, userAgent, country, region, city, coords, asn) =>  city })
		  ./:("")({ case (c,(generatedTime, agent, service, clientReverse, clientIP, authMethod, 
                                  loginFailed, userAgent, country, region, city, coords, asn)) 
			  => 
          
        var loginFailedString="SUCCESS"
		    if(loginFailed>0)
          loginFailedString="FAILED"
          
			  c+"\n"+clientIP+"("+clientReverse+") => "+agent+":"+service+"  [Location: "+city+"/"+region+"/"+country+", UA: "+userAgent+", AuthMethod: "+authMethod+", ASN: "+asn+" "+loginFailedString+"]"
		  })
	  }

  
  /**
   * 
   * 
   * 
   */
  def realRun(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)],spark:SparkContext)
  {
    
   val myNetsTemp =  new HashSet[String]
      
   val it = HogHBaseRDD.hogzilla_mynets.getScanner(new Scan()).iterator()
   while(it.hasNext())
   {
      myNetsTemp.add(Bytes.toString(it.next().getValue(Bytes.toBytes("net"),Bytes.toBytes("prefix"))))
   }
    
   val myNets:scala.collection.immutable.Set[String] = myNetsTemp.toSet
   
   
  val summary1: RDD[(Double,String,String,String,String,String,String,Int,String,String,String,String,String,String)] 
                      = HogRDD
                        .map ({  case (id,result) => 
                                    
                                      val generatedTime  = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("generatedTime"))).toDouble
                                      val agent          = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("agent")))
                                      val service        = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("service")))
                                      val clientReverse  = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("clientReverse")))
                                      val clientIP       = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("clientIP")))
                                      val userName       = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("userName")))
                                      val authMethod     = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("authMethod")))
                                      val loginFailed    = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("loginFailed"))).toInt
                                      val userAgent      = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("userAgent")))
                                      val country        = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("country")))
                                      val region         = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("region")))
                                      val city           = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("city")))
                                      val coords         = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("coords")))
                                      val asn            = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("asn")))
                                      
                                      (generatedTime, agent, service, clientReverse, clientIP, userName, authMethod, 
                                          loginFailed, userAgent, country, region, city, coords, asn)
                           }).cache

                           
  val summary1Count = summary1.count()
  if(summary1Count.equals(0))
    return
    
  val summaryUser:PairRDDFunctions[(String),(String,HashSet[(Double,String,String,String,String,String,Int,String,String,String,String,String,String)])] =                     
        summary1
        .map({case (generatedTime, agent, service, clientReverse, clientIP, userName, authMethod, 
                                          loginFailed, userAgent, country, region, city, coords, asn) =>
                val authSet:HashSet[(Double,String,String,String,String,String,Int,String,String,String,String,String,String)] = new HashSet()  
                    authSet.add((generatedTime, agent, service, clientReverse, clientIP, authMethod, 
                                          loginFailed, userAgent, country, region, city, coords, asn))                     
                                           
                (userName, (userName,authSet))
        })
        .reduceByKey({  (a,b)=> (a._1,  a._2++b._2)    })

  
  
    summaryUser
    .values
    .foreach{ case (userName,hashSet) => 
      
      val cities1:Map[String,Double] = 
        collection.mutable.Map() ++
        hashSet.groupBy({case tuple => (tuple._9,tuple._11)})
               .map({case (a,b) => (b.head._11.replace(" ", "_").trim()+"/"+b.head._9.replace(" ", "_").trim(), b.map(a => 1D).sum )}).toMap
        val totalCities = cities1.map(_._2).sum.toLong
        val citiesHistogram:Map[String,Double] = cities1.map({case (city,count) => (city,count/totalCities)})
      
        val savedHistogram=HogHBaseHistogram.getHistogram("HIST20-"+userName)

        if(savedHistogram.histSize< 10)
        {
        	HogHBaseHistogram.saveHistogram(Histograms.merge(savedHistogram, new HogHistogram("",totalCities,citiesHistogram)))
        }else
        {
        	    val atypical   = Histograms.atypical(savedHistogram.histMap, citiesHistogram)

        			if(atypical.size>0 & savedHistogram.histMap.filter({case (key,value) => value > 0.001D}).size <5)
        			{
        				val atypicalAccess = hashSet.filter({case tuple => atypical.contains(tuple._11.replace(" ", "_").trim())})
                
                println("UserName: "+userName+ " - Atypical access location: "+atypical.toString)

        				val flowMap: Map[String,String] = new HashMap[String,String]
        						flowMap.put("flow:id",System.currentTimeMillis.toString)
        						val event = new HogEvent(new HogFlow(flowMap,hashSet.head._5,hashSet.head._2))
                
                             
                                
                event.data.put("userName", userName) 
                event.data.put("atypicalCities", atypical.toString)          
        				event.data.put("accessLogs",authTupleToString(hashSet))
        				

        				populateAtypicalAccessLocation(event).alert()
        			}

          HogHBaseHistogram.saveHistogram(Histograms.merge(savedHistogram, new HogHistogram("",totalCities,citiesHistogram)))
        }

  }
    
  
  
  }

}