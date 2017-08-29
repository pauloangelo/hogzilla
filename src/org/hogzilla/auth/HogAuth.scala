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

import scala.collection.mutable.HashMap
import org.uaparser.scala.Parser
import org.uaparser.scala.Client
import scala.collection.mutable.HashSet
import scala.collection.mutable.Map
import scala.math.floor
import scala.math.log
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.hogzilla.event.HogEvent
import org.hogzilla.event.HogSignature
import org.hogzilla.hbase.HogHBaseHistogram
import org.hogzilla.hbase.HogHBaseRDD
import org.hogzilla.histogram.Histograms
import org.hogzilla.histogram.HogHistogram
import org.hogzilla.util.HogFlow
import org.hogzilla.util.HogFlow
import org.hogzilla.util.HogFlow
import org.hogzilla.util.HogGeograph
import org.hogzilla.util.HogStringUtils
import com.typesafe.config.ConfigFactory
import java.io.File
import org.hogzilla.util.HogConfig
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Result
import java.util.Formatter.DateTime
import java.text.SimpleDateFormat


/**
 * 
 */
object HogAuth {

                                                                                          
  val signature = (HogSignature(3,"HZ/Auth: Atypical access location" ,                2,1,826001201,826).saveHBase(), //1
                   HogSignature(3,"HZ/Auth: Atypical access user-agent" ,              2,1,826001202,826).saveHBase(), //2
                   HogSignature(3,"HZ/Auth: Atypical access service or system" ,                  2,1,826001203,826).saveHBase()) //3
                
   val config = ConfigFactory.parseFile(new File("auth.conf"))
                
   // In KM, doesn't alert if the new location is near a typical location
   val locationDistanceMinThreshold = HogConfig.getInt (config,"location.allowedRadix",300)
   val locationExcludedCities = HogConfig.getSetString (config,"location.excludedCities",Set("Campinas"))
   val locationReverseDomainsWhitelist = HogConfig.getSetString (config,"location.reverseDomainsWhitelist",Set("google.com","gmail.com"))
   val UAexcludedCities = HogConfig.getSetString (config,"useragent.excludedCities",Set())
   val systemExcludedCities = HogConfig.getSetString (config,"system.excludedCities",Set())
   val UAReverseDomainsWhitelist = HogConfig.getSetString (config,"useragent.reverseDomainsWhitelist",Set("google.com","gmail.com"))
   val systemReverseDomainsWhitelist = HogConfig.getSetString (config,"system.reverseDomainsWhitelist",Set("google.com","gmail.com"))
   val locationDisabled = HogConfig.getInt(config,"location.disabled",0) // 1: just training, 2: nothing
   val UADisabled = HogConfig.getInt(config,"useragent.disabled",0) // 1: just training, 2: nothing
   val serviceDisabled = HogConfig.getInt(config,"system.disabled",0) // 1: just training, 2: nothing
 
  /**
   * 
   * 
   * 
   */
  def run(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)],spark:SparkContext)
  {
    
    
   if(locationDisabled>=2 &&
      UADisabled >=2 &&
      serviceDisabled>=2)
     return
    
   realRun(HogRDD,spark)
 
  }
  
  def runDeleting(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)],spark:SparkContext)
  {
    
    
   if(locationDisabled>=2 &&
      UADisabled >=2 &&
      serviceDisabled>=2)
     return
    
   realRun(HogRDD,spark,true)
 
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
    
    event.title = f"HZ/Auth: Atypical access location ($atypicalCities) for user $userName"
    
    event.text =  "The user accessed from locations which are not usual for his profile.\n"+
                  "Username: "+userName+"\n"+
                  "Atypical Cities: "+atypicalCities+"\n"+
                  "Atypical access logs:\n"+accessLogs
                  
    event.signature_id = signature._1.signature_id   
    event.username = userName
    event
  }
  
  def populateAtypicalAccessUserAgent(event:HogEvent):HogEvent =
  {           
    val userName:String = event.data.get("userName")
    val atypicalUserAgents:String = event.data.get("atypicalUserAgents")
    val accessLogs:String = event.data.get("accessLogs")
    
    event.title = f"HZ/Auth: Atypical access UserAgent ($atypicalUserAgents) for user $userName"
    
    event.text =  "The user accessed using UserAgents which are not usual for his profile.\n"+
                  "Username: "+userName+"\n"+
                  "Atypical UserAgents: "+atypicalUserAgents+"\n"+
                  "Atypical access logs:\n"+accessLogs
                  
    event.signature_id = signature._2.signature_id   
    event.username = userName    
    event
  }
  
  def populateAtypicalAccessService(event:HogEvent):HogEvent =
  {           
    val userName:String = event.data.get("userName")
    val atypicalServices:String = event.data.get("atypicalServices")
    val accessLogs:String = event.data.get("accessLogs")
    
    event.title = f"HZ/Auth: Atypical access Service ($atypicalServices) for user $userName"
    
    event.text =  "The user accessed services which are not usual for his profile.\n"+
                  "Username: "+userName+"\n"+
                  "Atypical Services: "+atypicalServices+"\n"+
                  "Atypical access logs:\n"+accessLogs
                  
    event.signature_id = signature._3.signature_id  
    event.username = userName     
    event
  }
  
  def locationString(country:String, region:String, city:String):String = {
    
      if(city.equals("N/A") || region.equals("N/A"))
        if(country.equals("N/A"))
          "location not defined"
        else
          country
      else
        city+"/"+region+"/"+country
  }

  def authTupleToString(hashSet:HashSet[(Double,String,String,String,String,String,Int,String,String,String,String,String,String)]):String =
	  {
		  hashSet.toList.sortBy({case (generatedTime, agent, service, clientReverse, clientIP, authMethod, 
                                  loginFailed, userAgent, country, region, city, coords, asn) =>  city })
		  ./:("")({ case (c,(generatedTime, agent, service, clientReverse, clientIP, authMethod, 
                                  loginFailed, userAgent, country, region, city, coords, asn)) 
			  => 
          
        val datePrinted =(new SimpleDateFormat("dd/MM/yyyy")).format(generatedTime.toLong*1000) 
        val timePrinted =(new SimpleDateFormat("HH'h'mm'm'")).format(generatedTime.toLong*1000) 
          
        var loginFailedString="SUCCESS"
		    if(loginFailed>0)
          loginFailedString="FAILED"
          
          if(clientReverse.equals(""))
			      c+"\n"+clientIP+" => "+agent+":"+service+"  [Location: "+locationString(country,region,city)+", UA: "+userAgent+", AuthMethod: "+authMethod+", ASN: "+asn+", DATE: "+datePrinted+", TIME: "+timePrinted+", "+loginFailedString+"]"
          else
            c+"\n"+clientIP+"("+clientReverse+") => "+agent+":"+service+"  [Location: "+locationString(country,region,city)+", UA: "+userAgent+", AuthMethod: "+authMethod+", ASN: "+asn+", DATE: "+datePrinted+", TIME: "+timePrinted+", "+loginFailedString+"]"
		  })
	  }
  

  
  /**
   * 
   * 
   * 
   */
  def realRun(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)],spark:SparkContext, deleteRecord:Boolean=false):RDD[(Double,String,String,String,String,String,String,Int,String,String,String,String,String,String,Result)] =
  {
    
   val myNetsTemp =  new HashSet[String]
      
   val it = HogHBaseRDD.hogzilla_mynets.getScanner(new Scan()).iterator()
   while(it.hasNext())
   {
      myNetsTemp.add(Bytes.toString(it.next().getValue(Bytes.toBytes("net"),Bytes.toBytes("prefix"))))
   }
    
   val myNets:scala.collection.immutable.Set[String] = myNetsTemp.toSet
   
  
  //println("Mapping auth records...")                     
  val summary1: RDD[(Double,String,String,String,String,String,String,Int,String,String,String,String,String,String,Result)] 
                      = HogRDD
                        .map ({  case (id,result) => 
                                    
                                      val generatedTime  = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("generatedTime"))).toDouble
                                      val agent          = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("agent")))
                                      val service        = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("service")))
                                      val clientReverse  = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("clientReverse")))
                                      val clientIP       = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("clientIP")))
                                      val userName       = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("userName")))
                                      val authMethod     = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("authMethod")))
                                      var loginFailed1=0
                                      try {
                                        var loginFailed1    = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("loginFailed"))).toInt
                                      } catch {
                                        case t: Throwable =>// t.printStackTrace() // TODO: handle error
                                      
                                      }
                                     
                                      val loginFailed=loginFailed1
                                      val userAgent      = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("userAgent")))
                                      val clientUA       = { if (userAgent.length().equals(0)) "" else 
                                                              { 
                                                                 val client=Parser.default.parse(userAgent)
                                                                 client.os.family+"/"+client.userAgent.family
                                                              } 
                                                            }
                                      val country        = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("country")))
                                      val region         = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("region")))
                                      val city           = Bytes.toString(new String(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("city")),"ISO-8859-1").getBytes("UTF-8"))
                                      //val city         = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("city")))
                                      val coords         = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("coords")))
                                      val asn            = Bytes.toString(result.getValue(Bytes.toBytes("auth"), Bytes.toBytes("asn")))
                                      
                                      if(deleteRecord)
                                         HogHBaseRDD.hogzilla_authrecords.delete(new Delete(result.getRow))

                                      
                                      (generatedTime, agent, service, clientReverse, clientIP, userName, authMethod, 
                                          loginFailed, clientUA, country.replace("Brazil","Brasil"), region, city.substring(0, Math.min(city.length(), 20)), coords, asn, result)
                           }).filter({case (generatedTime, agent, service, clientReverse, clientIP, userName, authMethod, 
                                          loginFailed, userAgent, country, region, city, coords, asn, result) =>
                                            coords.length()<30 &&
                                            userName.length>0 }).cache

  //println("Counting auth records...")                         
  val summary1Count = summary1.count()
  if(summary1Count.equals(0))
    return summary1;
    
  val summaryUser:PairRDDFunctions[(String),(String,HashSet[(Double,String,String,String,String,String,Int,String,String,String,String,String,String)])] =                     
        summary1
        .map({case (generatedTime, agent, service, clientReverse, clientIP, userName, authMethod, 
                                          loginFailed, userAgent, country, region, city, coords, asn, result) =>
                val authSet:HashSet[(Double,String,String,String,String,String,Int,String,String,String,String,String,String)] = new HashSet()  
                    authSet.add((generatedTime, agent, service, clientReverse, clientIP, authMethod, 
                                          loginFailed, userAgent, country, region, city, coords, asn))                     
                   
                (userName, (userName,authSet))
        })
        .reduceByKey({  (a,b)=> (a._1,  a._2++b._2)    })

  
  
    summaryUser
    .values
    .foreach{ case (userName,hashSet) => 
      
        //println(f"Auth profile for user $userName")
      
        // Atypical Cities
        val cities1:Map[String,(Double,String)] = 
              collection.mutable.Map() ++
              hashSet.groupBy({case tuple => (tuple._9,tuple._11,tuple._12)})
                     .map({case (a,b) => (b.head._12, (b.size.toDouble , b.head._11.replace(" ", "_").trim()+"/"+b.head._9.replace(" ", "_").trim()))}).toMap
        val totalCities = cities1.map({case (coord,(count,label))=> count }).sum.toLong
        val citiesHistogram:Map[String,Double] = cities1.map({case (coord,(count,label)) => (coord,count/totalCities)})
        val citiesHistogramLabels:Map[String,String] = cities1.map({case (coord,(count,label)) => (coord,label)})
        val citiesSavedHistogram=HogHBaseHistogram.getHistogram("HIST20-"+userName)
        
        // Atypical UserAgents
        val userAgents1:Map[String,Double] = 
              collection.mutable.Map() ++
              hashSet.filter(!_._8.isEmpty())
                     .groupBy({case tuple => tuple._8})
                     .map({case (a,b) => (b.head._8, b.size.toDouble )}).toMap
        val totalUserAgents = userAgents1.map(_._2).sum.toLong
        val userAgentHistogram:Map[String,Double] = userAgents1.map({case (ua,count) => (ua,count/totalUserAgents)})
        val userAgentSavedHistogram=HogHBaseHistogram.getHistogram("HIST21-"+userName)
        
        // Atypical Server/Service
        val services1:Map[String,Double] = 
              collection.mutable.Map() ++
              hashSet.groupBy({case tuple => (tuple._2,tuple._3)})
                     .map({case (a,b) => (b.head._2.replace(" ", "_").trim()+"/"+b.head._3.replace(" ", "_").trim(), b.size.toDouble )}).toMap
        val totalServices = services1.map(_._2).sum.toLong
        val servicesHistogram:Map[String,Double] = services1.map({case (service,count) => (service,count/totalServices)})
        val servicesSavedHistogram=HogHBaseHistogram.getHistogram("HIST22-"+userName)
      
        
        // Atypical Cities
        if(locationDisabled<2) // if not fully disabled
        if(citiesSavedHistogram.histSize< 10){
        	HogHBaseHistogram.saveHistogram(Histograms.merge(citiesSavedHistogram, new HogHistogram("",totalCities,citiesHistogram,citiesHistogramLabels)))
        }else{
        	    val atypicalCities   = 
                Histograms.atypical(citiesSavedHistogram.histMap, citiesHistogram)
                         /* Implemented below
                           .filter( { case (coords1)  =>  // Filter just far cities from the known cities.
                                      ! citiesSavedHistogram.histLabels.keySet
                                        .map ({ coords2 => HogGeograph.haversineDistanceFromStrings(coords1,coords2) < locationDistanceMinThreshold })
                                        .contains(true)
                                })*/
                

        			if(atypicalCities.size>0 & citiesSavedHistogram.histMap.filter({case (key,value) => value > 0.001D}).size <5)
        			{
        				val atypicalAccess = hashSet.filter({case tuple => 
                                                           atypicalCities.contains(tuple._12) &&
                                                           ! citiesSavedHistogram.histLabels.keySet
                                                             .map ({ coords2 => HogGeograph.haversineDistanceFromStrings(tuple._12,coords2) < locationDistanceMinThreshold })
                                                             .contains(true) &&
                                                           ! locationExcludedCities.contains(tuple._11)  &&
                                                           ! locationReverseDomainsWhitelist
                                                              .map { domain => tuple._4.endsWith(domain) }
                                                              .contains(true) &&
                                                            ! tuple._11.equals("N/A")&& //city
                                                            ! tuple._10.equals("N/A")&& //region
                                                            ! tuple._9.equals("N/A") &&   // country
                                                            ! tuple._11.equals(" ")&& //city
                                                            ! tuple._10.equals(" ")&& //region
                                                            ! tuple._9.equals(" ")    // country
                                                    })
                                                    
                  if(atypicalAccess.size>0) {
                      val atypicalCitiesNames = atypicalAccess.map({ tuple => tuple._11.replace(" ", "_").trim()+"/"+tuple._9.replace(" ", "_").trim()})
                      
                      //println("UserName: "+userName+ " - Atypical access location: "+atypicalCitiesNames.mkString(","))
      
              				val flowMap: Map[String,String] = new HashMap[String,String]
              				flowMap.put("flow:id",System.currentTimeMillis.toString)
              				val event = new HogEvent(new HogFlow(flowMap,atypicalAccess.head._5,atypicalAccess.head._2))
                                   
                      event.data.put("userName", userName) 
                      event.data.put("atypicalCities", atypicalCitiesNames.mkString(","))          
              				event.data.put("accessLogs",authTupleToString(atypicalAccess))          
                      event.data.put("coords",atypicalAccess.map(_._12).head)
              			
                      if(locationDisabled<1)
              				  populateAtypicalAccessLocation(event).alert()
                  }
        			}
              HogHBaseHistogram.saveHistogram(Histograms.merge(citiesSavedHistogram, new HogHistogram("",totalCities,citiesHistogram,citiesHistogramLabels)))
        }
        
        // Atypical UserAgents
        if(UADisabled<2) // if not fully disabled
        if(userAgentSavedHistogram.histSize< 10){
          HogHBaseHistogram.saveHistogram(Histograms.merge(userAgentSavedHistogram, new HogHistogram("",totalUserAgents,userAgentHistogram)))
        }else{
              val atypicalUserAgents   = Histograms.atypical(userAgentSavedHistogram.histMap, userAgentHistogram)

              if(atypicalUserAgents.size>0 & userAgentSavedHistogram.histMap.filter({case (key,value) => value > 0.001D}).size <5)
              {
                val atypicalAccess = hashSet.filter({case tuple => atypicalUserAgents.contains(tuple._8) && 
                                                                   ! UAexcludedCities.contains(tuple._11) &&
                                                           ! UAReverseDomainsWhitelist
                                                              .map { domain => tuple._4.endsWith(domain) }
                                                              .contains(true) })
                if(atypicalAccess.size>0) {
                    //println("UserName: "+userName+ " - Atypical access UserAgent: "+atypicalAccess.map(_._8).mkString(","))
    
                    val flowMap: Map[String,String] = new HashMap[String,String]
                    flowMap.put("flow:id",System.currentTimeMillis.toString)
                    val event = new HogEvent(new HogFlow(flowMap,atypicalAccess.head._5,atypicalAccess.head._2))
                                 
                    event.data.put("userName", userName) 
                    event.data.put("atypicalUserAgents", atypicalAccess.map(_._8).mkString(","))          
                    event.data.put("accessLogs",authTupleToString(atypicalAccess))    
                    event.data.put("coords",atypicalAccess.map(_._12).head)
                    
                    if(UADisabled<1)
                    populateAtypicalAccessUserAgent(event).alert()
                }
              }
              HogHBaseHistogram.saveHistogram(Histograms.merge(userAgentSavedHistogram, new HogHistogram("",totalUserAgents,userAgentHistogram)))
        }
        
        // Atypical Server/Service
        if(serviceDisabled<2) // if not fully disabled
        if(servicesSavedHistogram.histSize< 10){
          HogHBaseHistogram.saveHistogram(Histograms.merge(servicesSavedHistogram, new HogHistogram("",totalServices,servicesHistogram)))
        }else{
              val atypicalServices   = Histograms.atypical(servicesSavedHistogram.histMap, servicesHistogram)

              if(atypicalServices.size>0 & servicesSavedHistogram.histMap.filter({case (key,value) => value > 0.001D}).size <5)
              {
                val atypicalAccess = hashSet.filter({case tuple => atypicalServices.contains(tuple._2.replace(" ", "_").trim()+"/"+tuple._3.replace(" ", "_").trim()) && 
                                                                   ! systemExcludedCities.contains(tuple._11) &&
                                                                   ! systemReverseDomainsWhitelist
                                                                      .map { domain => tuple._4.endsWith(domain) }
                                                                      .contains(true)})
                if(atypicalAccess.size>0) {

                    //println("UserName: "+userName+ " - Atypical access services: "+atypicalServices.mkString(","))
    
                    val flowMap: Map[String,String] = new HashMap[String,String]
                    flowMap.put("flow:id",System.currentTimeMillis.toString)
                    val event = new HogEvent(new HogFlow(flowMap,atypicalAccess.head._5,atypicalAccess.head._2))
                                 
                    event.data.put("userName", userName) 
                    event.data.put("atypicalServices", atypicalServices.mkString(","))          
                    event.data.put("accessLogs",authTupleToString(atypicalAccess))    
                    event.data.put("coords",atypicalAccess.map(_._12).head)
                    
                    if(serviceDisabled<1)
                      populateAtypicalAccessService(event).alert()
                }
              }
              HogHBaseHistogram.saveHistogram(Histograms.merge(servicesSavedHistogram, new HogHistogram("",totalServices,servicesHistogram)))
        }

  }

  return summary1;
  

  
  }

}