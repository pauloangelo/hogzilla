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

import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.collection.mutable.Set
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.hogzilla.histogram.HogHistogram
import org.hogzilla.histogram.Histograms
import org.apache.commons.lang3.StringUtils



object HogHBaseHistogram {

  def mapByResult(result:Result):(HashMap[String,Double],HashMap[String,String]) =
  {
  
   val map=new HashMap[String,Double]
   val mapLabels=new HashMap[String,String]

   if(!result.isEmpty())
   {
       val cells = result.listCells()
       
       val it = cells.iterator()
       while(it.hasNext())
       {
          val cell = it.next()
          
          val column = new String(CellUtil.cloneFamily(cell))
          val columnQualifier = new String(CellUtil.cloneQualifier(cell))
          val value = new String(CellUtil.cloneValue(cell))
          
          //println("Column: "+column+" ::"+value)
          
          if(column.equals("values"))
             map.put(columnQualifier,value.toDouble)
          else if (column.equals("labels")) {
             mapLabels.put(columnQualifier,value)
          }
       }  
    }
   
    (map,mapLabels)
  }
  
  def getHistogram(histName:String):HogHistogram =  
	{
	  
     val get1 = new Get(Bytes.toBytes(histName))
     
    val result = HogHBaseRDD.hogzilla_histograms.get(get1)  //getScanner(new Scan()).iterator()
    val tuple=mapByResult(result)
    val map=tuple._1
    val mapLabels=tuple._2
    
     if(!map.isEmpty)
     {
       //val histName = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
       val sizeArray = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("size"))
       if(sizeArray.length==0)
       {
         new HogHistogram(histName,0L,map,mapLabels)
       }
       else
       {
         new HogHistogram(histName,Bytes.toString(sizeArray).toLong,map,mapLabels)
       }
       
    }else
    {
      new HogHistogram(histName,0,map,mapLabels)
    }
	}
  
  
  //def saveHistogram(histName:String,size:Long,hist:Map[String,Double]) =
  def saveHistogram(hogHist:HogHistogram) =
  {
      val (histName,size,map,mapLabels) = (hogHist.histName, hogHist.histSize, hogHist.histMap, hogHist.histLabels)
    
      val put = new Put(Bytes.toBytes(histName))
      
      put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(histName))
      put.add(Bytes.toBytes("info"), Bytes.toBytes("size"), Bytes.toBytes(hogHist.histSize.toString()))
      
      map./:(0){ case (ac,(port,weight)) =>
                                put.add(Bytes.toBytes("values"), Bytes.toBytes(port), Bytes.toBytes(weight.toString()))
                                           0 }
      if(mapLabels!=null)
      mapLabels./:(0){ case (ac,(key,label)) =>
                                put.add(Bytes.toBytes("labels"), Bytes.toBytes(key), Bytes.toBytes(StringUtils.stripAccents(label).take(50)))
                                           0 }
     
      
      HogHBaseRDD.hogzilla_histograms.delete(new Delete(put.getRow))
      
      try {
        HogHBaseRDD.hogzilla_histograms.put(put)
      } catch {
        case t: Throwable => t.printStackTrace() 
        println(hogHist.histName)
        hogHist.histLabels.foreach(println(_))
        hogHist.histMap.foreach({case (key,map) => println(key+" => "+map.toString)})
        
      }
    
  }
  
  
  // Ex: FTP Servers
  def getIPListHIST01(spark: SparkContext,filterPort:String):scala.collection.immutable.Set[String] =
  {
    val table = "hogzilla_histograms"
    val conf = HBaseConfiguration.create()

    conf.set(TableInputFormat.INPUT_TABLE, table)
    conf.set("zookeeper.session.timeout", "600000")
    conf.setInt("hbase.client.scanner.timeout.period", 600000)
    
    
    val hBaseRDD = spark.newAPIHadoopRDD(conf, classOf[TableInputFormat],
                                                classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
                                                classOf[org.apache.hadoop.hbase.client.Result])

    hBaseRDD
    .map ({  case (id,result) => 
                 val port    = Bytes.toString(result.getValue(Bytes.toBytes("values"),Bytes.toBytes(filterPort)))
                 val name    = Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("name")))
                 val size    = Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("size")))
                 if(port==null || port.isEmpty())
                  (Histograms.getIPFromHistName(name),size,0D)
                 else
                  (Histograms.getIPFromHistName(name),size,port.toDouble)
        })
    .filter({case (ip,size,port) =>  port > Histograms.atypicalThreshold})
    .map({case (ip,size,port) => ip})
    .toArray()
    .toSet
  }

}