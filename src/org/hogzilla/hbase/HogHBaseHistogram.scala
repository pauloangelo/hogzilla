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
import scala.collection.mutable.HashSet
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.CellUtil
import org.hogzilla.histogram.HogHistogram
import breeze.stats.hist
import org.apache.hadoop.hbase.client.Result



object HogHBaseHistogram {

  def mapByResult(result:Result):HashMap[String,Double] =
  {
  
   val map=new HashMap[String,Double]

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
       }  
    }
   
    map
  }
  
  def getHistogram(histName:String):HogHistogram =  
	{
	  
     val get1 = new Get(Bytes.toBytes(histName))
     
    val result = HogHBaseRDD.hogzilla_histograms.get(get1)  //getScanner(new Scan()).iterator()
    val map=mapByResult(result)
    
     if(!map.isEmpty)
     {
       //val histName = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
       val sizeArray = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("size"))
       if(sizeArray.length==0)
       {
         new HogHistogram(histName,0L,map)
       }
       else
       {
         new HogHistogram(histName,Bytes.toString(sizeArray).toLong,map)
       }
       
    }else
    {
      new HogHistogram(histName,0,map)
    }
	}
  
  
  //def saveHistogram(histName:String,size:Long,hist:Map[String,Double]) =
  def saveHistogram(hogHist:HogHistogram) =
  {
    val (histName,size,map) = (hogHist.histName, hogHist.histSize, hogHist.histMap)
    
      val put = new Put(Bytes.toBytes(histName))
      
      put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(histName))
      put.add(Bytes.toBytes("info"), Bytes.toBytes("size"), Bytes.toBytes(hogHist.histSize.toString()))
      
      map./:(0){ case (ac,(port,weight)) =>
                                put.add(Bytes.toBytes("values"), Bytes.toBytes(port), Bytes.toBytes(weight.toString()))
                                           0 }
      
      
      HogHBaseRDD.hogzilla_histograms.put(put)
    
  }

}