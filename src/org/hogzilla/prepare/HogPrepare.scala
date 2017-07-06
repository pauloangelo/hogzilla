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

package org.hogzilla.prepare

import java.util.HashMap
import java.util.Map
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.hogzilla.hbase.HogHBaseRDD
import org.apache.hadoop.hbase.client.RowMutations
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.filter.BinaryComparator
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.CompareFilter


object HogPrepare {
  
  def prepare(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)])
  {
    println("Cleaning HBase...")
    cleanFlows(HogRDD)
    cleanSFlows(HogRDD)
    cleanAuthRecords(HogRDD)
  }
  
  def cleanFlows(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)])
  {
  
    
 /**
  * This is an illustration of the purge process in a fancy time-line.
  * 
  * 
  *        Sup1-denseTime       tSup1                         tSup2                          now
  *  old flows   |  dense period  |    training dirty period    |       don't touch           |      future    
  * ------------------------------------------------------------------------------------------------------------->
  *  remove all     remove all        Remove flows w/o events
  *                   in par           priority_id=1 in par
  *            
  *  You can change this, but the time below are reasonable
  *                   
  *  tSup2     = now - timeUnit    
  *  tSup1     = now - 100*timeUnit
  *  denseTime = 2*timeUnit
  *  
  *  24h = 86400000
  *  12h = 43200000
  *  06h = 21600000         
  */
    
    // Delete old data from HBase 86400 is one day. You should need even more, depends on your available resources.
    
    println("Cleaning hogzilla_flows...")
    val now = System.currentTimeMillis
    
    val timeUnit:Long = 21600000 /* maybe one day (86400000) or half (43200000) */
    val timeSuperior1 = now - (timeUnit*100)
    val timeSuperior2 = now - timeUnit
    val nSplits = 4 /* number of parallel tasks */
    val denseTime = timeUnit*4
    val deltaT1 = denseTime/nSplits
    val deltaT2 = (timeSuperior2-timeSuperior1)/nSplits
 
    println("Removing all older than "+timeSuperior1)
    val totalOld = (0 to nSplits).toList.par.map({ k => 
       
      val scan = new Scan
      
      if(k.equals(0))
        scan.setTimeRange(0, timeSuperior1-denseTime)
      else
        scan.setTimeRange(timeSuperior1-denseTime + deltaT1*(k-1), timeSuperior1-denseTime + deltaT1*k)
        
      
      println("TimeRange: "+scan.getTimeRange.toString())  
    
      val scanner = HogHBaseRDD.hogzilla_flows.getScanner(scan).iterator()
    
      var counter=0;
      while(scanner.hasNext())
      {
        HogHBaseRDD.hogzilla_flows.delete(new Delete(scanner.next().getRow))
        counter+=1
      }
      
      counter
    }).reduce( (a,b) => a+b)
    
    println("Old rows dropped: "+totalOld)
    
    println("Removing flows w/o events priority 1, which are between "+timeSuperior1+" and "+timeSuperior2)
    val totalWOEvent = (1 to nSplits).toList.par.map({ k => 
       
      val scan = new Scan
      val filter = new SingleColumnValueFilter(Bytes.toBytes("event"),
                                               Bytes.toBytes("priority_id"), 
                                               CompareOp.valueOf("NOT_EQUAL"),
                                               new BinaryComparator(Bytes.toBytes("1")))
      
      filter.setFilterIfMissing(false)
      
      scan.setTimeRange(timeSuperior1 + deltaT2*(k-1), timeSuperior1 + deltaT2*k)
      
      scan.setFilter(filter)
      
      println("TimeRange: "+scan.getTimeRange.toString())
    
      val scanner = HogHBaseRDD.hogzilla_flows.getScanner(scan).iterator()
    
      var counter=0;
      while(scanner.hasNext())
      {
        HogHBaseRDD.hogzilla_flows.delete(new Delete(scanner.next().getRow))
        counter+=1
      }
      counter
    }).reduce((a,b) => a+b)
    
    println("Flows without event priority 1 dropped: "+totalWOEvent)
    
    /*
   
   
     //scan.setStartRow(Bytes.toBytes("0"))
      //scan.setStopRow(Bytes.toBytes(time))
       * 
     //THIS CODE HAS BUGS
   
    // TODO HZ: Update flow:inter_time_stddev and flow:packet_size_stddev using "flow:inter_time-%d","flow:packet_size-%d"
   
        
    val prepareRDD = HogRDD.
        map { case (id,result) => {
          val map: Map[String,String] = new HashMap[String,String]
              map.put("flow:id",Bytes.toString(id.get).toString())
              HogHBaseRDD.columns.foreach { column => map.put(column, 
                  Bytes.toString(result.getValue(Bytes.toBytes(column.split(":")(0).toString()),Bytes.toBytes(column.split(":")(1).toString())))) 
        }
        map
        }
    }
    
    prepareRDD.filter(_.get("flow:packet_size_stddev").isEmpty()).map({
      map =>
        val avg=map.get("flow:avg_packet_size").toDouble
        var total:Double =0
        for(i <- 0 to map.get("flow:packets").toInt-1)
        {
          total=total+ (map.get("flow:packet_size-"+i.toString).toDouble-avg) * (map.get("flow:packet_size-"+i.toString).toDouble-avg)
        }
        // TODO HZ: Salve in HBase here
        // ID: map.get("flow:id")
        map.put("flow:packet_size_stddev",total.toString())
        
        val mutation = new RowMutations()
        val put = new Put(Bytes.toBytes(map.get("flow:id")))
        put.add(Bytes.toBytes("flow"), Bytes.toBytes("packet_size_stddev"), Bytes.toBytes(Math.sqrt(total)))
        mutation.add(put)
        HogHBaseRDD.hogzilla_flows.mutateRow(mutation)
    })
   */

  }
  
  
  
  def cleanSFlows(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)])
  {
  
    
 /**
  * This is an illustration of the purge process in a fancy time-line.
  * 
  * 
  *        Sup1-denseTime       tSup1                          now
  *  old flows   |  dense period  |       don't touch           |      future    
  * ------------------------------------------------------------------------------------------------->
  *  remove all     remove all        
  *                   in par          
  *            
  *  You can change this, but the time below are reasonable
  *                   
  *  tSup2     = now - timeUnit    
  *  tSup1     = now - 100*timeUnit
  *  denseTime = 2*timeUnit
  *  
  *  24h = 86400000
  *  12h = 43200000
  *  06h = 21600000         
  */
    
    // Delete old data from HBase 86400 is one day. You should need even more, depends on your available resources.
    
    println("Cleaning hogzilla_sflows...")
    val now = System.currentTimeMillis
    
    val timeUnit:Long = 21600000 /* maybe one day (86400000) or half (43200000) or quarter (21600000) */
    val timeSuperior1 = now - timeUnit
    val nSplits = 5 /* number of parallel tasks */
    val denseTime = timeUnit*1
    val deltaT1 = denseTime/nSplits
    //val deltaT2 = (timeSuperior2-timeSuperior1)/nSplits
 
    println("Removing all older than "+timeSuperior1)
    val totalOld = (0 to nSplits).toList.par.map({ k => 
       
      val scan = new Scan
      
      if(k.equals(0))
        scan.setTimeRange(0, timeSuperior1-denseTime)
      else
        scan.setTimeRange(timeSuperior1-denseTime + deltaT1*(k-1), timeSuperior1-denseTime + deltaT1*k)
        
      
      println("TimeRange: "+scan.getTimeRange.toString())  
    
      val scanner = HogHBaseRDD.hogzilla_sflows.getScanner(scan).iterator()
    
      var counter=0;
      while(scanner.hasNext())
      {
        HogHBaseRDD.hogzilla_sflows.delete(new Delete(scanner.next().getRow))
        counter+=1
      }
      
      counter
    }).reduce( (a,b) => a+b)
    
    println("Old rows dropped: "+totalOld)
        

  }
  
  
  
  
  
  def cleanAuthRecords(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)])
  {
  
    
    
    // Delete old data from HBase 86400 is one day. You should need even more, depends on your available resources.
    
    println("Cleaning hogzilla_authrecords...")
    val now = System.currentTimeMillis
    
    val timeUnit:Long = 21600000 /* maybe one day (86400000) or half (43200000) or quarter (21600000) */
    val timeSuperior1 = now - timeUnit
    val nSplits = 5 /* number of parallel tasks */
    val denseTime = timeUnit*1
    val deltaT1 = denseTime/nSplits
    //val deltaT2 = (timeSuperior2-timeSuperior1)/nSplits
 
    println("Removing all older than "+timeSuperior1)
    val totalOld = (0 to nSplits).toList.par.map({ k => 
       
      val scan = new Scan
      
      if(k.equals(0))
        scan.setTimeRange(0, timeSuperior1-denseTime)
      else
        scan.setTimeRange(timeSuperior1-denseTime + deltaT1*(k-1), timeSuperior1-denseTime + deltaT1*k)
        
      
      println("TimeRange: "+scan.getTimeRange.toString())  
    
      val scanner = HogHBaseRDD.hogzilla_authrecords.getScanner(scan).iterator()
    
      var counter=0;
      while(scanner.hasNext())
      {
        HogHBaseRDD.hogzilla_authrecords.delete(new Delete(scanner.next().getRow))
        counter+=1
      }
      
      counter
    }).reduce( (a,b) => a+b)
    
    println("Old rows dropped: "+totalOld)
        

  }
  
}