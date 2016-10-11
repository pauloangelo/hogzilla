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

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Delete


object HogHBaseCluster {

 def formatTitle(clusterCentroid: List[(Long,Double)], clusterIdx:Int):String =
 {
   val mainTitle = 
   "Cluster "+clusterIdx.toString+" - "+
   clusterCentroid
   .filter({case (port,rate) =>
            rate > 9.999
          })
   .map({case (port,rate) =>
            port.toString()+":"+"%.0f".format(rate)+"%"
        }).mkString(", ")
        
   val onePercentList=
   clusterCentroid
   .filter({case (port,rate) =>
            .9999 < rate & rate < 10
          })
          
   if(onePercentList.size>0)
   {
     mainTitle+", "+
     onePercentList.map({case (port,rate) =>
            port.toString()
        }).mkString("(",", ",")"+"> 1%")
     
   }else
   {
     mainTitle
   }
 }
 
 def deleteCluster(clusterIdx:Int)=
 {
     val del = new Delete(Bytes.toBytes(clusterIdx.toString))
     HogHBaseRDD.hogzilla_clusters.delete(del)
 }
 
 def saveCluster(clusterIdx:Int, clusterCentroid: List[(Long,Double)], clusterSize: Long, members:Array[String]) = {
   
     val memberString = members.mkString(",")
   
     val put = new Put(Bytes.toBytes(clusterIdx.toString))
     put.add(Bytes.toBytes("info"), Bytes.toBytes("title"), Bytes.toBytes(formatTitle(clusterCentroid,clusterIdx)))
     put.add(Bytes.toBytes("info"), Bytes.toBytes("size"), Bytes.toBytes(clusterSize.toString))
     put.add(Bytes.toBytes("info"), Bytes.toBytes("centroid"), Bytes.toBytes(clusterCentroid.mkString("[",",","]")))
     put.add(Bytes.toBytes("info"), Bytes.toBytes("members"), Bytes.toBytes(memberString))
     
     HogHBaseRDD.hogzilla_clusters.put(put)
  }
  

}