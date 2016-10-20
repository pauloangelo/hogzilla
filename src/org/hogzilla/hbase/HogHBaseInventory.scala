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
import org.hogzilla.cluster.HogClusterMember


object HogHBaseInventory {

 
 def deleteInventory(myIP:Int)=
 {
     val del = new Delete(Bytes.toBytes(myIP.toString))
     HogHBaseRDD.hogzilla_inventory.delete(del)
 }
 
 def saveInventory(myIP:String, opSystem:String) = {
   
    
     val put = new Put(Bytes.toBytes(myIP+"-"+opSystem))
     put.add(Bytes.toBytes("info"), Bytes.toBytes("ip"), Bytes.toBytes(myIP))
     put.add(Bytes.toBytes("info"), Bytes.toBytes("OS"), Bytes.toBytes(opSystem))
     
     HogHBaseRDD.hogzilla_inventory.put(put)
  }
 
  

}