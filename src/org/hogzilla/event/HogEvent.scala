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

package org.hogzilla.event

import java.util.HashMap
import java.util.Map
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.hogzilla.hbase.HogHBaseRDD
import org.hogzilla.util.HogFlow
import java.net.InetAddress


class HogEvent(flow:HogFlow) 
{
	var sensorid:Int=0
	var signature_id:Double=0
	var priorityid:Int=0
	var text:String=""
	var data:Map[String,String]=new HashMap()
  var ports:String=""
  var title:String=""
 
  
  def formatIPtoBytes(ip:String):Array[Byte] =
  {
    try {
       // Eca! Snorby doesn't support IPv6 yet. See https://github.com/Snorby/snorby/issues/65
    if(ip.contains(":"))
      InetAddress.getByName("255.255.6.6").getAddress
    else  
      InetAddress.getByName(ip).getAddress
    } catch {
      case t: Throwable => 
        // Bogus address!
        InetAddress.getByName("255.255.1.1").getAddress
    }   
   
  }

  
   def alert()
   {
	   val put = new Put(Bytes.toBytes(flow.get("flow:id")))
     put.add(Bytes.toBytes("event"), Bytes.toBytes("note"), Bytes.toBytes(text))
     put.add(Bytes.toBytes("event"), Bytes.toBytes("lower_ip"), formatIPtoBytes(flow.lower_ip))
     put.add(Bytes.toBytes("event"), Bytes.toBytes("upper_ip"), formatIPtoBytes(flow.upper_ip))
     put.add(Bytes.toBytes("event"), Bytes.toBytes("lower_ip_str"), Bytes.toBytes(flow.lower_ip))
     put.add(Bytes.toBytes("event"), Bytes.toBytes("upper_ip_str"), Bytes.toBytes(flow.upper_ip))
     put.add(Bytes.toBytes("event"), Bytes.toBytes("signature_id"), Bytes.toBytes("%.0f".format(signature_id)))
     put.add(Bytes.toBytes("event"), Bytes.toBytes("time"), Bytes.toBytes(System.currentTimeMillis))
     put.add(Bytes.toBytes("event"), Bytes.toBytes("ports"), Bytes.toBytes(ports))
     put.add(Bytes.toBytes("event"), Bytes.toBytes("title"), Bytes.toBytes(title))
     HogHBaseRDD.hogzilla_events.put(put)

     //println(f"ALERT: $text%100s\n\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
   }
}

