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

import org.hogzilla.hbase.HogHBaseRDD
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put



/**
 * @author pa
 */
case class HogSignature(signature_class:Int, signature_name:String, signature_priority:Int, signature_revision:Int, signature_id:Double,signature_group_id:Int) {
  //Example: 3,"HZ: Suspicious DNS flow identified by K-Means clustering",2,1,826000001,826
  
  def saveHBase():HogSignature =
  {
    val get = new Get(Bytes.toBytes("%.0f".format(signature_id)))
    
    if(!HogHBaseRDD.hogzilla_sensor.exists(get))
    {
      val put = new Put(Bytes.toBytes("%.0f".format(signature_id)))
      put.add(Bytes.toBytes("signature"), Bytes.toBytes("id"), Bytes.toBytes("%.0f".format(signature_id)))
      put.add(Bytes.toBytes("signature"), Bytes.toBytes("class"), Bytes.toBytes(signature_class.toString()))
      put.add(Bytes.toBytes("signature"), Bytes.toBytes("name"), Bytes.toBytes(signature_name))
      put.add(Bytes.toBytes("signature"), Bytes.toBytes("priority"), Bytes.toBytes(signature_priority.toString()))
      put.add(Bytes.toBytes("signature"), Bytes.toBytes("revision"), Bytes.toBytes(signature_revision.toString()))
      put.add(Bytes.toBytes("signature"), Bytes.toBytes("group_id"), Bytes.toBytes(signature_group_id.toString()))
      HogHBaseRDD.hogzilla_signatures.put(put)
    }
    
    this
  }
}