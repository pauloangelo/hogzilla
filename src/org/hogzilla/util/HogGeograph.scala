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

package org.hogzilla.util

import java.security.MessageDigest
import org.apache.hadoop.hbase.util.Bytes
import javax.xml.bind.DatatypeConverter
import math._


/**
 * @author pa
 */
object HogGeograph {
  
   val R = 6372.8 //radius in km
  
  def haversineDistance(lat1:Double, lon1:Double, lat2:Double, lon2:Double):Double =
  {
      val dLat=(lat2 - lat1).toRadians
      val dLon=(lon2 - lon1).toRadians

      val a = pow(sin(dLat/2),2) + pow(sin(dLon/2),2) * cos(lat1.toRadians) * cos(lat2.toRadians)
      val c = 2 * asin(sqrt(a))
      R * c    
  }
   
   
   def haversineDistanceFromStrings(coords1:String, coords2:String):Double =
	   {
		   try {
			   val coordsDouble1 = coords1.split(",").map({ x => x.toDouble })
				 val coordsDouble2 = coords2.split(",").map({ x => x.toDouble })

				 haversineDistance(coordsDouble1(0),coordsDouble1(1),coordsDouble2(0),coordsDouble2(1))
		   } catch {
		        case t: Throwable => // t.printStackTrace() 
             // Return a large distance 
             999999999D
		   }
	   }

}