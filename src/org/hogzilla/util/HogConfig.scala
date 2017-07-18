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
import com.typesafe.config.Config
import scala.collection.mutable.HashSet


/**
 * @author pa
 */
object HogConfig {
  
   
  
  def get(config:Config,key:String,valueType:String,default:Any):Any =
  {
    if(config==null)
      return default
    
      
    val value = config.getString(key)
    
    if(value.isEmpty())
      return default // Return default value
    else
    {
      try {
        
        println(f"Configuration: $key => $value")
       
        if(valueType.equals("Int"))
          value.toInt 
        else if(valueType.equals("Double"))
          value.toDouble 
        else if(valueType.equals("Long"))
          value.toLong 
        else if(valueType.equals("Set(Int)"))
        {
          val patternSet="Set\\(".r
          val patternSetEnd="\\)".r
          
          return (patternSetEnd replaceAllIn((patternSet replaceAllIn(value, "")),""))
                  .split(",").map({x => x.toInt}).toSet
        }
        else if(valueType.equals("Set(String)"))
        {
          val patternSet="Set\\(".r
          val patternSetEnd="\\)".r
          
          return (patternSetEnd replaceAllIn((patternSet replaceAllIn(value, "")),""))
                  .split(",").map({x => println(x.toString.trim()) ; x.toString.trim()}).toSet
        }
        else
          default // Create type first
          
      } catch {
        case t: Throwable =>//t.printStackTrace() 
        println(f"Problem parsing $key = $value . Check if it is ok. Using default value")
        
        return default
      } 
  
    }
  }
  
  def getInt(config:Config,key:String,default:Any):Int =
  {
    get(config,key,"Int",default).asInstanceOf[Int]
  }
  
  def getLong(config:Config,key:String,default:Any):Long =
  {
    get(config,key,"Long",default).asInstanceOf[Long]
  }
  
  def getDouble(config:Config,key:String,default:Any):Double =
  {
    get(config,key,"Double",default).asInstanceOf[Long]
  }
  
  def getSetInt(config:Config,key:String,default:Any):Set[Int] =
  {
    get(config,key,"Set(Int)",default).asInstanceOf[Set[Int]]
  }
  
  def getSetString(config:Config,key:String,default:Any):Set[String] =
  {
    get(config,key,"Set(String)",default).asInstanceOf[Set[String]]
  }
   

}