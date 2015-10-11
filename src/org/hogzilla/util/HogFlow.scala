package org.hogzilla.util

import java.util.Map



/**
 * @author pa
 */
case class HogFlow(map:Map[String,String],lower_ip:Array[Byte],upper_ip:Array[Byte]) {
  
  
  
  def get(key:String):String =
  {
    map.get(key)
  }
}