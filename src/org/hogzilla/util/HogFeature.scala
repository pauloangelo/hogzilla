package org.hogzilla.util

import scala.collection.immutable.HashMap



/**
 * @author pa
 */
case class HogFeature(name:String,ctype:String,useOnTrain:Boolean = true, isNumeric:Boolean = true /*or categorical*/, label:Int=0) {
 
  var index=0;
  var possibleCategoricalValues:Map[String,Int] = new HashMap;
  
  def getColumn1():String = {
    name.split(":")(0).toString()
  } 
  
  def getColumn2():String = {
    name.split(":")(1).toString()
  }
}