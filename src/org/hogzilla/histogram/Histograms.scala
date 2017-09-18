package org.hogzilla.histogram

import scala.collection.mutable.HashSet
import scala.collection.mutable.Map
import scala.collection.mutable.Set
import scala.math.log


/**
 * @author pa
 */

object Histograms {

  
  val atypicalThreshold = 0.0000001D
  
  def KullbackLiebler(histogram1:Map[String,Double],histogram2:Map[String,Double]):Double = 
  {
        
    val keys = histogram1.keySet ++ histogram2.keySet
    
    keys./:(0.0){ case (ac,key) =>
                  val p:Double = { if(histogram1.get(key).isEmpty) 0 else histogram1.get(key).get }
                  val q:Double = { if(histogram2.get(key).isEmpty) 0 else histogram2.get(key).get }
                  if(p==0)
                    ac
                  else
                  {
                    if(q==0)
                      ac + 0
                    else
                      ac + p*log(p/q)
                  }
                }
  }
  
  
  def atypical(histogram1:Map[String,Double],histogram2:Map[String,Double]):Set[String] = 
  {
    
    val ret = new HashSet[String] 
        
    val keys = histogram2.keySet
    
    keys./:(0.0){ case (ac,key) =>
                  val p:Double = { if(histogram1.get(key).isEmpty) 0 else histogram1.get(key).get }
                  val q:Double = { if(histogram2.get(key).isEmpty) 0 else histogram2.get(key).get }
                  if(p<atypicalThreshold && q>atypicalThreshold)
                  {
                   ret.add(key)
                   ac+1
                  }
                  else
                   0
                }
    
    ret
  }
  
  // Return typical events in histogram1 (main saved), which occurred in histogram2 (current)  
  def typical(histogram1:Map[String,Double],histogram2:Map[String,Double]):Set[String] = 
  {
    
    val ret = new HashSet[String] 
    
    val keys = histogram2.keySet
    
    keys./:(0.0){ case (ac,key) =>
                  val p:Double = { if(histogram1.get(key).isEmpty) 0 else histogram1.get(key).get }
                  val q:Double = { if(histogram2.get(key).isEmpty) 0 else histogram2.get(key).get }
                  if(p>atypicalThreshold && q>atypicalThreshold)
                  {
                   ret.add(key)
                   ac+1
                  }
                  else
                   0
                }
    
    ret
  }
  
  def isTypicalEvent(histogram1:Map[String,Double],event:String):Boolean= 
  {
    
    val p:Double = { if(histogram1.get(event).isEmpty) 0 else histogram1.get(event).get }
    if(p>atypicalThreshold)
    {
    	true
    }
    else
    	false
         
  }
  
  def isAtypicalEvent(histogram1:Map[String,Double],event:String):Boolean= 
  {    
    !isTypicalEvent(histogram1,event)
  }
  
  
   def merge(histogram1:HogHistogram,histogram2:HogHistogram):HogHistogram = 
  {
          
    val keys      = histogram1.histMap.keySet ++ histogram2.histMap.keySet
    val keysLabel = histogram1.histLabels.keySet ++ histogram2.histLabels.keySet
    var div:Double = 1
    if(histogram1.histSize.toDouble > 1000)
      div = 2
    
    keys./:(0.0){ case (ac,key) =>
                  val p:Double = { if(histogram1.histMap.get(key).isEmpty) 0 else histogram1.histMap.get(key).get }
                  val q:Double = { if(histogram2.histMap.get(key).isEmpty) 0 else histogram2.histMap.get(key).get }
                  
                  if(p>0 || q>0)
                  {
                	  val newp = (
                			  p*histogram1.histSize.toDouble/div+
                			  q*histogram2.histSize.toDouble

                			  )/(histogram1.histSize.toDouble/div+histogram2.histSize.toDouble)

                			  histogram1.histMap.put(key,newp)
                  }
                  0D
                }
    
    keysLabel./:(0.0){ case (ac,key) =>
                        if(histogram1.histLabels.get(key).isEmpty)
                           histogram1.histLabels.put(key,histogram2.histLabels.get(key).get)
                          
                        0D
                     }
    
    val total = histogram1.histSize/div+histogram2.histSize
    new HogHistogram(histogram1.histName,total.toInt,histogram1.histMap,histogram1.histLabels)
  }
   
  // It is not exactly a histogram, but... 
  def mergeMax(histogram1:HogHistogram,histogram2:HogHistogram):HogHistogram = 
  {
        
         
    val keys = histogram1.histMap.keySet ++ histogram2.histMap.keySet
    val keysLabel = histogram1.histLabels.keySet ++ histogram2.histLabels.keySet
    
    keys./:(0.0){ case (ac,key) =>
                  val p:Double = { if(histogram1.histMap.get(key).isEmpty) 0 else histogram1.histMap.get(key).get }
                  val q:Double = { if(histogram2.histMap.get(key).isEmpty) 0 else histogram2.histMap.get(key).get }
                  
                  if(p>0 || q>0)
                  {
                        histogram1.histMap.put(key,p.max(q))
                  }
                  0D
                }
    
     keysLabel./:(0.0){ case (ac,key) =>
                        if(histogram1.histLabels.get(key).isEmpty)
                           histogram1.histLabels.put(key,histogram2.histLabels.get(key).get)
                          
                        0D
                     }
    
    val total = histogram1.histSize+histogram2.histSize
    new HogHistogram(histogram1.histName,total,histogram1.histMap,histogram1.histLabels)
  }
  
   
  // hist1 - hist2
  def difference(histogram1:HogHistogram,histogram2:HogHistogram):HogHistogram = 
  {
         
    val keys = histogram2.histMap.keySet // ++ histogram2.histMap.keySet
    
    keys./:(0.0){ case (ac,key) =>
                  val p:Double = { if(histogram1.histMap.get(key).isEmpty) 0 else histogram1.histMap.get(key).get }
                  val q:Double = { if(histogram2.histMap.get(key).isEmpty) 0 else histogram2.histMap.get(key).get }
                  
                  if(p>0 || q>0)
                  {
                    val newp = (
                        
                        p*histogram1.histSize.toDouble-
                        q*histogram2.histSize.toDouble

                        )/(histogram1.histSize.toDouble-histogram2.histSize.toDouble)

                        histogram1.histMap.put(key,newp)
                  }
                  0D
                }
    
    val total = histogram1.histSize-histogram2.histSize
    new HogHistogram(histogram1.histName,total,histogram1.histMap,histogram1.histLabels)
  }
  
  
  def getIPFromHistName(histogramName:String):String =
  {
    histogramName.subSequence(histogramName.lastIndexOf("-")+1, histogramName.length()).toString
  }
  
  /*
  final val EPS = 1e-10
  
  type DATASET = Iterator[(Double, Double)]

  def execute( xy: DATASET, f: Double => Double): Double = {
    val z = xy.filter{ case(x, y) => abs(y) > EPS}
    - z./:(0.0){ case(s, (x, y)) => s + y*log(f(x)/y)}
  }

  def execute( xy: DATASET, fs: Iterable[Double=>Double]): Iterable[Double] = 
    fs.map(execute(xy, _))
    
   
 */
  
  
}