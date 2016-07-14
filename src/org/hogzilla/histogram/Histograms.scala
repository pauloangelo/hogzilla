package org.hogzilla.histogram

import scala.collection.mutable.HashSet
import scala.collection.mutable.Map
import scala.collection.mutable.Set
import scala.math.log


/**
 * @author pa
 */

object Histograms {

  
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
    
    val atypicalThreshold = 0.00001D
        
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
  
  
   def merge(histogram1:HogHistogram,histogram2:HogHistogram):HogHistogram = 
  {
        
         
    val keys = histogram1.histMap.keySet ++ histogram2.histMap.keySet
    
    keys./:(0.0){ case (ac,key) =>
                  val p:Double = { if(histogram1.histMap.get(key).isEmpty) 0 else histogram1.histMap.get(key).get }
                  val q:Double = { if(histogram2.histMap.get(key).isEmpty) 0 else histogram2.histMap.get(key).get }
                  
                  if(p>0 || q>0)
                  {
                	  val newp = (
                			  p*histogram1.histSize.toDouble+
                			  q*histogram2.histSize.toDouble

                			  )/(histogram1.histSize.toDouble+histogram2.histSize.toDouble)

                			  histogram1.histMap.put(key,newp)
                  }
                  0D
                }
    
    val total = histogram1.histSize+histogram2.histSize
    new HogHistogram(histogram1.histName,total,histogram1.histMap)
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