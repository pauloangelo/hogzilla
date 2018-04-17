
package org.hogzilla.snort

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.hogzilla.hbase.HogHBaseRDD
import org.hogzilla.util.HogFeature
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.rdd.PairRDDFunctions
import org.hogzilla.event.HogEvent
import org.hogzilla.util.HogFlow
import scala.collection.mutable.HashMap
import org.hogzilla.event.HogSignature




object HogSnort {
  
  val signature = ( HogSignature(3,"HZ: Suspicious flow detected by similarity with Snort alerts",2,1,826000001,826).saveHBase(), null )

  

  def run(HogRDD: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)],spark:SparkContext):String = {
    
     val excludedSigs:Array[(String,String)] = Array() // Ex. Array((1,1),(1,2))
     val (maxbin,maxdepth,mtry,malThreshold) = (500,30,90,80) 
  

    val sqlContext = new SQLContext(spark)
    import sqlContext.implicits._
     
                        
       val filteredColumns = HogHBaseRDD.columns.filter({_.useOnTrain}).toSeq
       val orderedColumns = filteredColumns.zip(0 to filteredColumns.size-1)
       
       
       val convertFn: PartialFunction[Any,Any] = { 
        case (column:HogFeature,value:String) => 
          try {
            
          if(column.ctype.equals("char"))
             value
          else if(column.ctype.equals("u_int64_t"))
             value.toLong
          else 
             value.toInt
          } catch {
              case t: Throwable => 
                //println("ERROR - column name: "+column.name)
                //t.printStackTrace()
              
              if(column.ctype.equals("char"))
                 ""
              else if(column.ctype.equals("u_int64_t"))
                 0L
              else 
                 0
              
          }
        }
    
    
      val labRDD2 = HogRDD.
      map { case (id,result) => {
        
            val rowId = Bytes.toString(id.get).toString()
            
            val ndpi_risk       = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("ndpi_risk")))
            val event_signature = Bytes.toString(result.getValue(Bytes.toBytes("event"),Bytes.toBytes("signature_id")))
            val event_generator = Bytes.toString(result.getValue(Bytes.toBytes("event"),Bytes.toBytes("generator_id")))
            val src_name        = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("src_name")))
            val dst_name        = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("dst_name")))
            val ctu_label       = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("ctu_label")))
            val duration        = Bytes.toString(result.getValue(Bytes.toBytes("flow"),Bytes.toBytes("flow_duration")))
            
            val actualclass = 1 // Not known at this time. Supposing all is an actual intrusion.

            val tuple = orderedColumns
                        .map ({ case (column,index) => 
                                    val ret = result.getValue(Bytes.toBytes(column.getColumn1()),Bytes.toBytes(column.getColumn2()))
                                    val value = Bytes.toString(ret)
                                    if(value==null||value.equals(""))
                                        (column,"-1") 
                                    else
                                        (column,value)
                              })
                              

            if(event_signature!=null && !event_signature.isEmpty() 
                && event_generator!=null && !event_generator.isEmpty())        
              (1,rowId,src_name,dst_name,actualclass,tuple,event_generator,event_signature,ndpi_risk)
            else if(ndpi_risk!=null && ( ndpi_risk.equals("Safe") || ndpi_risk.equals("Fun") ) )         
              (0,rowId,src_name,dst_name,actualclass,tuple,event_generator,event_signature,ndpi_risk) 
            else
              (-1,rowId,src_name,dst_name,actualclass,tuple,event_generator,event_signature,ndpi_risk) // discard
        }
    }
    
    val Signatures:PairRDDFunctions[(String,String),Long] = labRDD2
     .map({case (label,rowId,src_name,dst_name,actualclass,tuple,event_generator,event_signature,ndpi_risk) =>
           ((event_generator,event_signature),1L)
       })
      
    val Sarray = Signatures.reduceByKey(_+_).sortBy(_._2, false, 5).collect()  
    val Sarray_size = Sarray.size  
   
    // Print the found signatures. It may be useful to define what is FP and should be considered to be removed.
    Sarray.foreach({case ((gen,sig),count) => println(s"($gen,$sig) => $count")})
      


  val labRDD1 = labRDD2
     .map({case (label,rowId,src_name,dst_name,actualclass,tuple,event_generator,event_signature,ndpi_risk) =>
       
            if(event_signature!=null && !event_signature.isEmpty() 
                && event_generator!=null && !event_generator.isEmpty() && !excludedSigs.contains((event_generator,event_signature)))        
              (1,rowId,src_name,dst_name,actualclass,tuple,event_generator,event_signature,ndpi_risk)
            else if(ndpi_risk!=null && ( ndpi_risk.equals("Safe") || ndpi_risk.equals("Fun") ) )         
              (0,rowId,src_name,dst_name,actualclass,tuple,event_generator,event_signature,ndpi_risk) 
            else
              (-1,rowId,src_name,dst_name,actualclass,tuple,event_generator,event_signature,ndpi_risk) // discard
           
       })
     .map({case (label,rowId,src_name,dst_name,actualclass,tuple,event_generator,event_signature,ndpi_risk) =>
           Row.fromSeq({
                        Seq(label,rowId,src_name,dst_name,actualclass)++tuple.collect(convertFn)
                       })
           
       })
       
    val ccRDD    = labRDD1.filter { x => x.get(0) == 1 }
   // val cleanRDD = sqlContext.sparkContext.parallelize(labRDD1.filter { x => x.get(0) == 0 }.takeSample(false, 12000, 123L))   
    val cleanRDD    = labRDD1.filter { x => x.get(0) == 0 }

    val trainRDD = ccRDD++cleanRDD
    
       
    println("0: "+trainRDD.filter { x => x.get(0) == 0 }.count+" 1:"+trainRDD.filter { x => x.get(0) == 1 }.count)
      
   
   val rawFeaturesStructsSeq = orderedColumns.map({case (column,index) => 
                if(column.ctype.equals("char"))
                 StructField(column.name, DataTypes.StringType,true)
                else if(column.ctype.equals("u_int64_t"))
                 StructField(column.name, DataTypes.LongType,false)
                else 
                 StructField(column.name, DataTypes.IntegerType,false)
             })
       
   val dataScheme = new StructType(Array(StructField("label",    DataTypes.IntegerType,true),
                                         StructField("rowId",    DataTypes.StringType,true),
                                         StructField("src_name", DataTypes.StringType,true),
                                         StructField("dst_name", DataTypes.StringType,true),
                                         StructField("actual_class", DataTypes.IntegerType,true))
                                   ++ rawFeaturesStructsSeq)
 
     
  val data = sqlContext.createDataFrame(trainRDD, dataScheme).cache()
  val dataSize = data.count
  println("Sample size: "+dataSize)
    
        
  val dataOut = sqlContext.createDataFrame(labRDD1.filter { x => x.get(0).toString.toInt < 0 }, dataScheme).cache()
  val dataSizeOut = dataOut.count
  println("Sample size Out (not labelled): "+dataSizeOut)
    
  val  trainingData = data

   val stringIndexers =   Array(new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").setHandleInvalid("keep"))++
                          Array(new StringIndexer().setInputCol("actual_class").setOutputCol("indexedActual_class"))++
                          orderedColumns.filter({case (column,index) => column.ctype.equals("char") })
                                      .map({case (column,index) => new StringIndexer().setInputCol(column.name).setOutputCol(column.name+"CAT").setHandleInvalid("skip").fit(data) })
   
   val selectedFeaturesStringArray =  orderedColumns.map({case (column,index) => if(column.ctype.equals("char")) column.name+"CAT" else column.name }).toArray
   
   
   val assembler = new VectorAssembler()
                      .setInputCols(selectedFeaturesStringArray)
                      .setOutputCol("rawFeatures") 
                      
       

      val rf = new RandomForestClassifier()
          .setLabelCol("indexedLabel").setFeaturesCol("rawFeatures").setProbabilityCol("probabilities")
          .setNumTrees(100).setImpurity("gini").setPredictionCol("prediction").setRawPredictionCol("rawPrediction")
          .setMaxBins(maxbin).setMaxDepth(maxdepth).setFeatureSubsetStrategy(mtry.toString)
          .setThresholds(Array((100D-malThreshold.toDouble)/100D,malThreshold.toDouble/100D,0D))
      

      val pipeline = new Pipeline().setStages(stringIndexers++Array(assembler,rf))
      val model = pipeline.fit(trainingData)

      
     // val predictionsOut = model.transform(dataOut.union(testData))
      val predictionsOut = model.transform(dataOut)

                         
// ALERT
predictionsOut.filter( $"prediction" > 0 ) // prediction==1
              .select("src_name","dst_name","flow:src_port","flow:dst_port","prediction")
              .foreach({ row =>
                            val (src,dst,src_port,dst_port,predicted) = (row.get(0),row.get(1),row.get(2),row.get(3),row.get(4))
                            
                            val flowMap: scala.collection.mutable.Map[String,String] = new HashMap[String,String]
                            flowMap.put("flow:id",System.currentTimeMillis.toString)
                            val event = new HogEvent(new HogFlow(flowMap,src.toString,dst.toString))
                            
                            event.title = f"HZ: Suspicious flow detected by similarity with Snort alerts"
                                    
                            event.ports = ""
                                  
                            event.text = "This flow was detected by Hogzilla based on its similarities with Snort alerts.\n\n"+
                                         s"$src:$src_port -> $dst:$dst_port"                                             
                                              
                            event.signature_id = signature._1.signature_id       
                            println("")
                                 
                      })
                            
""

  }
  
    
}







