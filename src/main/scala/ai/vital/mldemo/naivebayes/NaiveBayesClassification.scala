package ai.vital.mldemo.naivebayes

import java.util.LinkedHashMap
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.mapAsScalaMap
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.io.IOUtils
import org.apache.commons.lang.SerializationUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.linalg.Vectors
import com.typesafe.config.Config
import ai.vital.mldemo.common.AbstractJob
import spark.jobserver.NamedRddSupport
import org.apache.spark.rdd.RDD
import au.com.bytecode.opencsv.CSVReader
import au.com.bytecode.opencsv.CSVWriter
import java.io.StringReader
import java.io.StringWriter
import ai.vital.twentynews.TwentyNewsDataToNQuads

object NaiveBayesClassification extends AbstractJob with NamedRddSupport {

  def getJobClassName(): String = {
     return NaiveBayesClassification.getClass.getCanonicalName
  }

  def getJobName(): String = {
     return "vital naive bayes classification"
  }

  val modelOption = new Option("mod", "model", true, "input model path (directory)")
  modelOption.setRequired(true)
  
  val inputNameOption = new Option("in", "input-name", true, "input RDD[(String, String, String)] (gid,newsgroup,text) name")
  inputNameOption.setRequired(true)
  
  val outputNameOption = new Option("out", "output-name", true, "output RDD[(String, String)] (gid, categoryquads) name")
  outputNameOption.setRequired(true)
  

  
  def getOptions(): Options = {
    return new Options()
      .addOption(masterOption)
      .addOption(modelOption)
      .addOption(inputNameOption)
      .addOption(outputNameOption)
  }
  
  def main(args: Array[String]): Unit = {
    _mainImpl(args)
  }
  
  def runJob(sc: SparkContext, jobConfig: Config): Any = {
    
    val modelPath = new Path(jobConfig.getString(modelOption.getLongOpt))
    val inputRDDName = jobConfig.getString(inputNameOption.getLongOpt)
    val outputRDDName = jobConfig.getString(outputNameOption.getLongOpt)
    
    println("Model path     : " + modelPath)
    println("Input RDD name : " + inputRDDName)
    println("Output RDD name: " + outputRDDName)
    
    var inputRDD : RDD[(String,String, String)] = null
    
    if(inputRDDName.startsWith("path:")) {
      
      println("test mode - input path: " + inputRDDName)
      
      val inputPath = new Path(inputRDDName.substring(5))
      
      val inputFS = FileSystem.get(inputPath.toUri(), new Configuration())
      
      if (!inputFS.exists(inputPath) || !inputFS.isDirectory(inputPath)) {
        System.err.println("Input train path does not exist or is not a directory: " + inputRDDName)
        return
      }

      
      val csvRDD = sc.textFile(inputPath.toString())
      
      inputRDD = csvRDD.map { line => 
           val reader = new CSVReader(new StringReader(line), ',', '"', '\\')
           val record = reader.readNext()
           ( record(0), record(1), record(2) )
      }

      //cache it only if it's not a named RDD? 
      inputRDD.cache()
      
    } else {
      
      inputRDD = this.namedRdds.get[(String, String, String)](inputRDDName).get
      
      
    }
    
    
    val modelFS = FileSystem.get(modelPath.toUri(), new Configuration())
    
    println("Loading model files from path: " + modelPath.toString())
    
    val categoriesPath = new Path(modelPath, "categories.tsv");
    val dictionaryPath = new Path(modelPath, "dictionary.tsv");
    val modelBinPath = new Path(modelPath, "model.bin");
    
    if(!modelFS.exists(modelPath) || !modelFS.isDirectory(modelPath)) {
      throw new RuntimeException("Model path does not exist or is not a directory: " + modelPath)
    }
    
    if(!modelFS.exists(categoriesPath) || !modelFS.isFile(categoriesPath)) {
      throw new RuntimeException("Categories file does not exist or is not a file: " + categoriesPath);
    }
    
    if(!modelFS.exists(dictionaryPath) || !modelFS.isFile(dictionaryPath)) {
    	throw new RuntimeException("Dictionary file does not exist or is not a file: " + dictionaryPath);
    }
    
    if(!modelFS.exists(modelBinPath) || !modelFS.isFile(modelBinPath)) {
    	throw new RuntimeException("Model binary file does not exist or is not a file: " + modelBinPath);
    }
    
    val categoriesMap = new LinkedHashMap[Int, String]()
    val dictionaryMap = new LinkedHashMap[String, Int]()
    
    val categoriesIS = modelFS.open(categoriesPath)
    for(catLine <- IOUtils.readLines(categoriesIS, "UTF-8")) {
      if(!catLine.isEmpty()) {
    	  val s = catLine.split("\t")
        categoriesMap.put(Integer.parseInt(s(0)), s(1))
      }
    }
    categoriesIS.close()
    
    println("Categories loaded: " + categoriesMap.size() + " : " + categoriesMap)
    
    val dictionaryIS = modelFS.open(dictionaryPath)
    for(dictLine <- IOUtils.readLines(dictionaryIS, "UTF-8")) {
      if(!dictLine.isEmpty()) {
        val s = dictLine.split("\t")
        dictionaryMap.put(s(1), Integer.parseInt(s(0)))
      }
    }
    dictionaryIS.close()
    
    println("Dictionary size: " + dictionaryMap.size())
    
    var modelIS = modelFS.open(modelBinPath)
    val deserializedModel = SerializationUtils.deserialize(modelIS)
    modelIS.close()
    
    val model : NaiveBayesModel = deserializedModel match {
      case x: NaiveBayesModel => x
      case _ => throw new ClassCastException
    }

     
    val vectorized = inputRDD.map { gidNewsgroupText =>

      var index2Value: Map[Int, Double] = Map[Int, Double]()

      val words = gidNewsgroupText._3.toLowerCase().split("\\s+")

      for (x <- words) {

        val index = dictionaryMap.getOrElse(x, -1)

        if (index >= 0) {

          var v = index2Value.getOrElse(index, 0D);
          v = v + 1
          index2Value += (index -> v)

        }

      }

      val s = index2Value.toSeq.sortWith({ (p1, p2) =>
        p1._1 < p2._1
      })

      (gidNewsgroupText._1, gidNewsgroupText._2, Vectors.sparse(dictionaryMap.size, s))

    }
    
    
    val hasCategoryURI = TwentyNewsDataToNQuads.hasCategory.stringValue()
    
    
    val gidOutQuadsRDD = vectorized.map { p =>
      
      val category = model.predict(p._3)
      
      val label = categoriesMap.get(category.intValue())
      
      //n-triple in the short term
      val catQuad = "<" + p._1 + "> <" + hasCategoryURI + "> \"" + label + "\" . \n" 
      
      (p._1, catQuad)
      
    }
    
    
    if(outputRDDName.startsWith("path:")) {
      println("TEST MODE - output path: " + outputRDDName)
      val csvRDD = gidOutQuadsRDD.map( tuple => {
        
        //persist as csv ?
        val sw = new StringWriter()
        val csvWriter= new CSVWriter(sw, ',', '"', '\\', "\n")
        csvWriter.writeNext(Array[String](tuple._1, tuple._2.replaceAll("\r?\n", "__NEWLINE__")))
        csvWriter.close()
        
        sw.toString().trim()
        
      })
      csvRDD.saveAsTextFile(outputRDDName.substring(5))
    } else {
        this.namedRdds.update(outputRDDName, gidOutQuadsRDD)
    }
    
  }

}