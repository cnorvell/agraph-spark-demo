package ai.vital.mldemo.naivebayes

import java.util.ArrayList
import java.util.HashSet
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.asScalaSet
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
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import com.typesafe.config.Config
import ai.vital.mldemo.common.AbstractJob
import au.com.bytecode.opencsv.CSVReader
import java.io.StringReader
import spark.jobserver.NamedRddSupport

object NaiveBayesTraining extends AbstractJob with NamedRddSupport {

  def MIN_DF = 1
  
	def MAX_DF_PERCENT = 100

  def getJobClassName(): String = {
    return NaiveBayesTraining.getClass.getCanonicalName
  }
  
  def getJobName(): String = {
      return "vital naive bayes training"
  }
  
  
  val inputNameOption = new Option("in", "input-name", true, "input RDD[(String, String, String)] (gid,newsgroup,text) name")
  inputNameOption.setRequired(true)
  
  val outputOption = new Option("mod", "model", true, "output model path (directory)")
  outputOption.setRequired(true)
  
  val overwriteOption = new Option("ow", "overwrite", false, "overwrite model if exists")
  overwriteOption.setRequired(false)
  
  val minDFOption = new Option("minDF", "minimumDocFreq", true, "minimum term document frequency, default: " + MIN_DF)
  minDFOption.setRequired(false)
  
  val maxDFPercentOption = new Option("maxDFP", "maxDocFreqPercent", true, "maximum term document frequency (percent), default: " + MAX_DF_PERCENT)
  maxDFPercentOption.setRequired(false)
  

  
  def getOptions(): Options = {
    return new Options()
      .addOption(masterOption)
      .addOption(inputNameOption)
      .addOption(outputOption)
      .addOption(overwriteOption)
      .addOption(minDFOption)
      .addOption(maxDFPercentOption)
  }
  
  def main(args: Array[String]): Unit = {
    _mainImpl(args)
  }
  
  def runJob(sc: SparkContext, jobConfig: Config): Any = {
    
    val inputName = jobConfig.getString(inputNameOption.getLongOpt)
    val outputModelPath = new Path(jobConfig.getString(outputOption.getLongOpt))
    val overwrite = jobConfig.getBoolean(overwriteOption.getLongOpt)

    var minDF = MIN_DF
    var maxDFPercent = MAX_DF_PERCENT
    if(jobConfig.entrySet().contains(minDFOption.getLongOpt)) {
      minDF = Integer.parseInt(jobConfig.getString(minDFOption.getLongOpt))
    } 
    if(jobConfig.entrySet().contains(maxDFPercentOption.getLongOpt)) {
      maxDFPercent = Integer.parseInt(jobConfig.getString(maxDFPercentOption.getLongOpt))
    }
    
    if(minDF < 1) {
      System.err.println("minDF must be > 0")
      return
    }
    
    if(maxDFPercent > 100 || maxDFPercent < 1) {
      System.err.println("maxDFPercent must be within range [1, 100]")
      return
    }
    
    
    
    println("input train name: " + inputName)
    println("output model path: " + outputModelPath)
    println("overwrite if exists: " + overwrite)
    println("minDF: " + minDF)
    println("maxDFPercent: " + maxDFPercent)

    val modelFS = FileSystem.get(outputModelPath.toUri(), new Configuration())

    
    if (modelFS.exists(outputModelPath) && !overwrite) {
      System.err.println("Output model path already exists, use -ow option")
      return
    }
    
    //gid, newsgroup, text
    var inputRDD : RDD[(String, String, String)] = null
    
    
    if(inputName.startsWith("path:")) {
      
      println("test mode - input path: " + inputName)
      
      val inputPath = new Path(inputName.substring(5))
      
      val inputFS = FileSystem.get(inputPath.toUri(), new Configuration())
      
    	if (!inputFS.exists(inputPath) || !inputFS.isDirectory(inputPath)) {
    		System.err.println("Input train path does not exist or is not a directory: " + inputName)
    		return
    	}

      
      val csvRDD = sc.textFile(inputPath.toString())
      
      inputRDD = csvRDD.map { line => 
           val reader = new CSVReader(new StringReader(line), ',', '"', '\\')
           val record = reader.readNext()
           (record(0), record(1), record(2))
      }

      //cache it only if it's not a named RDD? 
      inputRDD.cache()
      
    } else {
      
      inputRDD = this.namedRdds.get[(String, String, String)](inputName).get
      
      
    }

    
    val splits = inputRDD.randomSplit(Array(0.6, 0.4), seed = 11L)
    
    val trainRDD = splits(0)
    
    val testRDD = splits(1)
    
    
    //filename->category | message -> extracted text content

    val docsCount = trainRDD.count();
    
    println("Documents count: " + docsCount)
    
    val maxDF = docsCount * maxDFPercent / 100
    
    println("MaxDF: " + maxDF)
    
    //collect distinct categories
    val categoriesRDD: RDD[String] = trainRDD.map { gidNewsgroupText =>

      gidNewsgroupText._2

    }.distinct()

    val categories: Array[String] = categoriesRDD.toArray().sortWith((s1, s2) => s1.compareTo(s2) < 0)

    println("categories count: " + categories.size)

    val wordsRDD: RDD[String] = trainRDD.flatMap { gidNewsgroupText =>

      var l = new HashSet[String]()

      for (x <- gidNewsgroupText._3.toLowerCase().split("\\s+")) {
        if (x.isEmpty()) {

        } else {
          l.add(x)
        }
      }

      l.toSeq

    }
    
    val categoriesOS = modelFS.create(new Path(outputModelPath, "categories.tsv"), true)
    
    var i = 0
    for(c <- categories) {
      categoriesOS.write( (i + "\t" + c + "\n").getBytes("UTF-8") )
      i += 1
    }
    categoriesOS.close()

    //wordFrequencies
    val wordsOccurences = wordsRDD.map(x => (x, 1)).reduceByKey((i1, i2) => i1 + i2).filter(wordc => wordc._2 > 1)

    var dictionary = new java.util.HashMap[String, Int]()

    var docFreq = new java.util.ArrayList[String]()

    var dicList = new java.util.ArrayList[String]()

    var l = wordsOccurences.toLocalIterator.toList

    l.sortBy(e => e._2)

    var counter = 0

    val dictionaryFilePath = new Path(outputModelPath, "dictionary.tsv")
    
    var dictionaryOS = modelFS.create(dictionaryFilePath, true)
    
    for (e <- l) {
      
      if(e._2 >= minDF && e._2 <= maxDF) {
        
    	  dictionary.put(e._1, counter)
        
        dictionaryOS.write( (counter + "\t" + e._1 + "\n").getBytes("UTF-8") )
        
    	  counter += 1
        
      }
      
      

    }
    
    dictionaryOS.close()

    val comp = new java.util.Comparator[String]() {
      def compare(s1: String, s2: String): Int = {
        return Integer.parseInt(s2.split("\\s+")(1)).compareTo(Integer.parseInt(s1.split("\\s+")(1)));
      }
    }

    java.util.Collections.sort(docFreq, comp)

    
    val vectorized = trainRDD.map { gidNewsgroupText =>

      val catgoryID: Double = categories.indexOf(gidNewsgroupText._2);

      var index2Value: Map[Int, Double] = Map[Int, Double]()

      val words = gidNewsgroupText._3.toLowerCase().split("\\s+")

      for (x <- words) {

        val index = dictionary.getOrElse(x, -1)

        if (index >= 0) {

          var v = index2Value.getOrElse(index, 0D);
          v = v + 1
          index2Value += (index -> v)

        }

      }

      val s = index2Value.toSeq.sortWith({ (p1, p2) =>
        p1._1 < p2._1
      })

      LabeledPoint(catgoryID, Vectors.sparse(dictionary.size, s))

    }

//    println("Training dataset size: " + vectorized.count());

    println("Training model...")
    
    val model = NaiveBayes.train(vectorized, lambda = 1.0)

    println("Model trained, persisting...")

    val modelBinPath = new Path(outputModelPath, "model.bin")

    val os = modelFS.create(modelBinPath, true)

    SerializationUtils.serialize(model, os)

    
    os.close()
    
    
    println("Testing ...")
    
    println("Test documents count: " + testRDD.count())
    
    val vectorizedTest = testRDD.map { gidNewsgroupText =>

      val catgoryID: Double = categories.indexOf(gidNewsgroupText._2);

      var index2Value: Map[Int, Double] = Map[Int, Double]()

      val words = gidNewsgroupText._3.toLowerCase().split("\\s+")

      for (x <- words) {

        val index = dictionary.getOrElse(x, -1)

        if (index >= 0) {

          var v = index2Value.getOrElse(index, 0D);
          v = v + 1
          index2Value += (index -> v)

        }

      }

      val s = index2Value.toSeq.sortWith({ (p1, p2) =>
        p1._1 < p2._1
      })

      LabeledPoint(catgoryID, Vectors.sparse(dictionary.size, s))

    }
    
    
    val predictionAndLabel = vectorizedTest.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / vectorizedTest.count()

    println("Accuracy: " + accuracy)
    
    

    println("DONE")

  }
  
  
}