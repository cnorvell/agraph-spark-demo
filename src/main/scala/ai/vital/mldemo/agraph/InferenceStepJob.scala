package ai.vital.mldemo.agraph

import ai.vital.mldemo.common.AbstractJob
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import java.io.StringReader
import java.io.StringWriter
import au.com.bytecode.opencsv.CSVReader
import au.com.bytecode.opencsv.CSVWriter
import spark.jobserver.NamedRddSupport
import org.apache.spark.rdd.RDD
import ai.vital.mldemo.reasoners.ReasonerHolder
import com.hp.hpl.jena.rdf.model.ModelFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

object InferenceStepJob extends AbstractJob with NamedRddSupport {
  
  val inputNameOption = new Option("in", "input-name", true, "input RDD[(String, String)] (gid,categoryNQuads) name")
  inputNameOption.setRequired(true)
  
  val outputNameOption = new Option("out", "output-name", true, "output RDD[(String, String)] (gid, moreCategoryNQuads) name")
  outputNameOption.setRequired(true)
  
  def ontologyOption = new Option("ont", "ontology", true, "ontology file path")
  ontologyOption.setRequired(true)
  
  def rulesOption = new Option("rules", "rules", true, "rules file path")
  rulesOption.setRequired(true)
  
  
  def getJobClassName(): String = {
    InferenceStepJob.getClass.getCanonicalName
  }

  def getJobName(): String = {
    "vital inference step job"
  }

  def getOptions(): Options = {
    new Options()
    .addOption(masterOption)
    .addOption(inputNameOption)
    .addOption(outputNameOption)
    .addOption(ontologyOption)
    .addOption(rulesOption)
  }

  def main(args: Array[String]): Unit = {
    
     _mainImpl(args)
     
   }
  
  def runJob(sc: SparkContext, jobConfig: Config): Any = {

    val inputRDDName = jobConfig.getString(inputNameOption.getLongOpt)
    val outputRDDName = jobConfig.getString(outputNameOption.getLongOpt)
    val ontology = jobConfig.getString(ontologyOption.getLongOpt)
    val rules = jobConfig.getString(rulesOption.getLongOpt)
    
    println("Input RDD name : " + inputRDDName)
    println("Output RDD name: " + outputRDDName)
    println("Ontology path  : " + ontology)
    println("Rules path     : " + rules)
    
    var inputRDD : RDD[(String,String)] = null
    
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
           ( record(0), record(1).replace("__NEWLINE__", "\n") )
      }

      //cache it only if it's not a named RDD? 
      inputRDD.cache()
      
    } else {
      
      inputRDD = this.namedRdds.get[(String, String)](inputRDDName).get
      
    }
    
    
    val gidMoreQuads = inputRDD.map( gidQuads => {
      

      val inputModel= ModelFactory.createDefaultModel()
      inputModel.read(new ByteArrayInputStream(gidQuads._2.getBytes("UTF-8")), null, "N-TRIPLE")
      
      val reasoner = ReasonerHolder.get(ontology, rules)
      
      val infmodel = ModelFactory.createInfModel(reasoner, inputModel);
      
      //output model contains the 
      val outputBA = new ByteArrayOutputStream()
      infmodel.write(outputBA, "N-TRIPLE")
      
      ( gidQuads._1, outputBA.toString("UTF-8") )
      
    })
    
    
    //persist the output
    
    if(outputRDDName.startsWith("path:")) {
      
      println("TEST MODE - output path: " + outputRDDName)
      val csvRDD = gidMoreQuads.map( tuple => {
        
        //persist as csv ?
        val sw = new StringWriter()
        val csvWriter= new CSVWriter(sw, ',', '"', '\\', "\n")
        csvWriter.writeNext(Array[String](tuple._1, tuple._2.replaceAll("\r?\n", "__NEWLINE__")))
        csvWriter.close()
        
        sw.toString().trim()
        
      })
      csvRDD.saveAsTextFile(outputRDDName.substring(5))
    } else {
        this.namedRdds.update(outputRDDName, gidMoreQuads)
    }
    

  }
}