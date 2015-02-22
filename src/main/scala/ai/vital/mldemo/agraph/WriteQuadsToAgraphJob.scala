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
import au.com.bytecode.opencsv.CSVReader
import spark.jobserver.NamedRddSupport
import org.apache.spark.rdd.RDD
import com.franz.agraph.repository.AGRepositoryConnection
import org.openrdf.rio.RDFFormat

object WriteQuadsToAgraphJob extends AbstractJob with NamedRddSupport {
  
  
  def urlOption = new Option("url", "url", true, "AG server URL")
  urlOption.setRequired(true)
  
  def userOption = new Option("user", "user", true, "username")
  userOption.setRequired(true)
  
  def passwordOption = new Option("p", "password", true, "password")
  passwordOption.setRequired(true)
  
  def catalogOption = new Option("c", "catalog", true, "catalog, ROOT or name")
  catalogOption.setRequired(true)
  
  def repoOption = new Option("r", "repository", true, "repository")
  repoOption.setRequired(true)
  
  def inputNameOption = new Option("in", "input-name", true, "input RDD[(String, String)] (gid,categoryNQuads) name")
  inputNameOption.setRequired(true)
  
  
  def getJobClassName(): String = {
    WriteQuadsToAgraphJob.getClass.getCanonicalName
  }

  def getJobName(): String = {
    "vital write quads to agraph job"
  }

  def getOptions(): Options = {
    new Options()
    .addOption(masterOption)
    .addOption(urlOption)
    .addOption(userOption)
    .addOption(passwordOption)
    .addOption(catalogOption)
    .addOption(repoOption)
    .addOption(inputNameOption)
  }
  
  def main(args: Array[String]): Unit = {
    
     _mainImpl(args)
     
   }
  
  def runJob(sc: SparkContext, jobConfig: Config): Any = {

    val serverURL = jobConfig.getString(urlOption.getLongOpt)
    val username = jobConfig.getString(userOption.getLongOpt)
    val password = jobConfig.getString(passwordOption.getLongOpt)
    var catalog = jobConfig.getString(catalogOption.getLongOpt)
    val repository = jobConfig.getString(repoOption.getLongOpt)
    val inputRDDName = jobConfig.getString(inputNameOption.getLongOpt)
 
    println("serverURL : " + serverURL)
    println("username  : " + username)
    println("password  : " + password)
    println("catalog   : " + catalog)
    println("repository: " + repository)
    println("RDDName   : " + inputRDDName)
    
    if(catalog.equals("ROOT")) {
      catalog = ""
    }
    
    var inputRDD : RDD[(String, String)] = null
    
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

    } else {
      
      inputRDD = this.namedRdds.get[(String, String)](inputRDDName).get
      
    }
    
    val persisted = inputRDD.map( gid2NQuads => {
      
      //convert the ntriples to nquads actually
      var connection : AGRepositoryConnection = null
      
      var subject : String = null; 
      var body : String = null; 
      var newsgroup : String = null; 
      
      try {
        
        connection = AGContextHolder.getConnection(serverURL, username, password, catalog, repository, 1, 5)
        
        //iterate over 
        connection.add(new StringReader(gid2NQuads._2), null, RDFFormat.NTRIPLES, connection.getValueFactory.createURI(gid2NQuads._1))
        
      } finally {
        
        if(connection != null) try { connection.close() } catch { 
         
          case ex: Exception => {
          }
          
        }
        
      }
      
    }).count()
    

  }
}