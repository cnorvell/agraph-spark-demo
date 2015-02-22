package ai.vital.mldemo.agraph

import ai.vital.mldemo.common.AbstractJob
import com.franz.agraph.jena.AGGraphMaker
import com.franz.agraph.jena.AGModel
import com.franz.agraph.repository.AGRepositoryConnection
import com.typesafe.config.Config
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spark.jobserver.NamedRddSupport
import ai.vital.twentynews.TwentyNewsDataToNQuads
import au.com.bytecode.opencsv.CSVWriter
import java.io.OutputStreamWriter
import java.io.StringWriter
import org.openrdf.repository.RepositoryResult
import org.openrdf.model.Statement

object SelectModelFeaturesAgraphJob extends AbstractJob with NamedRddSupport {
  
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
  
  def inputNameOption = new Option("in", "input-name", true, "input RDD[String] (gid) name")
  inputNameOption.setRequired(true)
  
  def outputNameOption = new Option("out", "output-name", true, "output RDD[(String, String, String)] (gid,newsgroup,text) name")
  outputNameOption.setRequired(true)
  
  def getJobClassName(): String = {
    SelectModelFeaturesAgraphJob.getClass.getCanonicalName
  }

  def getJobName(): String = {
    "vital select model features agraph job"
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
    .addOption(outputNameOption)
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
    val inputRddName = jobConfig.getString(inputNameOption.getLongOpt)
    val outputRddName = jobConfig.getString(outputNameOption.getLongOpt)
    
    
    println("serverURL : " + serverURL)
    println("username  : " + username)
    println("password  : " + password)
    println("catalog   : " + catalog)
    println("repository: " + repository)
    println("inRDDName : " + inputRddName)
    println("outRDDName: " + outputRddName)
    
    if(catalog.equals("ROOT")) {
      catalog = ""
    }
    
    //load rdds
    
    var gidsRDD : RDD[String] = null
    
    if(inputRddName.startsWith("path:")) {

      println("TEST MODE - input path: " + inputRddName)
      
      gidsRDD = sc.textFile(inputRddName.substring(5))
      
    } else {
      
      gidsRDD = this.namedRdds.get[String](inputRddName).get
      
    }
    
    
    val hasSubjectURI = TwentyNewsDataToNQuads.hasSubject.stringValue()
   	val hasBodyURI = TwentyNewsDataToNQuads.hasBody.stringValue()
    val hasNewsgroupURI = TwentyNewsDataToNQuads.hasNewsgroup.stringValue()
    
    
    //gid, newsgroup, text tuple
    val featuresRDD = gidsRDD.map { gid =>
      
      var connection : AGRepositoryConnection = null
      
      var results : RepositoryResult[Statement] = null 
      
      var subject : String = null; 
      var body : String = null; 
      var newsgroup : String = null; 
      
      
      try {
        
        connection = AGContextHolder.getConnection(serverURL, username, password, catalog, repository, 1, 5)
        
        val contextURI = connection.getValueFactory.createURI(gid)
          
        results = connection.getStatements(null, null, null, false, contextURI)
          
        while(results.hasNext()) {
          val stmt = results.next()
          if(stmt.getSubject.equals(contextURI)) {
            val pURI = stmt.getPredicate.stringValue()
            if(pURI.equals(hasSubjectURI)) {
             subject = stmt.getObject.stringValue()
            } else if(pURI.equals(hasBodyURI)) {
              body = stmt.getObject.stringValue()
            } else if(pURI.equals(hasNewsgroupURI)) {
              newsgroup = stmt.getObject.stringValue()
            }
          } 
          
        }
        
      } finally {
        
        if(results != null) try { results.close() } catch {
          case ex: Exception => {
            
          }
        }
        
        if(connection != null) try { connection.close() } catch { 
         
          case ex: Exception => {
          }
          
        }
        
      }
      (gid, newsgroup, subject + " " + body)
      
    }
      
    println("Persisting gids RDD ...")
    
    if(outputRddName.startsWith("path:")) {
      println("TEST MODE - output path: " + outputRddName)
      val csvRDD = featuresRDD.map( triple => {
        
        //persist as csv ?
        val sw = new StringWriter()
        val csvWriter= new CSVWriter(sw, ',', '"', '\\', "\n")
        csvWriter.writeNext(Array[String](triple._1, triple._2, triple._3.replace("\n", " ")))
        csvWriter.close()
        
        sw.toString().trim()
        
      })
      csvRDD.saveAsTextFile(outputRddName.substring(5))
    } else {
        this.namedRdds.update(outputRddName, featuresRDD)
    }
    
  }
  
  
}