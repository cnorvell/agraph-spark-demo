package ai.vital.mldemo.agraph

import ai.vital.mldemo.common.AbstractJob
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import com.franz.agraph.repository.AGRepositoryConnection
import org.apache.commons.io.IOUtils
import org.openrdf.model.Statement
import com.franz.openrdf.rio.nquads.NQuadsWriter
import java.io.StringWriter
import org.apache.spark.rdd.RDD
import ai.vital.mldemo.reasoners.ReasonerHolder
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.franz.agraph.jena.AGGraphMaker
import com.franz.agraph.jena.AGModel
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration

object AgraphJob extends AbstractJob {
  
  def getJobClassName(): String = {
    return AgraphJob.getClass.getCanonicalName
  }

  def getJobName(): String = {
    return "Agraph worker test"
  }

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
  
  
  def inputOption = new Option("i", "input", true, "input graph ids txt file")
  inputOption.setRequired(true)
  
  def outputOption = new Option("o", "output", true, "output nquads file")
  outputOption.setRequired(true)
  
  def ontologyOption = new Option("ont", "ontology", true, "ontology file path")
  ontologyOption.setRequired(true)
  
  def rulesOption = new Option("rules", "rules", true, "rules file path")
  rulesOption.setRequired(true)
  
  def overwriteOption = new Option("ow", "overwrite", false, "overwrite output file if exists")
  overwriteOption.setRequired(false)
  
  def getOptions(): Options = {
    return new Options()
    .addOption(masterOption)
    .addOption(urlOption)
    .addOption(userOption)
    .addOption(passwordOption)
    .addOption(catalogOption)
    .addOption(repoOption)
    .addOption(inputOption)
    .addOption(outputOption)
    .addOption(overwriteOption)
    .addOption(ontologyOption)
    .addOption(rulesOption)
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
    
    val ontology = jobConfig.getString(ontologyOption.getLongOpt)
    val rules = jobConfig.getString(rulesOption.getLongOpt)

    if(catalog.equals("ROOT")) {
    	catalog = ""
    }
    
    val input = jobConfig.getString(inputOption.getLongOpt)
    
    val output = jobConfig.getString(outputOption.getLongOpt)
    val overwrite= jobConfig.getBoolean(overwriteOption.getLongOpt)
    
    
    
    println("serverURL : " + serverURL)
    println("username  : " + username)
    println("password  : " + password)
    println("catalog   : " + catalog)
    println("repository: " + repository)
    println("input gids: " + input)
    println("output    : " + output)
    println("overwrite : " + overwrite)
    println("ontology  : " + ontology)
    println("rules     : " + rules)
    
    val outputPath = new Path(output)
    val outputFS = FileSystem.get(outputPath.toUri(), new Configuration())
    
    if(outputFS.exists(outputPath)) {
      if(!overwrite) {
        throw new RuntimeException("output path already exist, use --overwrite option: " + outputPath)
      }
      println("Output path will be overwritten: " + outputPath)
    }
    
    val inputRDD = sc.textFile(input)
    
    val outputRDD = inputRDD.map { gid => 

      println("Getting: " + gid)
    
      var maker : AGGraphMaker = null
      var connection : AGRepositoryConnection = null

      val sw = new StringWriter()
      
      try {

    	  val reasoner = ReasonerHolder.get(ontology, rules)
        
        connection = AGContextHolder.getConnection(serverURL, username, password, catalog, repository, 2, 2)
        
        maker = new AGGraphMaker(connection)
        
        val graph = maker.openGraph(gid)
        
        val dataModel = new AGModel(graph)
        
        
//        for( stmt <- m.listStatements().toList().toArray()) {
//          
//        	val stmtO = stmt.asInstanceOf[com.hp.hpl.jena.rdf.model.Statement]
//          
//          println(stmtO)
//          
//        }

        val infmodel = ModelFactory.createInfModel(reasoner, dataModel);
        

        val diff = infmodel.difference(dataModel)
        
        var first = true
        
        for( stmt <- diff.listStatements().toList().toArray()) {
          
          val stmtO = stmt.asInstanceOf[com.hp.hpl.jena.rdf.model.Statement]
          
    		  if(first) {
     			  first = false
     		  } else {
    			  sw.append("\n")
    		  }
          
          sw.append(stmtO.toString());
          
          
        }
        
        
//        val w = new NQuadsWriter(sw)
//        
//        w.startRDF()
//        
//        for( stmt <- diff.listStatements().toList().toArray()) {
//          
//          val stmtO = stmt.asInstanceOf[com.hp.hpl.jena.rdf.model.Statement]
//          
//          println(stmtO)
//          
//        }
//        
//        w.endRDF()
        
        
        
        
        /*
        val context = connection.getValueFactory.createURI(gid)
        
        val statements = connection.getStatements(null,null,null, false, context)
        
        val w = new NQuadsWriter(sw)
        
        w.startRDF()
        
        val dataModel = ModelFactory.createDefaultModel()
        
        for(stmt <- statements.asList().toArray()) {
        
          val stmtO = stmt.asInstanceOf[Statement]

          stmtO.getSubject()
          
          w.handleStatement(stmtO)
          
        }

        w.endRDF()
        */
        
      } finally {
        
        if(maker != null) try { maker.close() } catch {
          
          case ex: Exception => {
          }
          
        } 
        
        if(connection != null) try { connection.close() } catch { 
         
          case ex: Exception => {
          }
          
        }
        
      }
      
      (gid, sw.toString())
      
    }
    
    outputFS.delete(outputPath, true)
    outputRDD.saveAsTextFile(output)
    
    println("DONE")
    
  }
  
  
  
  
}