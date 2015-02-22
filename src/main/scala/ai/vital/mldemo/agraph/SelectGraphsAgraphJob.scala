package ai.vital.mldemo.agraph

import ai.vital.mldemo.common.AbstractJob
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import spark.jobserver.NamedRddSupport
import com.franz.agraph.repository.AGRepositoryConnection
import scala.collection.mutable.MutableList
import org.openrdf.query.QueryLanguage
import org.openrdf.query.TupleQueryResult
import scala.collection.mutable.SortedSet
import org.openrdf.model.URI

object SelectGraphsAgraphJob extends AbstractJob with NamedRddSupport {

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
  
  def sparqlOption = new Option("s", "sparql", true, "input sparql query, should return distinct list of graph ids")
  sparqlOption.setRequired(true)
  
  def outputNameOption = new Option("on", "output-name", true, "output RDD[String] name")
  outputNameOption.setRequired(true)
  
  def getJobClassName(): String = {
    return SelectGraphsAgraphJob.getClass.getCanonicalName
  }

  def getJobName(): String = {
    return "vital select sparql graphs job"
  }

  def getOptions(): Options = {
    return new Options()
      .addOption(masterOption)
      .addOption(urlOption)
      .addOption(userOption)
      .addOption(passwordOption)
      .addOption(catalogOption)
      .addOption(repoOption)
      .addOption(sparqlOption)
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
    val sparql = jobConfig.getString(sparqlOption.getLongOpt)
    val rddName = jobConfig.getString(outputNameOption.getLongOpt)
    
    println("serverURL : " + serverURL)
    println("username  : " + username)
    println("password  : " + password)
    println("catalog   : " + catalog)
    println("repository: " + repository)
    println("sparql    : " + sparql)
    println("outRDDName: " + rddName)
    
    if(catalog.equals("ROOT")) {
      catalog = ""
    }
    
    
    //it all happens inside driver
    
    var tupleQueryResults : TupleQueryResult = null
    var connection : AGRepositoryConnection = null 
    
    val gids = SortedSet[String]()
    
    try {
      
        connection = AGContextHolder.getConnection(serverURL, username, password, catalog, repository, 2, 2)

        val tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, sparql);
//        if( tupleQuery.getBindingsArray.size != 1) throw new Exception("Sparql query may only have a single binding, current: " + tupleQuery.getBindingsArray.size + " - " + sparql)
        

        tupleQueryResults = tupleQuery.evaluate()

        if( tupleQueryResults.getBindingNames.size() != 1 ) {
          throw new RuntimeException("Query")
        }
        
        while(tupleQueryResults.hasNext()) {

          val r = tupleQueryResults.next()
          
          val v = r.iterator().next().getValue
          
          if(!v.isInstanceOf[URI]) throw new RuntimeException("Query results is not a URI resource: " + v)
          
          val gid = v.stringValue()
          
          gids += gid
          
          
        }
        
        //initially just list contexts
//        val contexts = connection.getContextIDs
//        while ( contexts.hasNext() ) {
//          
//          val context = contexts.next()
//          
//          val gid = context.stringValue()
//          
//          gids += gid
//          
//        }
        
        
    } finally {

        if(tupleQueryResults != null) try {tupleQueryResults.close()} catch {
          case ex: Exception => {
          }
        }
      
        if(connection != null) try { connection.close() } catch { 
         
          case ex: Exception => {
          }
          
        }
        
        AGContextHolder.close()
        
    }
    
    
    println("Parallelizing gids...")
    val gidsRDD = sc.parallelize(gids.toSeq)
    
    println("Persisting gids RDD ...")
    
    if(rddName.startsWith("path:")) {
      println("TEST MODE - path")
      gidsRDD.saveAsTextFile(rddName.substring(5))
    } else {
    	this.namedRdds.update(rddName, gidsRDD)
    }
    
    
  }
  
  
}