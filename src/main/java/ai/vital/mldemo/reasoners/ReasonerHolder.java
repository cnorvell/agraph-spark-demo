package ai.vital.mldemo.reasoners;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.reasoner.Reasoner;
import com.hp.hpl.jena.reasoner.rulesys.GenericRuleReasoner;
import com.hp.hpl.jena.reasoner.rulesys.GenericRuleReasonerFactory;
import com.hp.hpl.jena.reasoner.rulesys.Rule;

//holds map of named reasoners
public class ReasonerHolder {

	private final static Logger log = LoggerFactory.getLogger(ReasonerHolder.class);
	
	private static Reasoner singleton;
	
	private ReasonerHolder() {}
	
	
	public static Reasoner get(String ontologyPath, String rulesPath) {
		
		if(singleton == null) {
			log.info("Creating new reasoner...");
			synchronized (ReasonerHolder.class) {
				
				if(singleton != null) { return singleton;}

				Path ontP = new Path(ontologyPath);
				Path rulesP = new Path(rulesPath);
				InputStream ontIS = null;
				FileSystem ontFS = null;
				InputStream rulesIS = null;
				BufferedReader rulesReader = null;
				FileSystem rulesFS = null;
				
				
				try {
					Configuration c = new Configuration();
					ontFS = FileSystem.get(ontP.toUri(), c);
				
					ontIS = ontFS.open(ontP);
					
					Model ontologyModel = ModelFactory.createDefaultModel();
					ontologyModel.read(ontIS, null);

					rulesFS = FileSystem.get(rulesP.toUri(), c);
					
					rulesIS = rulesFS.open(rulesP);
					rulesReader = new BufferedReader(new InputStreamReader(rulesIS, "UTF-8"));
					
					List<Rule> rules = Rule.parseRules(Rule.rulesParserFromReader(rulesReader));
					
					GenericRuleReasoner reasoner = (GenericRuleReasoner) GenericRuleReasonerFactory.theInstance().create(null);
					reasoner.setMode(GenericRuleReasoner.BACKWARD);
					reasoner.setRules(rules);
					reasoner.bindSchema(ontologyModel);

					singleton = reasoner;
					
				} catch (IOException e) {
					throw new RuntimeException(e);
				} finally {
					IOUtils.closeQuietly(rulesFS);
					IOUtils.closeQuietly(rulesIS);
					IOUtils.closeQuietly(rulesReader);
					IOUtils.closeQuietly(ontFS);
					IOUtils.closeQuietly(ontIS);
				}
				
			}
		} else {
			log.info("Reasoner already created - reusing...");
		}
		
		return singleton;
	}
	
	
	
	
	
}
