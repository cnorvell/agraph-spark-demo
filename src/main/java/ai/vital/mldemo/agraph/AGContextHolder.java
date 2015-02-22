package ai.vital.mldemo.agraph;

import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.franz.agraph.pool.AGConnPool;
import com.franz.agraph.pool.AGConnProp;
import com.franz.agraph.pool.AGPoolProp;
import com.franz.agraph.repository.AGCatalog;
import com.franz.agraph.repository.AGRepository;
import com.franz.agraph.repository.AGRepositoryConnection;
import com.franz.agraph.repository.AGServer;

/**
 * This class is meant to keep the active connection pool in workers
 * @author Derek
 *
 */
public class AGContextHolder {

	private AGContextHolder(){}

	private static AGConnPool repositoryConnPool;
	
	private static AGRepository repository;
	
	private final static Logger log = LoggerFactory.getLogger(AGContextHolder.class);
	
	public static AGRepositoryConnection getConnection(String serverURL, String username, String password, String catalogName, String repositoryID, int initial, int max) throws RepositoryException {
		
		if(repositoryConnPool == null) {
		
			log.info("Creating new AG connections pool...");
			
			synchronized (AGContextHolder.class) {
				
				if(repositoryConnPool == null) {
					
					repositoryConnPool = AGConnPool.create(
							AGConnProp.serverUrl, serverURL,
							AGConnProp.username, username,
							AGConnProp.password, password,
							AGConnProp.catalog, catalogName,
							AGConnProp.repository, repositoryID,
							AGConnProp.session, AGConnProp.Session.SHARED,
							
//		               AGConnProp.sessionLifetime, SESSION_LIFETIME,
//		               AGPoolProp.shutdownHook, false,
//		               AGPoolProp.testOnBorrow, true,
//		               AGPoolProp.maxActive, POOL_MAX_ACTIVE_CONNECTIONS,
//		               AGPoolProp.maxIdle, POOL_MAX_IDLE_CONNECTIONS,
//		               AGPoolProp.maxWait, POOL_MAX_TIME_TO_WAIT_FOR_CONNECTION
							
							
//					AGPoolProp.shutdownHook, true,
							AGPoolProp.maxActive, max,
							AGPoolProp.initialSize, initial);
					
					AGServer server = new AGServer(serverURL, username, password);
					
					AGCatalog catalog = null;
					if(catalogName != null && !catalogName.isEmpty()) {
						catalog = server.getCatalog(catalogName);
					}else {
						catalog = server.getRootCatalog();
					}
					
					repository = catalog.createRepository(repositoryID);  
					repository.setConnPool(repositoryConnPool); 
					
				}
				
			}
			
		} else {
			log.info("AG connections pool already exists");
		}
		
		return repository.getConnection();
	}
	
	public static void close() {
		
		if(repository != null) {
			try {
				repository.close();
			} catch (RepositoryException e) {
			}
			try {
				repositoryConnPool.close();
			} catch (Exception e) {
			}
			
			repository = null;
			repositoryConnPool = null;
		}
		
	}
	
}
