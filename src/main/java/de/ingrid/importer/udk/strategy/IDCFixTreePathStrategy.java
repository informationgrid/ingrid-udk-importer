/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Single Strategy for fixing tree path attribute in object/address nodes !
 * @author martin
 */
public class IDCFixTreePathStrategy extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCFixTreePathStrategy.class);

	/**
	 * Write NO Version, this strategy should be executed on its own on chosen catalogues
	 * @see de.ingrid.importer.udk.strategy.IDCStrategy#getIDCVersion()
	 */
	public String getIDCVersion() {
		return null;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		System.out.print("  Fixing object_node/address_node tree_path attribute ...");
		fixTreePath();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Fix finished successfully.");
	}

	protected void fixTreePath() throws Exception {
		String NODE_SEPARATOR = "|";  

		// update all objects !
		if (log.isInfoEnabled()) {
			log.info("Fixing tree_path in object_node...");
		}

		// first set up map representing tree structure
		HashMap<String, String> nodeToParentMap = new HashMap<String, String>();
		ResultSet rs = jdbc.executeQuery("select obj_uuid, fk_obj_uuid from object_node");
		while (rs.next()) {
			String uuid = rs.getString("obj_uuid");
			String parentUuid = rs.getString("fk_obj_uuid");
			
			nodeToParentMap.put(uuid, parentUuid);
		}
		rs.close();

		// then process all nodes and write their path !
		int numNodes = 0;
		Iterator<String> nodeIt = nodeToParentMap.keySet().iterator();
		while (nodeIt.hasNext()) {
			String nodeUuid = nodeIt.next();
			String parentUuid = nodeToParentMap.get(nodeUuid);
			String path = "";
			
			// set up path
			while (parentUuid != null) {
				// insert parent at front !
				path = NODE_SEPARATOR + parentUuid + NODE_SEPARATOR + path;
				parentUuid = nodeToParentMap.get(parentUuid);
			}
			
			// write path. NOTICE: top nodes have path ''
			jdbc.executeUpdate("UPDATE object_node SET tree_path = '" + path + "' " +
				"where obj_uuid = '" + nodeUuid + "'");
			numNodes++;
		}
		
		if (log.isInfoEnabled()) {
			log.info("Processed " + numNodes + " object_nodes");
			log.info("Fixing tree_path in object_node... done");
		}

		
		// update all addresses !
		if (log.isInfoEnabled()) {
			log.info("Fixing tree_path in address_node...");
		}

		// first set up map representing tree structure
		nodeToParentMap = new HashMap<String, String>();
		rs = jdbc.executeQuery("select addr_uuid, fk_addr_uuid from address_node");
		while (rs.next()) {
			String uuid = rs.getString("addr_uuid");
			String parentUuid = rs.getString("fk_addr_uuid");
			
			nodeToParentMap.put(uuid, parentUuid);
		}
		rs.close();

		// then process all nodes and write their path !
		numNodes = 0;
		nodeIt = nodeToParentMap.keySet().iterator();
		while (nodeIt.hasNext()) {
			String nodeUuid = nodeIt.next();
			String parentUuid = nodeToParentMap.get(nodeUuid);
			String path = "";
			
			// set up path
			while (parentUuid != null) {
				// insert parent at front !
				path = NODE_SEPARATOR + parentUuid + NODE_SEPARATOR + path;
				parentUuid = nodeToParentMap.get(parentUuid);
			}
			
			// write path. NOTICE: top nodes have path ''
			jdbc.executeUpdate("UPDATE address_node SET tree_path = '" + path + "' " +
				"where addr_uuid = '" + nodeUuid + "'");				
			numNodes++;
		}
		
		if (log.isInfoEnabled()) {
			log.info("Processed " + numNodes + " address_nodes");
			log.info("Fixing tree_path in address_node... done");
		}
	}
}
