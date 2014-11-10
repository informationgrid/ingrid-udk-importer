/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 wemove digital solutions GmbH
 * ==================================================
 * Licensed under the EUPL, Version 1.1 or – as soon they will be
 * approved by the European Commission - subsequent versions of the
 * EUPL (the "Licence");
 * 
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 * 
 * http://ec.europa.eu/idabc/eupl5
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 * **************************************************#
 */
/**
 * 
 */
package de.ingrid.importer.udk.strategy.v1;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategy;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * Single Strategy for fixing tree path attribute in object/address nodes !
 * WAS INCONSISTENT ON TEST IGC DUE TO BUG (no writing of tree path when publishing new object/address) !
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
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery("select obj_uuid, fk_obj_uuid from object_node", st);
		while (rs.next()) {
			String uuid = rs.getString("obj_uuid");
			String parentUuid = rs.getString("fk_obj_uuid");
			
			nodeToParentMap.put(uuid, parentUuid);
		}
		rs.close();
		st.close();

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
		st = jdbc.createStatement();
		rs = jdbc.executeQuery("select addr_uuid, fk_addr_uuid from address_node", st);
		while (rs.next()) {
			String uuid = rs.getString("addr_uuid");
			String parentUuid = rs.getString("fk_addr_uuid");
			
			nodeToParentMap.put(uuid, parentUuid);
		}
		rs.close();
		st.close();

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
