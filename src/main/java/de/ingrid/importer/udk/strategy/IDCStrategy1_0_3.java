/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.ResultSet;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.JDBCHelper;

/**
 * @author Administrator
 * 
 */
public class IDCStrategy1_0_3 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy1_0_3.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_103;

	private int defaultSyslist6000EntryId = 3;
	private String defaultSyslist6000EntryValue =
		"nicht evaluiert: Die Konformität der Datenquelle wurde noch nicht evaluiert";

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {

		try {
			jdbc.setAutoCommit(false);

			// write version !
			setGenericKey(KEY_IDC_VERSION, MY_VERSION);

			System.out.print("  Updating sys_list...");
			updateSysList();
			System.out.println("done.");
			System.out.print("  Updating object_conformity...");
			updateObjectConformity();
			System.out.println("done.");
			// Updating of HI/LO table not necessary anymore ! is checked and updated when fetching next id
			// via getNextId() ...

			jdbc.commit();
			System.out.println("Update finished successfully.");

		} catch (Exception e) {
			System.out.println("Error executing strategy ! See log file for further information.");
			log.error("Error executing strategy!", e);
			throw e;
		}
	}

	protected void updateSysList() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating sys_list...");
		}

		int lstId = 6000;

		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" (Grad der Konformitaet)...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = " + lstId;
		jdbc.executeUpdate(sqlStr);

		// insert new syslist
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 1, 'de', 'konform: Die Datenquelle ist vollständig konform zur zitierten Spezifikation', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 2, 'de', 'nicht konform: Die Datenquelle ist nicht konform zur zitierten Spezifikation', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", " + defaultSyslist6000EntryId + ", 'de', '"
				+ defaultSyslist6000EntryValue + "', 0, 'Y');");

		if (log.isInfoEnabled()) {
			log.info("Updating sys_list... done");
		}
	}
	
	protected void updateObjectConformity() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating object_conformity...");
		}

		if (log.isInfoEnabled()) {
			log.info("Create table object_conformity...");
		}

		jdbc.getDBLogic().createTableObjectConformity(jdbc);
		
		if (log.isInfoEnabled()) {
			log.info("Add default entries for every object...");
		}

		// then add entries for ALL t01_objects (no matter whether working or published version) 
		String sql = "select distinct objNode.id as objNodeId, obj.id as objId " +
			"from t01_object obj, object_node objNode " +
			"where obj.obj_uuid = objNode.obj_uuid";

		ResultSet rs = jdbc.executeQuery(sql);
		HashMap<Long, Boolean> processedNodeIds = new HashMap<Long,Boolean>();
		while (rs.next()) {
			long objNodeId = rs.getLong("objNodeId");
			long objId = rs.getLong("objId");

			String defaultSpecification = "INSPIRE-Richtlinie";

			jdbc.executeUpdate("INSERT INTO object_conformity (id, obj_id, specification, degree_key, degree_value) " +
				"VALUES (" + getNextId() + ", " + objId + ", '" + defaultSpecification + "', "
				+ defaultSyslist6000EntryId + ", '" + defaultSyslist6000EntryValue + "');");
			
			// Node may contain different object versions, then we receive nodeId multiple times.
			// Write Index only once (index contains data of working version!) !
			if (!processedNodeIds.containsKey(objNodeId)) {
				JDBCHelper.updateObjectIndex(objNodeId, defaultSpecification, jdbc); // ObjectConformity.specification
				JDBCHelper.updateObjectIndex(objNodeId, defaultSyslist6000EntryValue, jdbc); // ObjectConformity.degreeValue
				
				processedNodeIds.put(objNodeId, true);
			}
		}
		rs.close();

		if (log.isInfoEnabled()) {
			log.info("Updating object_conformity... done");
		}
	}
}
