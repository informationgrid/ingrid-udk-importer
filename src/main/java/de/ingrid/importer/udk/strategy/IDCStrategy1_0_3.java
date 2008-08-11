/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.ResultSet;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.JDBCHelper;
import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;

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

	private int noData_Syslist6010EntryId = 1;
	private String noData_Syslist6010EntryValue = "keine";
	private int existingData_Syslist6010EntryId = 6;
	private String existingData_Syslist6010EntryValue = "aufgrund der Rechte des geistigen Eigentums";

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// FIRST EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit (e.g. on MySQL)
		System.out.print("  Extend datastructure...");
		extendDataStructure();
		System.out.println("done.");

		// THEN PERFORM DATA MANIPULATIONS !

		// write IDC structure version !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		System.out.print("  Updating sys_list...");
		updateSysList();
		System.out.println("done.");
		System.out.print("  Updating object_conformity...");
		updateObjectConformity();
		System.out.println("done.");
		System.out.print("  Updating t011_obj_geo...");
		updateT011ObjGeo();
		System.out.println("done.");
		System.out.print("  Updating object_access...");
		updateObjectAccess();
		System.out.println("done.");

		// Updating of HI/LO table not necessary anymore ! is checked and updated when fetching next id
		// via getNextId() ...

		// FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause commit (e.g. on MySQL)
		System.out.print("  Clean up datastructure...");
		cleanUpDataStructure();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	protected void extendDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Extending datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Create table 'object_conformity'...");
		}
		jdbc.getDBLogic().createTableObjectConformity(jdbc);

		if (log.isInfoEnabled()) {
			log.info("Add column 'datasource_uuid' to 'table t011_obj_geo'...");
		}
		jdbc.getDBLogic().addColumn("datasource_uuid", ColumnType.TEXT, "t011_obj_geo", true, jdbc);
		
		if (log.isInfoEnabled()) {
			log.info("Create table 'object_access'...");
		}
		jdbc.getDBLogic().createTableObjectAccess(jdbc);

		if (log.isInfoEnabled()) {
			log.info("Extending datastructure... done");
		}
	}
	
	protected void updateSysList() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating sys_list...");
		}

		int lstId = 6000;
		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" (Grad der Konformität)...");
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

		// --------------------

		lstId = 6010;
		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" (Zugangsbeschränkungen)...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = " + lstId;
		jdbc.executeUpdate(sqlStr);

		// insert new syslist
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", " + noData_Syslist6010EntryId + ", 'de', '" + noData_Syslist6010EntryValue 
			+ "', 0, 'Y');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", 2, 'de', 'aufgrund der Vertraulichkeit der Verfahren von Behörden', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", 3, 'de', 'aufgrund internationaler Beziehungen, der öffentliche Sicherheit oder der Landesverteidigung', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", 4, 'de', 'aufgrund laufender Gerichtsverfahren', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", 5, 'de', 'aufgrund der Vertraulichkeit von Geschäfts- oder Betriebsinformationen', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", " + existingData_Syslist6010EntryId + ", 'de', '" + existingData_Syslist6010EntryValue
			+ "', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", 7, 'de', 'aufgrund der Vertraulichkeit personenbezogener Daten', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", 8, 'de', 'aufgrund des Schutzes einer Person', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", 9, 'de', 'aufgrund des Schutzes von Umweltbereichen', 0, 'N');");

		if (log.isInfoEnabled()) {
			log.info("Updating sys_list... done");
		}
	}
	
	protected void updateObjectConformity() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating object_conformity...");
		}

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

			jdbc.executeUpdate("INSERT INTO object_conformity (id, obj_id, line, specification, degree_key, degree_value) " +
				"VALUES (" + getNextId() + ", " + objId + ", 1, '" + defaultSpecification + "', "
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

	protected void updateT011ObjGeo() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating t011_obj_geo...");
		}

		if (log.isInfoEnabled()) {
			log.info("Add 'datasource_uuid' default values, 'special_base' defaults...");
		}

		// get catalog name and create prefix for unique datasource_uuid
		String sql = "select cat_name from t03_catalogue";
		ResultSet rs = jdbc.executeQuery(sql);
		rs.next();
		String catName = rs.getString("cat_name");
		rs.close();
		
		String datasourceUuidPrefix = catName.replace(' ', '_');
		datasourceUuidPrefix += ":";

		// then add default data for ALL t011_obj_geo 
		sql = "select distinct objGeo.id as objGeoId, objGeo.special_base, obj.obj_uuid as objUuid " +
			"from t011_obj_geo objGeo, t01_object obj " +
			"where objGeo.obj_id = obj.id";

		rs = jdbc.executeQuery(sql);
		HashMap<String, Boolean> processedObjUuids = new HashMap<String,Boolean>();
		while (rs.next()) {
			long objGeoId = rs.getLong("objGeoId");
			String objUuid = rs.getString("objUuid");
			String objGeoSpecialBase = rs.getString("special_base");

			if (processedObjUuids.containsKey(objUuid)) {
				throw new Exception("Object with multiple 't011_obj_geo' records ! " +
					"obj_uuid(" + objUuid + "), failing t011_obj_geo.id(" + objGeoId + ")" );
			}
			processedObjUuids.put(objUuid, true);
			
			String datasourceUuid = datasourceUuidPrefix + objUuid;
			jdbc.executeUpdate("UPDATE t011_obj_geo SET datasource_uuid = '" + datasourceUuid + "' " +
				"where id = " + objGeoId);
			
			// special_base is now mandatory ! supply default value
			if (objGeoSpecialBase == null || objGeoSpecialBase.trim().length() == 0) {
				jdbc.executeUpdate("UPDATE t011_obj_geo SET special_base = 'Unbekannt' " +
						"where id = " + objGeoId);				
			}
		}
		rs.close();

		if (log.isInfoEnabled()) {
			log.info("Updating t011_obj_geo... done");
		}
	}

	protected void updateObjectAccess() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating object_access...");
		}

		if (log.isInfoEnabled()) {
			log.info("Add entries for every object...");
		}

		// then add entries for ALL t01_objects (no matter whether working or published version) 
		String sql = "select distinct objNode.id as objNodeId, objNode.obj_id as objWorkId, obj.id as objId, obj.avail_access_note, obj.fees " +
			"from t01_object obj, object_node objNode " +
			"where obj.obj_uuid = objNode.obj_uuid";

		// Node may contain different object versions (working and published version), just to be sure 
		// we track written data in hash maps to avoid multiple writing for same object (or should we trust upper sql ;)
		HashMap<Long, Boolean> processedObjIds = new HashMap<Long,Boolean>();
		HashMap<Long, Boolean> processedObjWorkIds = new HashMap<Long,Boolean>();

		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			long objNodeId = rs.getLong("objNodeId");
			long objWorkId = rs.getLong("objWorkId");
			long objId = rs.getLong("objId");
			String availAccessNote = rs.getString("avail_access_note");
			availAccessNote = (availAccessNote == null) ? "" : availAccessNote.trim();
			String fees = rs.getString("fees");
			fees = (fees == null) ? "" : fees.trim();

			// write values if not written yet !
			if (!processedObjIds.containsKey(objId)) {
				// default: NO access data set in object
				int syslist6010EntryId = noData_Syslist6010EntryId;
				String syslist6010EntryValue = noData_Syslist6010EntryValue;
				String termsOfUse = "keine Einschränkungen";

				// values when access data set in object
				if (availAccessNote.length() > 0 || fees.length() > 0) {
					syslist6010EntryId = existingData_Syslist6010EntryId;
					syslist6010EntryValue = existingData_Syslist6010EntryValue;
					termsOfUse = "";
					if (availAccessNote.length() > 0) {
						termsOfUse += availAccessNote;
					}
					if (fees.length() > 0) {
						if (termsOfUse.length() > 0) {
							termsOfUse += " // ";						
						}
						termsOfUse += fees;
					}
				}

				jdbc.executeUpdate("INSERT INTO object_access (id, obj_id, line, restriction_key, restriction_value, terms_of_use) "
					+ "VALUES (" + getNextId() + ", " + objId + ", 1, " + syslist6010EntryId + ", '" + syslist6010EntryValue
					+ "', '" + termsOfUse + "');");
				
				processedObjIds.put(objId, true);

				// extend object index if not written yet (index contains only data of working versions !)
				if (objWorkId == objId) {
					if (!processedObjWorkIds.containsKey(objId)) {
						JDBCHelper.updateObjectIndex(objNodeId, syslist6010EntryValue, jdbc); // ObjectAccess.restrictionValue
						JDBCHelper.updateObjectIndex(objNodeId, termsOfUse, jdbc); // ObjectAccess.termsOfUse
						
						processedObjWorkIds.put(objId, true);
					}				
				}
			}
		}
		rs.close();

		if (log.isInfoEnabled()) {
			log.info("Updating object_access... done");
		}
	}

	protected void cleanUpDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Drop 't01_object.avail_access_note' ...");
		}
		jdbc.getDBLogic().dropColumn("avail_access_note", "t01_object", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Drop 't01_object.fees' ...");
		}
		jdbc.getDBLogic().dropColumn("fees", "t01_object", jdbc);
		
		if (log.isInfoEnabled()) {
			log.info("Add not null constraint to 't011_obj_geo.special_base' ...");
		}
		jdbc.getDBLogic().modifyColumn("special_base", ColumnType.TEXT, "t011_obj_geo", true, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure... done");
		}
	}
}
