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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * InGrid 2.3, statische Änderungen:
 * - Erstellung einer eigenen Klasse für geographische Services (extend t011_obj_serv, add t011_obj_serv_url, manipulate syslists ...)
 */
public class IDCStrategy1_0_9 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy1_0_9.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_109;

	final public static int TYPE_KEY_OTHER_SERVICE = 6;
	final public static int CLASSIFIC_KEY_NON_GEO_SERVICE = 901;
	final public static String WORK_STATE_IN_BEARBEITUNG = "B";

	final public static int NEW_OBJ_CLASS_INFORMATIONSSYSTEM = 6;

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		// THEN EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit (e.g. on MySQL)
		// ---------------------------------
		System.out.print("  Extend datastructure...");
		extendDataStructure();
		System.out.println("done.");

		// THEN PERFORM DATA MANIPULATIONS !
		// ---------------------------------

		// InGrid 2.3: "Erstellung einer eigenen Klasse für geographische Services"

		System.out.print("  Extending sys_list...");
		extendSysList();
		System.out.println("done.");

		System.out.println("  Migrate services to new classes 'Geodatendienst' / 'Informationssystem/Dienst/Anwendung'...");
		migrateServices();
		System.out.println("done.");

		System.out.print("  Clean up sys_list...");
		cleanUpSysList();
		System.out.println("done.");

		System.out.print("  Updating sys_gui...");
		updateSysGui();
		System.out.println("done.");

		// FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause commit (e.g. on MySQL)
		// ---------------------------------
/*
		System.out.print("  Clean up datastructure...");
		cleanUpDataStructure();
		System.out.println("done.");
*/
		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void extendDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Manipulate datastructure -> CAUSES COMMIT ! ...");
		}

		// InGrid 2.3: "Erstellung einer eigenen Klasse für geographische Services"

		if (log.isInfoEnabled()) {
			log.info("Add columns 'has_access_constraint' to table 't011_obj_serv' ...");
		}
		jdbc.getDBLogic().addColumn("has_access_constraint", ColumnType.VARCHAR1, "t011_obj_serv", false, "'N'", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Create table 't011_obj_serv_url'...");
		}
		jdbc.getDBLogic().createTableT011ObjServUrl(jdbc);

		if (log.isInfoEnabled()) {
			log.info("Manipulate datastructure... done");
		}
	}

	protected void extendSysList() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Extending sys_list...");
		}

		int lstId = 5300;
		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" (Klassifikation für neue Klasse \"Informationssystem/Dienst/Anwendung\")...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = " + lstId;
		int numDeleted = jdbc.executeUpdate(sqlStr);
		if (numDeleted > 0) {
			String msg = "New Syslist " + lstId +	" EXISTED ! We deleted old values !";
			System.out.println("\n" + msg + " See also log file (WARN).");
			log.warn(msg);			
		}

		// insert new syslist
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", 1, 'de', 'Informationssystem', 0, 'N')");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 1, 'en', 'Information System', 0, 'N')");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", 2, 'de', 'nicht geographischer Dienst', 0, 'N')");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 2, 'en', 'Non Geographic Service', 0, 'N')");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", 3, 'de', 'Anwendung', 0, 'N')");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 3, 'en', 'Application', 0, 'N')");

		if (log.isInfoEnabled()) {
			log.info("Extending sys_list... done");
		}
	}

	protected void cleanUpSysList() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Clean up sys_list...");
		}
		
		int numDeleted;
/*
		if (log.isInfoEnabled()) {
			log.info("Remove entry " + TYPE_KEY_OTHER_SERVICE + " ('Other Service') from syslist 5100 (Typ des Dienstes)...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = 5100 and entry_id = " + TYPE_KEY_OTHER_SERVICE;
		numDeleted = jdbc.executeUpdate(sqlStr);
		if (log.isDebugEnabled()) {
			log.debug("Removed " + numDeleted +	" entries (all languages).");
		}
*/
		// --------------------

		if (log.isInfoEnabled()) {
			log.info("Remove entry 901 ('Non Geographic Service') from syslist 5200 (Service-Klassifikation)...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = 5200 and entry_id = 901";
		numDeleted = jdbc.executeUpdate(sqlStr);
		if (log.isDebugEnabled()) {
			log.debug("Removed " + numDeleted +	" entries (all languages).");
		}
		
		if (log.isInfoEnabled()) {
			log.info("Clean up sys_list... done");
		}
	}

	/** Add gui ids of all NEW OPTIONAL fields to sys_gui table ! (mandatory fields not added, behavior not changeable) */
	protected void updateSysGui() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating sys_gui...");
		}

		if (log.isInfoEnabled()) {
			log.info("Add ids of new OPTIONAL fields !...");
		}

		LinkedHashMap<String, Integer> newSysGuis = new LinkedHashMap<String, Integer>();
		Integer initialBehaviour = -1; // default behaviour, optional field only shown if section expanded ! 
//		Integer remove = 0; // do NOT show if section reduced (use case for optional fields ? never used) !
//		Integer mandatory = 1; // do also show if section reduced (even if field optional) !

		newSysGuis.put("3260", initialBehaviour);
		newSysGuis.put("3630", initialBehaviour);
		newSysGuis.put("3600", initialBehaviour);
		newSysGuis.put("3640", initialBehaviour);
		newSysGuis.put("3645", initialBehaviour);
		newSysGuis.put("3650", initialBehaviour);
		newSysGuis.put("3670", initialBehaviour);
		
		Iterator<String> itr = newSysGuis.keySet().iterator();
		while (itr.hasNext()) {
			String key = itr.next();
			jdbc.executeUpdate("INSERT INTO sys_gui (id, gui_id, behaviour) VALUES ("
					+ getNextId() + ", '" + key + "', " + newSysGuis.get(key) + ")");
		}
		
		if (log.isInfoEnabled()) {
			log.info("Updating sys_gui... done");
		}
	}

	protected void migrateServices() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Migrate 'Dienst/Anwendung/Informationssystem' to new classes 'Geodatendienst' / 'Informationssystem/Dienst/Anwendung'...");
		}

		String sql = "select " +
			"oNode.id as oNodeId, oNode.obj_uuid, oNode.obj_id, oNode.obj_id_published, " + // object node
			"obj.obj_name, " + // object
			"objServ.id as objServId, objServ.type_key as typeKey, objServ.type_value as typeValue, " + // type
			"objServType.serv_type_key as classificKey, " + // classification
			"objServOp.id as opId, objServOp.name_value as opName, objServOp.descr as opDescr, " + // operation
			"objServOpConn.connect_point as opUrl " + // operation url
			"from " +
			"object_node oNode " +
			// always join "working version" ! equals published version, if no working version
			"left join t01_object obj on (oNode.obj_id = obj.id) " +
			"left outer join t011_obj_serv objServ on (obj.id = objServ.obj_id) " +
			"left outer join t011_obj_serv_type objServType on (objServ.id = objServType.obj_serv_id) " +
			"left outer join t011_obj_serv_operation objServOp on (objServ.id = objServOp.obj_serv_id) " +
			"left outer join t011_obj_serv_op_connpoint objServOpConn on (objServOp.id = objServOpConn.obj_serv_op_id) " +
			"where " +
			"obj.obj_class = 3 " +
			"order by obj_id, objServId, objServType.line, objServOp.line, objServOpConn.line";

		MigrationStatistics stats = new MigrationStatistics();
		ServiceObject currentObj = null;

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			long nextObjId = rs.getLong("obj_id");
			
			// check whether all data of an object is read, then do migration !
			boolean objChange = false;
			if (currentObj != null && currentObj.objId != nextObjId) {
				// object changed, process finished object
				objChange = true;
				migrateObject(currentObj, stats);
			}
			
			if (currentObj == null || objChange) {
				// set up next object
				currentObj = new ServiceObject(rs.getLong("oNodeId"), rs.getString("obj_uuid"), nextObjId, rs.getLong("obj_id_published"), 
					rs.getString("obj_name"), rs.getLong("objServId"), rs.getInt("typeKey"), rs.getString("typeValue"));
			}

			// pass new stuff
			currentObj.addClassificKey(rs.getInt("classificKey"));
			currentObj.addOperation(rs.getLong("opId"),
				rs.getString("opName"), rs.getString("opDescr"), rs.getString("opUrl"));
		}
		// also migrate last object ! not done in loop due to end of loop !
		if (currentObj != null) {
			migrateObject(currentObj, stats);			
		}

		rs.close();
		st.close();

		// Protocol also to System.out !

		String msg = "Migrated " + stats.numGeo + " objects to class 'Geodatendienst'.";
		System.out.println("\n" + msg + " See also log file (INFO).");
		log.info(msg);
		
		msg = "Migrated " + stats.numNonGeo + " objects to class 'Informationssystem/Dienst/Anwendung'.";
		System.out.println("\n" + msg + " See also log file (INFO).");
		log.info(msg);
		
		if (stats.objNamesUnpublishedGeo.size() > 0) {
			msg = "The following " + stats.objNamesUnpublishedGeo.size() +
				" objects have been migrated to 'Geodatendienst' and put to WORKING STATE due to missing classification ! Please edit manually and publish again !\n\n" +
				stats.getObjNamesUnpublishedAsString(true);
			System.out.println("\n" + msg + "See also log file (WARN).");
			log.warn(msg);
		}
		if (stats.objNamesUnpublishedNonGeo.size() > 0) {
			msg = "The following " + stats.objNamesUnpublishedNonGeo.size() +
				" objects have been migrated to 'Informationssystem/Dienst/Anwendung' and put to WORKING STATE ! Please choose NEW type of service manually and publish again !\n\n" +
				stats.getObjNamesUnpublishedAsString(false);
			System.out.println("\n" + msg + "See also log file (WARN).");
			log.warn(msg);
		}

		if (log.isInfoEnabled()) {
			log.info("Migrate 'Dienst/Anwendung/Informationssystem' to new classes 'Geodatendienst' / 'Informationssystem/Dienst/Anwendung'... done");
		}
	}

	/** Migrate the given object. Pass stats for counting. 
	 * @param currentObj obj to migrate
	 * @param stats statistics object, will be updated
	 * @throws Exception
	 */
	protected void migrateObject(ServiceObject currentObj, MigrationStatistics stats) throws Exception {
		if (currentObj.isGeoService()) {
			// migrate to 'Geodatendienst'
			migrateToGeoService(currentObj, stats);

		} else {
			// migrate to 'Informationssystem/Dienst/Anwendung'
			migrateToNonGeoService(currentObj, stats);
		}
	}

	/** Migrate the given object to 'Geodatendienst'
	 * @param currentObj obj to migrate
	 * @param stats statistics object, will be updated
	 * @throws Exception
	 */
	protected void migrateToGeoService(ServiceObject currentObj, MigrationStatistics stats) throws Exception {
		// migrate to 'Geodatendienst'
		stats.numGeo++;

		if (log.isInfoEnabled()) {
			log.info("Start Migration: object '" + currentObj.name + "' of service type '" + currentObj.typeValue +
				"' and classification keys '" + currentObj.getClassificationKeysAsString() +
				"' to new class 'Geodatendienst'");
		}

		// check whether classification contains 'Non Geographic Service' !
		// may occur if type IS GEO-type and classification is NON-GEO, then we log WARNING !
		if (currentObj.classificKeys.contains(CLASSIFIC_KEY_NON_GEO_SERVICE)) {

			log.warn("!!! object '" + currentObj.uuid + ":" + currentObj.name + "' of service type '" + currentObj.typeValue +
				"' and classification keys '" + currentObj.getClassificationKeysAsString() +
				"' is classified as 'Non Geographic Service' (901)! We delete this classification and migrate to new class 'Geodatendienst' !");

			int numDeleted = jdbc.executeUpdate("DELETE FROM t011_obj_serv_type where obj_serv_id = " + currentObj.objServId +
				" AND serv_type_key = " + CLASSIFIC_KEY_NON_GEO_SERVICE);
			currentObj.classificKeys.remove((Integer)CLASSIFIC_KEY_NON_GEO_SERVICE);

			if (log.isDebugEnabled()) {
				log.debug("Removed " + numDeleted +	" entries from t011_obj_serv_type.");
			}
		}

		// check whether classification was removed, then we have to put object in working state !
		if (currentObj.classificKeys.size() == 0) {
			// if only published version we have to put object in working state !
			// if published version different from working version we have to delete published version !
			if (currentObj.isPublished()) {
				String msg = "!!! object '" + currentObj.uuid + ":" + currentObj.name + "' of service type '" + currentObj.typeValue +
					"' has NO classification and IS PUBLISHED ! WE PUT OBJECT INTO WORKING STATE AND REMOVE PUBLISHED VERSION !";
//				System.out.println("\n" + msg + " See also log file (WARN).");
				log.warn(msg);
				if (currentObj.hasWorkingVersion()) {
					msg = "!!! object '" + currentObj.uuid + ":" + currentObj.name + "' has to be unpublished and has separate WORKING VERSION, WE DELETE PUBLISHED VERSION !";
					System.out.println("\n" + msg + " See also log file (WARN).");
					log.warn(msg);

					// delete published version
					deleteObject(currentObj.objIdPublished);

				} else {
					log.warn("!!! object '" + currentObj.uuid + ":" + currentObj.name + "' has NO WORKING VERSION, WE MOVE PUBLISHED TO WORKING VERSION !");

					// put published version in working state
					setObjectWorkingState(currentObj.objIdPublished);
				}
				
				// UNPUBLISH ! update object node (reset published id)
				setObjectNodeUnpublished(currentObj.objNodeId);
				stats.addObjNameUnpublished(currentObj.uuid + ":" + currentObj.name, true);
			}
		}

		// NOTHING TO CHANGE, class number and data stays the same !
	}

	/** Migrate the given object to 'Informationssystem/Dienst/Anwendung'.
	 * @param currentObj obj to migrate
	 * @param stats statistics object, will be updated
	 * @throws Exception
	 */
	protected void migrateToNonGeoService(ServiceObject currentObj, MigrationStatistics stats) throws Exception {
		// migrate to 'Informationssystem/Dienst/Anwendung'
		stats.numNonGeo++;

		if (log.isInfoEnabled()) {
			log.info("Start Migration: object '" + currentObj.name + "' of service type '" + currentObj.typeValue +
				"' and classification keys '" + currentObj.getClassificationKeysAsString() +
				"' to new class 'Informationssystem/Dienst/Anwendung'");
		}

		// reset service type
		int numProcessed = jdbc.executeUpdate("UPDATE t011_obj_serv SET " +
			"type_key = NULL, type_value = NULL where id = " + currentObj.objServId);
		if (numProcessed != 1) {
			log.warn("Multiple resetting of former service type, NO single record (found " + numProcessed + " records in 't011_obj_serv') !");						
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Reset former service type '" + currentObj.typeValue + "', set t011_obj_serv.type to NULL.");
			}
		}

		// remove classification (= "Service-Klassifikation") !
		numProcessed = jdbc.executeUpdate("DELETE FROM t011_obj_serv_type where obj_serv_id = " + currentObj.objServId);
		if (numProcessed > 0 && log.isDebugEnabled()) {
			log.debug("Removed former classification entries -> " + numProcessed + " entries from t011_obj_serv_type.");
		}

		// remove scale (= "Erstellungsmassstab") !
		numProcessed = jdbc.executeUpdate("DELETE FROM t011_obj_serv_scale where obj_serv_id = " + currentObj.objServId);
		if (numProcessed > 0 && log.isDebugEnabled()) {
			log.debug("Removed former scale entries -> " + numProcessed + " entries from t011_obj_serv_scale.");
		}

		// migrate URLs from operations if present
		int line = 1;
		for (ServiceOperation op : currentObj.operations.values()) {
			String name = op.name;
			String description = op.description;
			for (String url : op.urls) {
				String sql = "INSERT INTO t011_obj_serv_url (id, obj_serv_id, line, name, url, description) "
					+ "VALUES (" + getNextId() + ", " + currentObj.objServId + ", " + line + ", '" 
					+ name + "', '" + url + "', '" + description + "')";
				jdbc.executeUpdate(sql);							
				line++;
				if (log.isInfoEnabled()) {
					log.info("Migrated former operation to URL '" + url + "' with description '" + description + "'");
				}
			}
		}

		// remove operations
		for (ServiceOperation op : currentObj.getOperations()) {
			numProcessed = jdbc.executeUpdate("DELETE FROM t011_obj_serv_operation where id = " + op.objServOpId);
			if (numProcessed > 0 && log.isDebugEnabled()) {
				log.debug("Removed former operation '" + op.name + "' -> " + numProcessed + " entries from t011_obj_serv_operation.");
			}
			numProcessed = jdbc.executeUpdate("DELETE FROM t011_obj_serv_op_connpoint where obj_serv_op_id = " + op.objServOpId);
			if (numProcessed > 0 && log.isDebugEnabled()) {
				log.debug("Removed operation connections -> " + numProcessed + " entries from t011_obj_serv_op_connpoint.");
			}
			numProcessed = jdbc.executeUpdate("DELETE FROM t011_obj_serv_op_depends where obj_serv_op_id = " + op.objServOpId);
			if (numProcessed > 0 && log.isDebugEnabled()) {
				log.debug("Removed operation depends -> " + numProcessed + " entries from t011_obj_serv_op_depends.");
			}
			numProcessed = jdbc.executeUpdate("DELETE FROM t011_obj_serv_op_para where obj_serv_op_id = " + op.objServOpId);
			if (numProcessed > 0 && log.isDebugEnabled()) {
				log.debug("Removed operation params -> " + numProcessed + " entries from t011_obj_serv_op_para.");
			}
			numProcessed = jdbc.executeUpdate("DELETE FROM t011_obj_serv_op_platform where obj_serv_op_id = " + op.objServOpId);
			if (numProcessed > 0 && log.isDebugEnabled()) {
				log.debug("Removed operation platforms -> " + numProcessed + " entries from t011_obj_serv_op_platform.");
			}
		}

		// change class type
		jdbc.executeUpdate("UPDATE t01_object SET obj_class = " + NEW_OBJ_CLASS_INFORMATIONSSYSTEM +
			" where id = " + currentObj.objId);


		// No service type set ! Move object into working state, NO published object !!!

		// if published version different from working version we have to delete published version !
		if (currentObj.isPublished()) {
			log.warn("!!! Migrated object '" + currentObj.uuid + ":" + currentObj.name + "' SET TO WORKING STATE ! PLEASE EDIT service type and publish again !");

			if (currentObj.hasWorkingVersion()) {
				String msg = "!!! object '" + currentObj.uuid + ":" + currentObj.name + "' has to be unpublished and has separate WORKING VERSION, WE DELETE PUBLISHED VERSION !";
				System.out.println("\n" + msg + " See also log file (WARN).");
				log.warn(msg);

				// delete published version
				deleteObject(currentObj.objIdPublished);

			} else {
				// put published version in working state
				setObjectWorkingState(currentObj.objIdPublished);
			}
			
			// UNPUBLISH ! update object node (reset published id)
			setObjectNodeUnpublished(currentObj.objNodeId);
			stats.addObjNameUnpublished(currentObj.uuid + ":" + currentObj.name, false);
		}
	}

	protected int setObjectWorkingState(long objId) throws Exception {
		int numUpdated = jdbc.executeUpdate("UPDATE t01_object " +
			"SET work_state = '" + WORK_STATE_IN_BEARBEITUNG +
			"' where id = " + objId);
		return numUpdated;
	}
	protected int setObjectNodeUnpublished(long objNodeId) throws Exception {
		int numUpdated = jdbc.executeUpdate("UPDATE object_node " +
			"SET obj_id_published = NULL where id = " + objNodeId);
		return numUpdated;
	}
	protected void deleteObject(long objId) throws Exception {
		// for tracking in debugger !
		int numDeleted;

		numDeleted = jdbc.executeUpdate("DELETE FROM object_access where obj_id = " + objId);
		numDeleted = jdbc.executeUpdate("DELETE FROM object_comment where obj_id = " + objId);
		numDeleted = jdbc.executeUpdate("DELETE FROM object_conformity where obj_id = " + objId);
		numDeleted = jdbc.executeUpdate("DELETE FROM object_reference where obj_from_id = " + objId);
		numDeleted = jdbc.executeUpdate("DELETE FROM object_use where obj_id = " + objId);
		numDeleted = jdbc.executeUpdate("DELETE FROM searchterm_obj where obj_id = " + objId);
		numDeleted = jdbc.executeUpdate("DELETE FROM spatial_reference where obj_id = " + objId);
		numDeleted = jdbc.executeUpdate("DELETE FROM t011_obj_topic_cat where obj_id = " + objId);
		numDeleted = jdbc.executeUpdate("DELETE FROM t0110_avail_format where obj_id = " + objId);
		numDeleted = jdbc.executeUpdate("DELETE FROM t0112_media_option where obj_id = " + objId);
		numDeleted = jdbc.executeUpdate("DELETE FROM t0113_dataset_reference where obj_id = " + objId);
		numDeleted = jdbc.executeUpdate("DELETE FROM t0114_env_category where obj_id = " + objId);
		numDeleted = jdbc.executeUpdate("DELETE FROM t0114_env_topic where obj_id = " + objId);
		numDeleted = jdbc.executeUpdate("DELETE FROM t012_obj_adr where obj_id = " + objId);
		numDeleted = jdbc.executeUpdate("DELETE FROM t014_info_impart where obj_id = " + objId);
		numDeleted = jdbc.executeUpdate("DELETE FROM t015_legist where obj_id = " + objId);
		numDeleted = jdbc.executeUpdate("DELETE FROM t017_url_ref where obj_id = " + objId);
		numDeleted = jdbc.executeUpdate("DELETE FROM t08_attr where obj_id = " + objId);
		
		// ignore Objektklasse 1 t011_obj_geo...
		// ignore Objektklasse 2 t011_obj_literature
		// ignore Objektklasse 4 t011_obj_project
		// ignore Objektklasse 5 t011_obj_data...

		// delete Objektklasse 3 data !
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery("select id from t011_obj_serv where obj_id = " + objId, st);
		while (rs.next()) {
			long servId = rs.getLong("id");
			numDeleted = jdbc.executeUpdate("DELETE FROM t011_obj_serv_scale where obj_serv_id = " + servId);
			numDeleted = jdbc.executeUpdate("DELETE FROM t011_obj_serv_type where obj_serv_id = " + servId);
			numDeleted = jdbc.executeUpdate("DELETE FROM t011_obj_serv_url where obj_serv_id = " + servId);
			numDeleted = jdbc.executeUpdate("DELETE FROM t011_obj_serv_version where obj_serv_id = " + servId);

			Statement st2 = jdbc.createStatement();
			ResultSet rs2 = jdbc.executeQuery("select id from t011_obj_serv_operation where obj_serv_id = " + servId, st2);
			while (rs2.next()) {
				long servOpId = rs2.getLong("id");
				numDeleted = jdbc.executeUpdate("DELETE FROM t011_obj_serv_op_connpoint where obj_serv_op_id = " + servOpId);
				numDeleted = jdbc.executeUpdate("DELETE FROM t011_obj_serv_op_depends where obj_serv_op_id = " + servOpId);
				numDeleted = jdbc.executeUpdate("DELETE FROM t011_obj_serv_op_para where obj_serv_op_id = " + servOpId);
				numDeleted = jdbc.executeUpdate("DELETE FROM t011_obj_serv_op_platform where obj_serv_op_id = " + servOpId);
			}
			rs2.close();
			st2.close();

			numDeleted = jdbc.executeUpdate("DELETE FROM t011_obj_serv_operation where obj_serv_id = " + servId);
		}
		rs.close();
		st.close();

		numDeleted = jdbc.executeUpdate("DELETE FROM t011_obj_serv where obj_id = " + objId);

		// delete 1:1 associations (id in t01_object) !
		st = jdbc.createStatement();
		rs = jdbc.executeQuery("select obj_metadata_id from t01_object where id = " + objId, st);
		while (rs.next()) {
			long objMetadataId = rs.getLong("obj_metadata_id");
			numDeleted = jdbc.executeUpdate("DELETE FROM object_metadata where id = " + objMetadataId);
		}
		rs.close();
		st.close();

		numDeleted = jdbc.executeUpdate("DELETE FROM t01_object where id = " + objId);
	}
/*	
	protected void cleanUpDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure... done");
		}
	}
*/
	/** Helper class encapsulating statistics */
	class MigrationStatistics {
		int numGeo = 0;
		ArrayList<String> objNamesUnpublishedGeo = new ArrayList<String>();
		int numNonGeo = 0;
		ArrayList<String> objNamesUnpublishedNonGeo = new ArrayList<String>();

		void addObjNameUnpublished(String objNameUnpublished, boolean isGeoService) {
			List<String> myList = objNamesUnpublishedGeo;
			if (!isGeoService) {
				myList = objNamesUnpublishedNonGeo;				
			}
			if (!myList.contains(objNameUnpublished)) {
				myList.add(objNameUnpublished);				
			}
		}
		String getObjNamesUnpublishedAsString(boolean geoServiceList) {
			List<String> myList = objNamesUnpublishedGeo;
			if (!geoServiceList) {
				myList = objNamesUnpublishedNonGeo;				
			}
			String names = "";
			for (String name : myList) {
				names = names + name + ",\n";
			}
			return names;
		}
	}
	/** Helper class encapsulating all needed data of an object of former class 3 ("Dienst/Anwendung/Informationssystem") */
	class ServiceObject {
		long objNodeId;
		String uuid;
		long objId;
		long objIdPublished;
		String name;
		long objServId;
		int typeKey;
		String typeValue;
		ArrayList<Integer> classificKeys;
		/** key = operation ID */
		HashMap<Long, ServiceOperation> operations;

		ServiceObject(long objNodeId, String objUuid, long objId, long objIdPublished,
				String name, long objServId, int typeKey, String typeValue) {
			this.objNodeId = objNodeId;
			this.uuid = objUuid;
			this.objId = objId;
			this.objIdPublished = objIdPublished;
			this.name = name;
			this.objServId = objServId;
			this.typeKey = typeKey;
			this.typeValue = typeValue;
			this.classificKeys = new ArrayList<Integer>();
			this.operations = new HashMap<Long, ServiceOperation>();
		}
		void addClassificKey(int classificKey) {
			if (!classificKeys.contains(classificKey)) {
				classificKeys.add(classificKey);				
			}
		}
		void addOperation(long operationId, String name, String description, String url) {
			// id may be 0 due to outer join fetching (when null in select)
			if (operationId > 0 && url != null && url.trim().length() > 0) {
				ServiceOperation op = operations.get(operationId);
				if (op == null) {
					operations.put(operationId, new ServiceOperation(operationId, name, description, url));
				} else {
					op.addUrl(url);
				}				
			}
		}
		Collection<ServiceOperation> getOperations() {
			return operations.values();
		}
		String getClassificationKeysAsString() {
			String classKeys = "";
			for (Integer key : classificKeys) {
				classKeys = classKeys + key + ",";
			}
			return classKeys;
		}
		boolean isGeoService() {
			if (typeKey == TYPE_KEY_OTHER_SERVICE) {
				return false;
/*
				if (classificKeys.contains(CLASSIFIC_KEY_NON_GEO_SERVICE)) {
					return false;
				} else {
					// ??? type is "other service" and classification does not contain NON_GEO_SERVICE
					// -> we return false -> is NOT a geo service !!!???
					return false;
				}
*/
			} else {
				// typeKey may be 0 if database value is null

				if (typeKey > 0) {
					// type set and is NOT "other service" -> is a geo service ! Ignore classification !
					return true;
				} else {
					// type NOT set
					return false;
/*
					// no type set, we check classification
					if (classificKeys.contains(CLASSIFIC_KEY_NON_GEO_SERVICE)) {
						return false;
					} else {
						// ??? no type set and classification does not contain NON_GEO_SERVICE
						// -> we return false -> is NOT a geo service !!!???
						return false;
					}
*/
				}
			}
		}
		boolean isPublished() {
			// if obj_id_published = null then jdbc returns 0 !
			if (objIdPublished == 0) {
				return false;
			}
			return true;
		}
		boolean hasWorkingVersion() {
			// NOTICE: objId always set, never 0 (null) !
			if (objId == objIdPublished) {
				return false;
			}
			return true;
		}
	}
	/** Helper class encapsulating all needed data of a former service operation (url and description) */
	class ServiceOperation {
		long objServOpId;
		String name;
		String description;
		ArrayList<String> urls;

		ServiceOperation(long id, String name, String description, String url) {
			this.objServOpId = id;
			this.name = (name == null) ? "" : name.trim();
			this.description = (description == null) ? "" : description.trim();
			this.urls = new ArrayList<String>();
			addUrl(url);
		}
		void addUrl(String url) {
			if (url != null &&
					url.trim().length() > 0 && 
					!urls.contains(url)) {
				urls.add(url);
			}
		}
	}
}
