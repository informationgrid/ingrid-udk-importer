/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;

/**
 * InGrid 2.3, statische Änderungen:
 * - Erstellung einer eigenen Klasse für geographische Services (extend t011_obj_serv, add t011_obj_serv_url, manipulate syslists ...)
 */
public class IDCStrategy1_0_9 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy1_0_9.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_109;

	final public static int TYPE_KEY_OTHER_SERVICE = 6;
	final public static int CLASSIFIC_KEY_NON_GEO_SERVICE = 901;

	final public static int NEW_OBJ_CLASS_INFORMATIONSSYSTEM = 6;

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// then write version of IGC structure !
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

		System.out.print("  Migrate services to new classes 'Geodatendienst' / 'Informationssystem/Dienst/Anwendung'...");
		migrateServices();
		System.out.println("done.");

		System.out.print("  Clean up sys_list...");
		cleanUpSysList();
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
			log.info("Add columns 'has_access_constraint' + 'name' to table 't011_obj_serv' ...");
		}
		jdbc.getDBLogic().addColumn("has_access_constraint", ColumnType.VARCHAR1, "t011_obj_serv", false, "'N'", jdbc);
		jdbc.getDBLogic().addColumn("name", ColumnType.VARCHAR255, "t011_obj_serv", false, null, jdbc);

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
			log.warn("New Syslist " + lstId +	" EXISTED ! We deleted old values !");			
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

	protected void migrateServices() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Migrate 'Dienst/Anwendung/Informationssystem' to new classes 'Geodatendienst' / 'Informationssystem/Dienst/Anwendung'...");
		}

		String sql = "select " +
			"obj.id as objId, obj.obj_name, " + // object
			"objServ.id as objServId, objServ.type_key as typeKey, objServ.type_value as typeValue, " + // type
			"objServType.serv_type_key as classificKey, " + // classification
			"objServOp.id as opId, objServOp.name_value as opName, objServOp.descr as opDescr, " + // operation
			"objServOpConn.connect_point as opUrl " + // operation url
			"from " +
			"t01_object obj " +
			"left outer join t011_obj_serv objServ on (obj.id = objServ.obj_id) " +
			"left outer join t011_obj_serv_type objServType on (objServ.id = objServType.obj_serv_id) " +
			"left outer join t011_obj_serv_operation objServOp on (objServ.id = objServOp.obj_serv_id) " +
			"left outer join t011_obj_serv_op_connpoint objServOpConn on (objServOp.id = objServOpConn.obj_serv_op_id) " +
			"where " +
			"obj.obj_class = 3 " +
			"order by objId, objServId, objServType.line, objServOp.line, objServOpConn.line";

		MigrationStatistics stats = new MigrationStatistics();
		ServiceObject currentObj = null;

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			long nextObjId = rs.getLong("objId");
			
			// check whether all data of an object is read, then do migration !
			boolean objChange = false;
			if (currentObj != null && currentObj.objId != nextObjId) {
				// object changed, process finished object
				objChange = true;
				migrateObject(currentObj, stats);
			}
			
			if (currentObj == null || objChange) {
				// set up next object
				currentObj = new ServiceObject(nextObjId, rs.getString("obj_name"),
					rs.getLong("objServId"), rs.getInt("typeKey"), rs.getString("typeValue"));
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

		if (log.isInfoEnabled()) {
			log.info("Migrated " + stats.numGeo + " objects to class 'Geodatendienst'");
			log.info("Migrated " + stats.numNonGeo + " objects to class 'Informationssystem/Dienst/Anwendung'");
		}

		if (log.isInfoEnabled()) {
			log.info("Migrate 'Dienst/Anwendung/Informationssystem' to new classes 'Geodatendienst' / 'Informationssystem/Dienst/Anwendung'... done");
		}
	}

	/** Migrate the given object. Pass Integer for counting. 
	 * @param currentObj obj to migrate
	 * @param stats statistics object, will be updated
	 * @throws Exception
	 */
	protected void migrateObject(ServiceObject currentObj, MigrationStatistics stats) throws Exception {
		if (currentObj.isGeoService()) {
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

				log.warn("!!! object '" + currentObj.name + "' of service type '" + currentObj.typeValue +
					"' and classification keys '" + currentObj.getClassificationKeysAsString() +
					"' is classified as 'Non Geographic Service' (901)! We delete this classification and migrate to new class 'Geodatendienst' !");

				int numDeleted = jdbc.executeUpdate("DELETE FROM t011_obj_serv_type where obj_serv_id = " + currentObj.objServId +
					" AND serv_type_key = " + CLASSIFIC_KEY_NON_GEO_SERVICE);
				if (log.isDebugEnabled()) {
					log.debug("Removed " + numDeleted +	" entries from t011_obj_serv_type.");
				}
			}

			// TODO: Check whether classification is present ! If not move object into working state, NO published object !!!?

			// NOTHING TO CHANGE, class number and data stays the same :) !
			// NOTICE: may still have classification entry 901 WHICH IS REMOVED FROM SYSLIST ('Non Geographic Service')!

		} else {
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
				String description = op.name + " : " + op.description;
				for (String url : op.urls) {
					jdbc.executeUpdate("INSERT INTO t011_obj_serv_url (id, obj_serv_id, line, url, description) "
						+ "VALUES (" + getNextId() + ", " + currentObj.objServId + ", " + line + ", '" 
						+ url + "', '" + description + "')");							
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

			// TODO: No service type set ! Move object into working state, NO published object !!!?
		}
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
		int numNonGeo = 0;		
	}
	/** Helper class encapsulating all needed data of an object of former class 3 ("Dienst/Anwendung/Informationssystem") */
	class ServiceObject {
		long objId;
		String name;
		long objServId;
		int typeKey;
		String typeValue;
		ArrayList<Integer> classificKeys;
		/** key = operation ID */
		HashMap<Long, ServiceOperation> operations;

		ServiceObject(long objId, String name, long objServId, int typeKey, String typeValue) {
			this.objId = objId;
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
