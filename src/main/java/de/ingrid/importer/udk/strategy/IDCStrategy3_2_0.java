/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;

/**
 * <p>
 * Changes InGrid 3.2:<p>
 * <ul>
 *   <li> adding NEW syslists for "Spezifikation der Konformität" (6005) and "Nutzungsbedingungen"
 *   (6020), modify according tables (add _key/_value), see https://dev.wemove.com/jira/browse/INGRID32-28
 * </ul>
 */
public class IDCStrategy3_2_0 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_2_0.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_2_0;

	int SYSLIST_ENTRY_ID_NO_INSPIRE_THEME = 99999;
	int INSPIRE_ENCODING_DEFAULT_KEY;
	String INSPIRE_ENCODING_DEFAULT_VALUE;

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

		System.out.print("  Extending sys_list (new ones)...");
		extendSysList();
		System.out.println("done.");

		System.out.print("  Updating object_use...");
		updateObjectUse();
		System.out.println("done.");

		System.out.print("  Updating object_conformity...");
		updateObjectConformity();
		System.out.println("done.");

		// FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause commit (e.g. on MySQL)
		// ---------------------------------

		System.out.print("  Clean up datastructure...");
		cleanUpDataStructure();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void extendDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Extending datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Add columns 'terms_of_use_key/_value' to table 'object_use' ...");
		}
		jdbc.getDBLogic().addColumn("terms_of_use_key", ColumnType.INTEGER, "object_use", false, null, jdbc);
		// we use TEXT_NO_CLOB because current free entries ARE > 255 chars !
		jdbc.getDBLogic().addColumn("terms_of_use_value", ColumnType.TEXT_NO_CLOB, "object_use", false, null, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Add columns 'specification_key/_value' to table 'object_conformity' ...");
		}
		jdbc.getDBLogic().addColumn("specification_key", ColumnType.INTEGER, "object_conformity", false, null, jdbc);
		// we use TEXT_NO_CLOB because free entries may be > 255 !
		jdbc.getDBLogic().addColumn("specification_value", ColumnType.TEXT_NO_CLOB, "object_conformity", false, null, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Extending datastructure... done");
		}
	}

	protected void extendSysList() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Extending sys_list...");
		}

// ---------------------------
		int lstId = 6005;
		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" = \"Spezifikation der Konformität\"...");
		}

		// german syslist
		LinkedHashMap<Integer, String> newSyslistMap_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_de.put(1, "INSPIRE Data Specification on Addresses – Guidelines");
		newSyslistMap_de.put(2, "INSPIRE Data Specification on Administrative units --Guidelines");
		newSyslistMap_de.put(3, "INSPIRE Data Specification on Cadastral parcels --Guidelines");
		newSyslistMap_de.put(4, "INSPIRE Data Specification on Geographical names – Guidelines");
		newSyslistMap_de.put(5, "INSPIRE Data Specification on Hydrography – Guidelines");
		newSyslistMap_de.put(6, "INSPIRE Data Specification on Protected Sites – Guidelines");
		newSyslistMap_de.put(7, "INSPIRE Data Specification on Transport Networks – Guidelines");
		newSyslistMap_de.put(8, "INSPIRE Specification on Coordinate Reference Systems – Guidelines");
		newSyslistMap_de.put(9, "INSPIRE Specification on Geographical Grid Systems – Guidelines");
		newSyslistMap_de.put(10, "INSPIRE Durchführungsbestimmung Netzdienste");
		newSyslistMap_de.put(11, "INSPIRE Durchführungsbestimmung Metadaten");
		newSyslistMap_de.put(12, "INSPIRE Durchführungsbestimmung Interoperabilität von Geodatensätzen und --diensten");
		newSyslistMap_de.put(13, "INSPIRE Richtline");
		// english syslist
		LinkedHashMap<Integer, String> newSyslistMap_en = new LinkedHashMap<Integer, String>();
		newSyslistMap_en.put(1, "INSPIRE Data Specification on Addresses – Guidelines");
		newSyslistMap_en.put(2, "INSPIRE Data Specification on Administrative units --Guidelines");
		newSyslistMap_en.put(3, "INSPIRE Data Specification on Cadastral parcels --Guidelines");
		newSyslistMap_en.put(4, "INSPIRE Data Specification on Geographical names – Guidelines");
		newSyslistMap_en.put(5, "INSPIRE Data Specification on Hydrography – Guidelines");
		newSyslistMap_en.put(6, "INSPIRE Data Specification on Protected Sites – Guidelines");
		newSyslistMap_en.put(7, "INSPIRE Data Specification on Transport Networks – Guidelines");
		newSyslistMap_en.put(8, "INSPIRE Specification on Coordinate Reference Systems – Guidelines");
		newSyslistMap_en.put(9, "INSPIRE Specification on Geographical Grid Systems – Guidelines");
		newSyslistMap_en.put(10, "INSPIRE Durchführungsbestimmung Netzdienste");
		newSyslistMap_en.put(11, "INSPIRE Durchführungsbestimmung Metadaten");
		newSyslistMap_en.put(12, "INSPIRE Durchführungsbestimmung Interoperabilität von Geodatensätzen und --diensten");
		newSyslistMap_en.put(13, "INSPIRE Richtline");

		writeNewSyslist(lstId, newSyslistMap_de, newSyslistMap_en, 13);
// ---------------------------
		lstId = 6020;
		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" = \"Nutzungsbedingungen\"...");
		}

		// german syslist
		newSyslistMap_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_de.put(1, "Keine");
		// english syslist
		newSyslistMap_en = new LinkedHashMap<Integer, String>(); 
		newSyslistMap_en.put(1, "No conditions apply");

		writeNewSyslist(lstId, newSyslistMap_de, newSyslistMap_en, 1);
// ---------------------------

		if (log.isInfoEnabled()) {
			log.info("Extending sys_list... done");
		}
	}

	/**
	 * @param defaultEntry pass key of default entry or < 0 if no default entry !
	 * @throws Exception
	 */
	private void writeNewSyslist(int listId,
			LinkedHashMap<Integer, String> syslistMap_de,
			LinkedHashMap<Integer, String> syslistMap_en,
			int defaultEntry) throws Exception {

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = " + listId;
		jdbc.executeUpdate(sqlStr);

		Iterator<Integer> itr = syslistMap_de.keySet().iterator();
		while (itr.hasNext()) {
			int key = itr.next();
			String isDefault = "N";
			if (key == defaultEntry) {
				isDefault = "Y";				
			}
			// german version
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
					+ getNextId() + ", " + listId + ", " + key + ", 'de', '" + syslistMap_de.get(key) + "', 0, '" + isDefault + "')");
			// english version
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
					+ getNextId() + ", " + listId + ", " + key + ", 'en', '" + syslistMap_en.get(key) + "', 0, '" + isDefault + "')");
		}
	}

	private void updateObjectUse() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating object_use...");
		}

		if (log.isInfoEnabled()) {
			log.info("Transfer old 'terms_of_use' as free entry to new 'terms_of_use_key/_value' ...");
		}
		
		// NOTICE: No mapping of former values to new syslists. Every value becomes a free entry !!!
		// We "keep" type TEXT_NO_CLOB of values, so we do not have to reduce size !
		// But we copy every entry, to avoid database problems (e.g. on ORACLE we transfer CLOB -> VARCHAR(4000))
		// We do NOT update search index due to same values (but keep that commented !)

		String sql = "select id as objectUseId, terms_of_use from object_use";
/*
		// We read from node to determine working version to update search index ! 
		String sql = "select objNode.id as objNodeId, objNode.obj_id as objIdWorking, " +
				"obj.id as objId, obj.obj_uuid, " +
				"objectUse.id as objectUseId, objectUse.terms_of_use " +
				"from object_node objNode, t01_object obj, object_use objectUse " +
				"where objNode.obj_uuid = obj.obj_uuid " +
				"and obj.id = objectUse.obj_id";
*/
		// use PreparedStatement to avoid problems when value String contains "'" !!!
		String psSql = "UPDATE object_use SET " +
				"terms_of_use_key = -1, " +
				"terms_of_use_value = ? " +
				"WHERE id = ?";		
		PreparedStatement psUpdate = jdbc.prepareStatement(psSql);

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
//		Set<Long> processedNodeIds = new HashSet<Long>();
		int numProcessed = 0;
		while (rs.next()) {
//			long objNodeId = rs.getLong("objNodeId");
//			long objIdWorking = rs.getLong("objIdWorking");
//			long objId = rs.getLong("objId");
//			String objUuid = rs.getString("obj_uuid");
			long objectUseId = rs.getLong("objectUseId");
			String termsOfUseText = rs.getString("terms_of_use");

			String termsOfUseVarchar = termsOfUseText;
/*
			if (termsOfUseText != null && termsOfUseText.length() > 255) {
				termsOfUseVarchar = termsOfUseText.substring(0, 255);
				if (log.isWarnEnabled()) {
					log.warn("Object '" + objUuid +	"', we reduce terms_of_use TEXT: '" + 
						termsOfUseText + "' --> VARCHAR255: '" + termsOfUseVarchar + "'");
				}
			}
*/
			psUpdate.setString(1, termsOfUseVarchar);
			psUpdate.setLong(2, objectUseId);
			psUpdate.executeUpdate();
/*
			// Node may contain different object versions, then we receive nodeId multiple times.
			// Write Index only once (index contains data of working version!) !
			if (!processedNodeIds.contains(objNodeId) && objIdWorking == objId) {
				JDBCHelper.updateObjectIndex(objNodeId, termsOfUseVarchar, jdbc);

				processedNodeIds.add(objNodeId);
			}
*/
			numProcessed++;
			if (log.isDebugEnabled()) {
//				log.debug("Object " + objUuid + " updated terms_of_use: '" + 
				log.debug("Updated terms_of_use: '" + termsOfUseText + "' --> '-1'/'" + termsOfUseVarchar + "'");
			}
		}
		rs.close();
		st.close();
		psUpdate.close();

		if (log.isInfoEnabled()) {
			log.info("Updated " + numProcessed + " entries... done");
			log.info("Updating object_use... done");
		}
	}

	private void updateObjectConformity() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating object_conformity...");
		}

		if (log.isInfoEnabled()) {
			log.info("Transfer old 'specification' as free entry to new 'specification_key/_value' ...");
		}

		// NOTICE: No mapping of former values to new syslists. Every value becomes a free entry !!!
		// We "keep" type TEXT_NO_CLOB of values, so we do not have to reduce size !
		// But we copy every entry, to avoid database problems (e.g. on ORACLE we transfer CLOB -> VARCHAR(4000))
		// We do NOT update search index due to same values.

		String sql = "select id, specification from object_conformity";

		// use PreparedStatement to avoid problems when value String contains "'" !!!
		String psSql = "UPDATE object_conformity SET " +
				"specification_key = -1, " +
				"specification_value = ? " +
				"WHERE id = ?";		
		PreparedStatement psUpdate = jdbc.prepareStatement(psSql);

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		int numProcessed = 0;
		while (rs.next()) {
			long id = rs.getLong("id");
			String specification = rs.getString("specification");

			psUpdate.setString(1, specification);
			psUpdate.setLong(2, id);
			psUpdate.executeUpdate();

			numProcessed++;
			if (log.isDebugEnabled()) {
				log.debug("Updated specification: '" + specification + "' --> '-1'/'" + specification + "'");
			}
		}
		rs.close();
		st.close();
		psUpdate.close();

		if (log.isInfoEnabled()) {
			log.info("Updated " + numProcessed + " entries... done");
			log.info("Updating object_conformity... done");
		}
	}

	private void cleanUpDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Drop column 'terms_of_use' from table 'object_use' ...");
		}
		jdbc.getDBLogic().dropColumn("terms_of_use", "object_use", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Drop columns 'specification', 'publication_date' from table 'object_conformity' ...");
		}
		jdbc.getDBLogic().dropColumn("specification", "object_conformity", jdbc);
		jdbc.getDBLogic().dropColumn("publication_date", "object_conformity", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure... done");
		}
	}
}
