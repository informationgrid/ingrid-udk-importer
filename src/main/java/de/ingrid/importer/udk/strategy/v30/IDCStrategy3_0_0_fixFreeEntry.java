/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2020 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v30;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.importer.udk.jdbc.JDBCHelper;
import de.ingrid.importer.udk.strategy.IDCStrategy;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * Changes InGrid 3.0:<p>
 * - Höhe Vertikaldatum: allow free entries, see https://dev.wemove.com/jira/browse/INGRID23-59
 */
public class IDCStrategy3_0_0_fixFreeEntry extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_0_0_fixFreeEntry.class);

    /**
     * Deliver NO Version, this strategy should NOT trigger a strategy workflow (of missing former
     * versions) and can be executed on its own !
     * NOTICE: BUT may be executed in workflow (part of workflow array) !
     * @see de.ingrid.importer.udk.strategy.IDCStrategy#getIDCVersion()
     */
    public String getIDCVersion() {
        return null;
    }

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit (e.g. on MySQL)
		// ---------------------------------

		System.out.print("  Extend datastructure...");
		extendDataStructure();
		System.out.println("done.");

		// THEN PERFORM DATA MANIPULATIONS !
		// ---------------------------------

		System.out.print("  Updating t01_object...");
		updateT01Object();
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
			log.info("Add columns vertical_extent_vdatum_key/_value to table 't01_object' ...");
		}
		jdbc.getDBLogic().addColumn("vertical_extent_vdatum_key", ColumnType.INTEGER, "t01_object", false, null, jdbc);
		jdbc.getDBLogic().addColumn("vertical_extent_vdatum_value", ColumnType.VARCHAR255, "t01_object", false, null, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Extending datastructure... done");
		}
	}

	private void updateT01Object() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating t01_object...");
		}

		if (log.isInfoEnabled()) {
			log.info("Map old vertical_extent_vdatum to new vertical_extent_vdatum_key/_value ...");
		}

		// select via node to also update index 
		String sql = "select distinct objNode.id as objNodeId, objNode.obj_id as objIdWorking, " +
			"obj.id as objId, obj.vertical_extent_vdatum, obj.obj_uuid " +
			"from t01_object obj, object_node objNode " +
			"where obj.obj_uuid = objNode.obj_uuid";

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		Set<Long> processedNodeIds = new HashSet<Long>();
		int numProcessed = 0;
		String catalogLanguage = getCatalogLanguageFromDescriptor();
//		Integer igcLangCode = UtilsLanguageCodelist.getCodeFromShortcut(catalogLanguage);

		while (rs.next()) {
			long objNodeId = rs.getLong("objNodeId");
			long objIdWorking = rs.getLong("objIdWorking");
			long objId = rs.getLong("objId");
			String objUuid = rs.getString("obj_uuid");
			int verticalExtentVdatumKey = rs.getInt("vertical_extent_vdatum");
			String verticalExtentVdatumValue = null;

			// determine languages
			if (verticalExtentVdatumKey <= 0) {
				continue;
			}

			verticalExtentVdatumValue = readSyslistValue(101, verticalExtentVdatumKey, catalogLanguage);				

			jdbc.executeUpdate("UPDATE t01_object SET " +
				"vertical_extent_vdatum_key = " + verticalExtentVdatumKey +
				", vertical_extent_vdatum_value = '" + verticalExtentVdatumValue + "'" +
				" WHERE id = " + objId);
			
			// Node may contain different object versions, then we receive nodeId multiple times.
			// Write Index only once (index contains data of working version!) !
			if (!processedNodeIds.contains(objNodeId) && objIdWorking == objId) {
				JDBCHelper.updateObjectIndex(objNodeId, String.valueOf(verticalExtentVdatumKey), jdbc);
				JDBCHelper.updateObjectIndex(objNodeId, verticalExtentVdatumValue, jdbc);
				
				processedNodeIds.add(objNodeId);
			}

			numProcessed++;
			if (log.isDebugEnabled()) {
				log.debug("Object " + objUuid + " updated " +
					"vertical_extent_vdatum: '" + verticalExtentVdatumKey + "' --> '" + verticalExtentVdatumKey + "'/'" + verticalExtentVdatumValue + "'");
			}
		}
		rs.close();
		st.close();

		if (log.isInfoEnabled()) {
			log.info("Updated " + numProcessed + " objects... done");
			log.info("Updating t01_object... done");
		}
	}

	private void cleanUpDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Drop column vertical_extent_vdatum from table 't01_object' ...");
		}
		jdbc.getDBLogic().dropColumn("vertical_extent_vdatum", "t01_object", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure... done");
		}
	}

	/** Read value of syslist entry from database !
	 * @param listId id of syslist
	 * @param entryId id of antry
	 * @param language language of entry
	 * @return returns null if not found
	 * @throws Exception
	 */
	protected String readSyslistValue(int listId, int entryId, String language) throws Exception {
		String retValue = null;

		sqlStr = "SELECT name FROM sys_list WHERE lst_id = ? and entry_id = ? and lang_id = ?";
		PreparedStatement ps = jdbc.prepareStatement(sqlStr);
		ps.setInt(1, listId);
		ps.setInt(2, entryId);
		ps.setString(3, language);

		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			retValue = rs.getString("name");
		}
		rs.close();
		ps.close();

		return retValue;
	}
}
