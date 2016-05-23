/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2016 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v331;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.JDBCHelper;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 3.3.1<p>
 * <ul>
 *   <li>Migration: Migrate open data data to new Open Data checkbox and categories, see REDMINE-245
 * </ul>
 * Writes NEW Catalog Schema Version to catalog !
 */
public class IDCStrategy3_3_1_d extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_3_1_d.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_3_1_d;

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		// THEN PERFORM DATA MANIPULATIONS !
		// ---------------------------------
		System.out.print("  Migrate open data objects...");
		migrateOpenData();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void migrateOpenData() throws Exception {
		log.info("\nMigrate Open Data from additional fields...");

		PreparedStatement psUpdateCheckbox = jdbc.prepareStatement(
				"UPDATE t01_object SET " +
				"is_open_data = 'Y' " +
				"WHERE id = ?");

		PreparedStatement psSelectCategories = jdbc.prepareStatement(
				"SELECT list_item_id, data " +
				"FROM additional_field_data " +
				"WHERE obj_id = ? " +
				"AND field_key = 'opendata' " +
				"ORDER BY sort");

		PreparedStatement psInsertCategory = jdbc.prepareStatement(
				"INSERT INTO object_open_data_category " +
				"(id, obj_id, line, category_key, category_value) " +
				"VALUES (?,?,?,?,?)");

		// select node where object is working version (different published version is not indexed)
		PreparedStatement psSelectNodeForIndex = jdbc.prepareStatement(
				"SELECT id " +
				"FROM object_node " +
				"WHERE obj_id = ?"); // working version
		
		// Fetch all Open Data objects then process every object
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(
				"SELECT distinct obj.id, obj.obj_uuid, obj.obj_name " +
				"FROM t01_object obj, searchterm_obj sto, searchterm_value stv " +
				"WHERE obj.id = sto.obj_id " +
				"AND sto.searchterm_id = stv.id " +
				"AND stv.term = '#opendata_hh#' " +
				"ORDER BY obj.id"
				, st);

		int numObjProcessed = 0;
		while (rs.next()) {
			long objId = rs.getLong("id");
			String objUuid = rs.getString("obj_uuid");
			String objName = rs.getString("obj_name");

			// update open data checkbox
			psUpdateCheckbox.setLong(1, objId);
			int numUpdated = psUpdateCheckbox.executeUpdate();
			if (numUpdated > 0) {
				log.info("Set open data checkbox to 'Y' in " +
					"OBJECT [id:" + objId + "/uuid:" + objUuid + "/name:'" + objName + "') !");
			} else {
				log.warn("PROBLEMS setting open data checkbox to 'Y' in " +
					"OBJECT [id:" + objId + "/uuid:" + objUuid + "/name:'" + objName + "') !");
			}

			// update index, determine node id (where object is working version)
			psSelectNodeForIndex.setLong(1, objId);
			ResultSet rsNode = psSelectNodeForIndex.executeQuery();
			long objNodeId = 0;
			while (rsNode.next()) {
				objNodeId = rsNode.getLong("id");				
			}
			rsNode.close();

			// update index if node found (then object is working version) and checkbox set
			if (objNodeId > 0 && numUpdated > 0) {
				JDBCHelper.updateObjectIndex(objNodeId, "opendata", jdbc);
				JDBCHelper.updateObjectIndex(objNodeId, "open data", jdbc);
				log.debug("Updated Index with \"opendata|open data\" for " +
					"OBJECT [nodeId:" + objNodeId + "/id:" + objId + "/uuid:" + objUuid + "/name:'" + objName + "') !");
			}

			// transfer open data categories
			psSelectCategories.setLong(1, objId);
			ResultSet rsCategories = psSelectCategories.executeQuery();

			int line = 1;
			while (rsCategories.next()) {
				String entryId = rsCategories.getString("list_item_id");
				String entryValue = rsCategories.getString("data");

				try {
					// some values have space at end !
					entryValue = entryValue.trim();

					numUpdated = 0;
					psInsertCategory.setLong(1, getNextId()); // id
					psInsertCategory.setLong(2, objId); // obj_id
					psInsertCategory.setInt(3, line++); // line
					psInsertCategory.setInt(4, new Integer(entryId)); // category_key
					psInsertCategory.setString(5, entryValue); // category_value
					numUpdated = psInsertCategory.executeUpdate();
					if (numUpdated > 0) {
						log.info("Insert CATEGORY [key:" + entryId + "/value:'" + entryValue + "'] to " +
							"OBJECT [id:" + objId + "/uuid:" + objUuid + "/name:'" + objName + "') !");
					} else {
						log.warn("PROBLEMS inserting CATEGORY [key:" + entryId + "/value:'" + entryValue + "'] to " +
							"OBJECT [id:" + objId + "/uuid:" + objUuid + "/name:'" + objName + "') !");
					}
					
				} catch (Exception exc) {
					log.warn("PROBLEMS inserting CATEGORY [key:" + entryId + "/value:'" + entryValue + "'] to " +
						"OBJECT [id:" + objId + "/uuid:" + objUuid + "/name:'" + objName + "') !", exc);					
				}


				// update index if node found (then object is working version) and category added
				if (objNodeId > 0 && numUpdated > 0) {
					JDBCHelper.updateObjectIndex(objNodeId, entryValue, jdbc);
					log.debug("Updated Index with \"" + entryValue + "\" for " +
						"OBJECT [nodeId:" + objNodeId + "/id:" + objId + "/uuid:" + objUuid + "/name:'" + objName + "') !");
				}
			}
			rsCategories.close();

			numObjProcessed++;
		}
		psUpdateCheckbox.close();
		psSelectNodeForIndex.close();
		psSelectCategories.close();
		psInsertCategory.close();
		st.close();

		log.info("Processed " + numObjProcessed + " objects setting open data data");
		log.info("Migrate Open Data from additional fields... done\n");
	}
}
