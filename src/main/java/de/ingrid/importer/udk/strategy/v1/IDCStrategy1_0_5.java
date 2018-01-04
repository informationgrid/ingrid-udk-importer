/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2018 wemove digital solutions GmbH
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.importer.udk.jdbc.JDBCHelper;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.importer.udk.util.UtilsCountryCodelist;
import de.ingrid.importer.udk.util.UtilsLanguageCodelist;

/**
 * IGC Update: Post InGrid 2.0 release
 * introduce:
 * - country syslist (country_key/_value)
 * - language syslist (language_key/_value)
 */
public class IDCStrategy1_0_5 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy1_0_5.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_105;

	private final String OLD_COUNTRY_CODE_GERMANY = "de";
	private final String OLD_COUNTRY_ZIP_CODE_GERMANY = "D";
	
	/** catalog language: will be determined. Default is "de" */
	private String catalogLanguageShortcut = "de";

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

		System.out.print("  Updating sys_list...");
		updateSysList();
		System.out.println("done.");

		// FIRST UPDATE CATALOG ! ALSO DETERMINES CATALOG LANGUAGE accessed from following methods !
		System.out.print("  Updating t03_catalogue...");
		updateT03Catalogue();
		System.out.println("done.");

		System.out.print("  Updating t02_address...");
		updateT02Address();
		System.out.println("done.");

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
			log.info("Add columns country_key/_value, language_key/_value to table 't03_catalogue' ...");
		}
		jdbc.getDBLogic().addColumn("country_key", ColumnType.INTEGER, "t03_catalogue", false, null, jdbc);
		jdbc.getDBLogic().addColumn("country_value", ColumnType.VARCHAR255, "t03_catalogue", false, null, jdbc);
		jdbc.getDBLogic().addColumn("language_key", ColumnType.INTEGER, "t03_catalogue", false, null, jdbc);
		jdbc.getDBLogic().addColumn("language_value", ColumnType.VARCHAR255, "t03_catalogue", false, null, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Add columns country_key/_value to table 't02_address' ...");
		}
		jdbc.getDBLogic().addColumn("country_key", ColumnType.INTEGER, "t02_address", false, null, jdbc);
		jdbc.getDBLogic().addColumn("country_value", ColumnType.VARCHAR255, "t02_address", false, null, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Add columns data_language_key/_value, metadata_language_key/_value to table 't01_object' ...");
		}
		jdbc.getDBLogic().addColumn("data_language_key", ColumnType.INTEGER, "t01_object", false, null, jdbc);
		jdbc.getDBLogic().addColumn("data_language_value", ColumnType.VARCHAR255, "t01_object", false, null, jdbc);
		jdbc.getDBLogic().addColumn("metadata_language_key", ColumnType.INTEGER, "t01_object", false, null, jdbc);
		jdbc.getDBLogic().addColumn("metadata_language_value", ColumnType.VARCHAR255, "t01_object", false, null, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Extending datastructure... done");
		}
	}

	private void updateSysList() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating sys_list...");
		}

		// ---------------------------------------------

		int lstId = UtilsCountryCodelist.COUNTRY_SYSLIST_ID;
		if (log.isInfoEnabled()) {
			log.info("Updating syslist " + lstId +	" Country ...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = " + lstId;
		jdbc.executeUpdate(sqlStr);

		// german syslist
		HashMap<Integer, String> newSyslistCountry_de = UtilsCountryCodelist.countryCodelist_de;
		// english syslist
		HashMap<Integer, String> newSyslistCountry_en = UtilsCountryCodelist.countryCodelist_en;

		Iterator<Integer> itr = newSyslistCountry_de.keySet().iterator();
		while (itr.hasNext()) {
			Integer key = itr.next();
			// german version
			String isDefault = (key.equals(UtilsCountryCodelist.NEW_COUNTRY_KEY_GERMANY)) ? "'Y'" : "'N'";
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", " + key + ", 'de', '" + newSyslistCountry_de.get(key) + "', 0, " + isDefault + ")");
			// english version
			isDefault = (key.equals(UtilsCountryCodelist.NEW_COUNTRY_KEY_GBR)) ? "'Y'" : "'N'";
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", " + key + ", 'en', '" + newSyslistCountry_en.get(key) + "', 0, " + isDefault + ")");
		}

		// ---------------------------------------------

		lstId = UtilsLanguageCodelist.LANGUAGE_SYSLIST_ID;
		if (log.isInfoEnabled()) {
			log.info("Updating syslist " + lstId +	" Language ...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = " + lstId;
		jdbc.executeUpdate(sqlStr);

		// german syslist
		HashMap<Integer, String> newSyslistLanguage_de = UtilsLanguageCodelist.languageCodelist_de;
		// english syslist
		HashMap<Integer, String> newSyslistLanguage_en = UtilsLanguageCodelist.languageCodelist_en;

		itr = newSyslistLanguage_de.keySet().iterator();
		while (itr.hasNext()) {
			Integer key = itr.next();
			// german version
			String isDefault = (key.equals(UtilsLanguageCodelist.IGC_CODE_GERMAN)) ? "'Y'" : "'N'";
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", " + key + ", 'de', '" + newSyslistLanguage_de.get(key) + "', 0, " + isDefault + ")");
			// english version
			isDefault = (key.equals(UtilsLanguageCodelist.IGC_CODE_ENGLISH)) ? "'Y'" : "'N'";
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", " + key + ", 'en', '" + newSyslistLanguage_en.get(key) + "', 0, " + isDefault + ")");
		}

		if (log.isInfoEnabled()) {
			log.info("Updating sys_list... done");
		}
	}

	private void updateT03Catalogue() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating t03_catalogue...");
		}

		if (log.isInfoEnabled()) {
			log.info("Map old country_code, language_code to new country_key/_value, language_key/_value ...");
		}

		// then add entries for ALL t01_objects (no matter whether working or published version) 
		String sql = "select distinct id, cat_name, country_code, language_code from t03_catalogue";

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			long catId = rs.getLong("id");
			String catName = rs.getString("cat_name");
			String catalogCountryShortcut = rs.getString("country_code");
			catalogLanguageShortcut = rs.getString("language_code");

			// determine country, default is "germany". Can be edited via IGE.
			Integer newCountryCode = UtilsCountryCodelist.getCodeFromShortcut(catalogCountryShortcut);
			if (newCountryCode == null) {
				log.error("!!! Problems determining country of catalog from t03_catalogue.country_code '" +
					catalogCountryShortcut + "' ! We set country to GERMANY !");
				newCountryCode = UtilsCountryCodelist.NEW_COUNTRY_KEY_GERMANY;
			}
			String newCountryName = UtilsCountryCodelist.getNameFromCode(newCountryCode, catalogLanguageShortcut);
			
			// determine language
			Integer newLangCode = UtilsLanguageCodelist.getCodeFromShortcut(catalogLanguageShortcut);
			String newLangName = UtilsLanguageCodelist.getNameFromCode(newLangCode, catalogLanguageShortcut);

			// update
			jdbc.executeUpdate("UPDATE t03_catalogue SET " +
					"country_key = " + newCountryCode +
					", country_value = '" + newCountryName + "'" +
					", language_key = " + newLangCode +
					", language_value = '" + newLangName + "' " +
					" WHERE id = " + catId);


			if (log.isInfoEnabled()) {
				log.info("Updated catalog " + catName + " to " +
					"country: '" + newCountryCode + "'/'" + newCountryName + "'" +
					", language:" + newLangCode + "'/'" + newLangName + "'");
			}
		}
		rs.close();
		st.close();

		if (log.isInfoEnabled()) {
			log.info("Updating t03_catalogue... done");
		}
	}

	private void updateT02Address() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating t02_address...");
		}

		if (log.isInfoEnabled()) {
			log.info("Map old 'country_code' to new 'country_key'/'country_value'...");
		}

		// then add entries for ALL t02_address (no matter whether working or published version) 
		String sql = "select distinct addrNode.id as addrNodeId, " +
			"addr.id as addrId, addr.country_code, addr.postcode, addr.postbox_pc, addr.adr_uuid " +
			"from t02_address addr, address_node addrNode " +
			"where addr.adr_uuid = addrNode.addr_uuid";

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		Set<Long> processedNodeIds = new HashSet<Long>();
		int numProcessed = 0;
		while (rs.next()) {
			String countryCode = rs.getString("country_code");

			long addrNodeId = rs.getLong("addrNodeId");
			long addrId = rs.getLong("addrId");
			String addrUuid = rs.getString("adr_uuid");
			String postcode = rs.getString("postcode");
			String postboxPostcode = rs.getString("postbox_pc");

			String newPostcode = null;
			String newPostboxPostcode = null;
			String sqlUpdatePlz = "";
			if (OLD_COUNTRY_CODE_GERMANY.equals(countryCode)) {
				if (postcode != null && postcode.trim().length() > 0) {
					newPostcode = OLD_COUNTRY_ZIP_CODE_GERMANY + "-" + postcode;
					sqlUpdatePlz = "postcode = '" + newPostcode + "', ";
				}
				if (postboxPostcode != null && postboxPostcode.trim().length() > 0) {
					newPostboxPostcode = OLD_COUNTRY_ZIP_CODE_GERMANY + "-" + postboxPostcode;
					sqlUpdatePlz += "postbox_pc = '" + newPostboxPostcode + "', ";
				}
			}

			// determine country, default is "germany".
			Integer newCountryCode = UtilsCountryCodelist.getCodeFromShortcut(countryCode);
			if (newCountryCode == null) {
				log.error("!!! Problems determining country from t02_address.country_code '" +
						countryCode + "' for address " + addrUuid +	" ! We set country to GERMANY !");
				newCountryCode = UtilsCountryCodelist.NEW_COUNTRY_KEY_GERMANY;
			}
			String newCountryName = UtilsCountryCodelist.getNameFromCode(newCountryCode, catalogLanguageShortcut);

			jdbc.executeUpdate("UPDATE t02_address SET " +
				sqlUpdatePlz +
				"country_key = " + newCountryCode +
				", country_value = '" + newCountryName + "'" +
				" WHERE id = " + addrId);
			
			// Node may contain different object versions, then we receive nodeId multiple times.
			// Write Index only once (index contains data of working version!) !
			if (!processedNodeIds.contains(addrNodeId)) {
				JDBCHelper.updateAddressIndex(addrNodeId, newCountryCode.toString(), jdbc);
				JDBCHelper.updateAddressIndex(addrNodeId, newCountryName, jdbc);
				if (newPostcode != null) {
					JDBCHelper.updateAddressIndex(addrNodeId, newPostcode, jdbc);					
				}
				if (newPostboxPostcode != null) {
					JDBCHelper.updateAddressIndex(addrNodeId, newPostboxPostcode, jdbc);					
				}
				
				processedNodeIds.add(addrNodeId);
			}

			numProcessed++;
			if (log.isDebugEnabled()) {
				log.debug("Address " + addrUuid + " updated from '" + countryCode +	"' to '" +
					newCountryCode + "'/'" + newCountryName + "'" +
					((newPostcode != null) ? (", postcode '" + newPostcode + "'") : "") +
					((newPostboxPostcode != null) ? (", postbox_pc '" + newPostboxPostcode + "'") : ""));
			}
		}
		rs.close();
		st.close();

		if (log.isInfoEnabled()) {
			log.info("Updated " + numProcessed + " addresses... done");
			log.info("Updating t02_address... done");
		}
	}

	private void updateT01Object() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating t01_object...");
		}

		if (log.isInfoEnabled()) {
			log.info("Map old data_language_code, metadata_language_code to new data_language_key/_value, metadata_language_key/_value ...");
		}

		// then add entries for ALL t01_objects (no matter whether working or published version) 
		String sql = "select distinct objNode.id as objNodeId, " +
			"obj.id as objId, obj.data_language_code, obj.metadata_language_code, obj.obj_uuid " +
			"from t01_object obj, object_node objNode " +
			"where obj.obj_uuid = objNode.obj_uuid";

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		Set<Long> processedNodeIds = new HashSet<Long>();
		int numProcessed = 0;
		while (rs.next()) {
			long objNodeId = rs.getLong("objNodeId");
			long objId = rs.getLong("objId");
			String objUuid = rs.getString("obj_uuid");
			String oldDataLanguageShortcut = rs.getString("data_language_code");
			String oldMetadataLanguageShortcut = rs.getString("metadata_language_code");

			// determine languages
			Integer newDataLanguageCode = UtilsLanguageCodelist.getCodeFromShortcut(oldDataLanguageShortcut);
			String newDataLanguageName = UtilsLanguageCodelist.getNameFromCode(newDataLanguageCode, catalogLanguageShortcut);
			Integer newMetadataLanguageCode = UtilsLanguageCodelist.getCodeFromShortcut(oldMetadataLanguageShortcut);
			String newMetadataLanguageName = UtilsLanguageCodelist.getNameFromCode(newMetadataLanguageCode, catalogLanguageShortcut);

			jdbc.executeUpdate("UPDATE t01_object SET " +
				"data_language_key = " + newDataLanguageCode +
				", data_language_value = '" + newDataLanguageName + "'" +
				", metadata_language_key = " + newMetadataLanguageCode +
				", metadata_language_value = '" + newMetadataLanguageName + "'" +
				" WHERE id = " + objId);
			
			// Node may contain different object versions, then we receive nodeId multiple times.
			// Write Index only once (index contains data of working version!) !
			if (!processedNodeIds.contains(objNodeId)) {
				JDBCHelper.updateObjectIndex(objNodeId, newDataLanguageCode.toString(), jdbc);
				JDBCHelper.updateObjectIndex(objNodeId, newDataLanguageName, jdbc);
				JDBCHelper.updateObjectIndex(objNodeId, newMetadataLanguageCode.toString(), jdbc);
				JDBCHelper.updateObjectIndex(objNodeId, newMetadataLanguageName, jdbc);
				
				processedNodeIds.add(objNodeId);
			}

			numProcessed++;
			if (log.isDebugEnabled()) {
				log.debug("Object " + objUuid + " updated " +
					"DataLanguage: '" + oldDataLanguageShortcut + "' --> '" + newDataLanguageCode + "'/'" + newDataLanguageName + "'" +
					", MetadataLanguage: '" + oldMetadataLanguageShortcut + "' --> '" + newMetadataLanguageCode + "'/'" + newMetadataLanguageName + "'");
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
			log.info("Drop columns country_code, language_code from table 't03_catalogue' ...");
		}
		jdbc.getDBLogic().dropColumn("country_code", "t03_catalogue", jdbc);
		jdbc.getDBLogic().dropColumn("language_code", "t03_catalogue", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Drop column country_code from table 't02_address' ...");
		}
		jdbc.getDBLogic().dropColumn("country_code", "t02_address", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Drop columns data_language_code, metadata_language_code from table 't01_object' ...");
		}
		jdbc.getDBLogic().dropColumn("data_language_code", "t01_object", jdbc);
		jdbc.getDBLogic().dropColumn("metadata_language_code", "t01_object", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure... done");
		}
	}
}
