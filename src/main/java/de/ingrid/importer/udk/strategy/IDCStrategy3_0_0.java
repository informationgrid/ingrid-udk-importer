/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.mdek.beans.ProfileBean;
import de.ingrid.mdek.beans.Rubric;
import de.ingrid.mdek.beans.controls.ExtendedControls;
import de.ingrid.mdek.beans.controls.OptionEntry;
import de.ingrid.mdek.beans.controls.SelectControl;
import de.ingrid.mdek.beans.controls.TextControl;
import de.ingrid.mdek.profile.ProfileMapper;
import de.ingrid.utils.tool.StringUtil;

/**
 * <p>
 * Changes InGrid 3.0:<p>
 * - Flexible data model: store default profile (xml) in IGC and migrate Additional Fields (also remove sys_gui table)
 * - migrate assigned "Kein INSPIRE-Thema" to new checkbox column 'is_inspire_relevant' (class 1 and 3), see https://dev.wemove.com/jira/browse/INGRID23-138
 */
public class IDCStrategy3_0_0 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_0_0.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_0_0;
	
	String profileXml = null;
    ProfileMapper profileMapper;
	ProfileBean profileBean = null;
    Rubric additionalFieldRubric = null;;

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

		System.out.print("  Store default Profile in database...");
		storeDefaultProfile();
		System.out.println("done.");

		System.out.print("  Migrate Additional Fields...");
		migrateAdditionalFields();
		System.out.println("done.");

		System.out.print("  Migrate to new 'is_inspire_relevant' column...");
		migrateIsInspireRelevant();
		System.out.println("done.");

		System.out.print("  Clean up sys_list...");
		cleanUpSysList();
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
			log.info("Manipulate datastructure -> CAUSES COMMIT ! ...");
		}
		
		if (log.isInfoEnabled()) {
			log.info("Modify column 'sys_generic_key.value_string' to \"MEDIUMTEXT\" ...");
		}
		jdbc.getDBLogic().modifyColumn("value_string", ColumnType.MEDIUMTEXT, "sys_generic_key", false, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Create table 'additional_field_data'...");
		}
		jdbc.getDBLogic().createTableAdditionalFieldData(jdbc);

		if (log.isInfoEnabled()) {
			log.info("Add column 'is_inspire_relevant' to table 't011_obj_geo' ...");
		}
		jdbc.getDBLogic().addColumn("is_inspire_relevant", ColumnType.VARCHAR1, "t011_obj_geo", false, "'N'", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Add column 'is_inspire_relevant' to table 't011_obj_serv' ...");
		}
		jdbc.getDBLogic().addColumn("is_inspire_relevant", ColumnType.VARCHAR1, "t011_obj_serv", false, "'N'", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Manipulate datastructure... done");
		}
	}

	private void migrateIsInspireRelevant() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Migrate to new 'is_inspire_relevant' column...");
		}

		// sql for selecting current object INSPIRE themes
		String sql = "select " +
			"obj.id as objId, obj.obj_class, stObj.id as stObjId, stValue.id as stValueId, stValue.entry_id, stValue.term " +
			"from " +
			"searchterm_obj stObj, searchterm_value stValue, t01_object obj " +
			"where " +
			"stObj.searchterm_id = stValue.id " +
			"AND stValue.type = 'I'" + // INSPIRE Theme
			"AND stObj.obj_id = obj.id";

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		int numNotINSPIRE = 0;
		int numWrongClass = 0;
		int numINSPIREClass1 = 0;
		int numINSPIREClass3 = 0;
		while (rs.next()) {
			long objId = rs.getLong("objId");
			int objClass = rs.getInt("obj_class");
			long stObjId = rs.getLong("stObjId");
//			long stValueId = rs.getLong("stValueId");
			int entry_id = rs.getInt("entry_id");
			String term = rs.getString("term");

			boolean deleteObjAssoc = false;
			if (entry_id == 99999 || entry_id == 0) {
				// "Kein INSPIRE-Thema" or null (?)
				if (log.isDebugEnabled()) {
					log.debug("'NO INSPIRE THEME' set in object with id " + objId + " -> we delete theme-object association !");
				}
				deleteObjAssoc = true;
				numNotINSPIRE++;
				
			} else {
				if (objClass == 1) {
					int numUpdated = jdbc.executeUpdate(
						"UPDATE t011_obj_geo SET is_inspire_relevant = 'Y' WHERE obj_id = " + objId);
					numINSPIREClass1++;
					if (log.isDebugEnabled()) {
						log.debug("Set " + numUpdated + " t011_obj_geo.is_inspire_relevant to 'Y', obj_id = " + objId);
					}
					
				} else if (objClass == 3) {
					int numUpdated = jdbc.executeUpdate(
							"UPDATE t011_obj_serv SET is_inspire_relevant = 'Y' WHERE obj_id = " + objId);
					numINSPIREClass3++;
					if (log.isDebugEnabled()) {
						log.debug("Set " + numUpdated + " t011_obj_serv.is_inspire_relevant to 'Y', obj_id = " + objId);
					}

				} else {
					// different object class ? only 1 and 3 has inspire themes, delete the theme !
					if (log.isDebugEnabled()) {
						log.debug("Assigned INSPIRE Theme '" + term + "'(" + entry_id + ") to object of class '" + objClass +
								"', object id = " + objId + ". WRONG CLASS -> we delete theme-object association !");
					}
					deleteObjAssoc = true;
					numWrongClass++;
				}
			}
			
			// delete Theme object association
			if (deleteObjAssoc) {
				int numDeleted = jdbc.executeUpdate("DELETE FROM searchterm_obj where id = " + stObjId);
				if (log.isDebugEnabled()) {
					log.debug("Deleted " + numDeleted + " searchterm_obj record (id=" + stObjId + ") where obj_id = " + objId);
				}
			}
		}


		// finally delete the "Kein INSPIRE-Thema" entry
		int numDeleted = jdbc.executeUpdate("DELETE FROM searchterm_value where type = 'I' AND entry_id = 99999");
		if (log.isDebugEnabled()) {
			log.debug("Deleted " + numDeleted + " searchterm_value record(s) ('Kein INSPIRE-Thema', theme_id=99999).");
		}				

		rs.close();
		st.close();

		if (log.isInfoEnabled()) {
			log.info("Removed " + numNotINSPIRE + " \"Kein INSPIRE-Thema\" associations.");
		}
		if (log.isInfoEnabled()) {
			log.info("Removed " + numWrongClass + " Inspire Theme Associations to objects NOT class 1 or 3.");
		}
		if (log.isInfoEnabled()) {
			log.info("Set " + numINSPIREClass1 + " times t011_obj_geo.is_inspire_relevant to 'Y' (GEO-INFORMATION/KARTE, class 1).");
		}
		if (log.isInfoEnabled()) {
			log.info("Set " + numINSPIREClass3 + " times t011_obj_serv.is_inspire_relevant to 'Y' (GEODATENDIENST, class 3).");
		}

		if (log.isInfoEnabled()) {
			log.info("Migrate to new 'is_inspire_relevant' column...done");
		}
	}

	private void storeDefaultProfile() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Store default Profile in database...");
		}
		
		profileXml = convertStreamToString(getClass().getResourceAsStream("/3_0_0_coreProfile.xml"), null);

		// write Profile !
		setGenericKey(KEY_PROFILE_XML, profileXml);

		if (log.isInfoEnabled()) {
			log.info("Store default Profile in database... done");
		}
	}

	private void migrateAdditionalFields() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Migrate Additional Fields to Profile / additional_field_data...");
		}

        // initialize profile stuff
		if (profileXml == null) {
			throw new Exception("igcProfile not set !");
		}
        profileMapper = new ProfileMapper();
		profileBean = profileMapper.mapStringToBean(profileXml);			


		// sql for field definitions
		String sql = "select " +
			"attrType.id, attrType.name, attrType.type, " + // field
			//"attrType.length, " +
			"attrList.type as listType, attrList.listitem_line, attrList.listitem_value, attrList.lang_code " + // field list
			"from " +
			"t08_attr_type attrType " +
			"left outer join t08_attr_list attrList on (attrType.id = attrList.attr_type_id) " +
			"order by attrType.id, attrList.attr_type_id, attrList.lang_code, attrList.listitem_line";

		AdditionalField currentField = null;
		MigrationStatistics stats = new MigrationStatistics();

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			long nextFieldId = rs.getLong("id");

			// check whether all data of an object is read, then do migration !
			boolean fieldChange = false;
			if (currentField != null && currentField.id != nextFieldId) {
				// field changed, process finished field
				fieldChange = true;
				migrateAdditionalField(currentField, stats);
			}

			if (currentField == null || fieldChange) {
				// set up next field
				currentField = new AdditionalField(nextFieldId, rs.getString("name"),
						rs.getString("type"), rs.getString("listType"));
			}

			// pass new stuff
			if (currentField.listType != null) {
				currentField.addListItem(rs.getString("lang_code"), rs.getString("listitem_value"));
			}
		}
		// also migrate last field ! not done in loop due to end of loop !
		if (currentField != null) {
			migrateAdditionalField(currentField, stats);
		}

		rs.close();
		st.close();

		// write Profile !
        profileXml = profileMapper.mapBeanToXmlString(profileBean);
		setGenericKey(KEY_PROFILE_XML, profileXml);

		if (log.isInfoEnabled()) {
			log.info("Migrated " + stats.numMigrated + " additional fields.");
		}

		if (log.isInfoEnabled()) {
			log.info("Migrate Additional Fields to Profile / additional_field_data...done");
		}
	}

	private void migrateAdditionalField(AdditionalField field, MigrationStatistics stats) throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Migrate additionalField id='" + field.id + "', name='" + field.name + "', type='" + field.type + "'");
		}

		// add to profile
		addToProfile(field);

		// migrate data
		migrateAdditionalFieldData(field);
		
		stats.numMigrated++;
	}

	private void addToProfile(AdditionalField field) throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Add to Profile: additionalField id='" + field.id + "', name='" + field.name + "', type='" + field.type + "'");
		}

        // Add rubric for additional fields to profile
		if (additionalFieldRubric == null) {
			additionalFieldRubric = new Rubric();
			additionalFieldRubric.setId("additionalFields");
			additionalFieldRubric.setIsLegacy(false);
			Map<String, String> label = new HashMap<String, String>();
			label.put("de", "Zusatzfelder");
			label.put("en", "Additional Fields");
			additionalFieldRubric.setLabel(label);
			Map<String, String> helpMessage = new HashMap<String, String>();
			helpMessage.put("de", "Zusatzfelder##Die Zusatzfelder gelten nur katalogweit und werden vom jeweiligen Katalogadministrator zusätzlich zu den Standardfelder des InGridCatalog hinzugefügt.");
			helpMessage.put("en", "Zusatzfelder##Die Zusatzfelder gelten nur katalogweit und werden vom jeweiligen Katalogadministrator zusätzlich zu den Standardfelder des InGridCatalog hinzugefügt.");
			additionalFieldRubric.setHelpMessage(helpMessage);
			profileBean.getRubrics().add(additionalFieldRubric);
		}

		// add control
		ExtendedControls ctrl;
		if (field.listType != null) {
			ctrl = new SelectControl();

			// list for each language !
			Map<String,List<OptionEntry>> options = new HashMap<String, List<OptionEntry>>();
	        for (String lang : field.listsLocalized.keySet()) {
	            List<OptionEntry> list = new ArrayList<OptionEntry>();
	            List<ListItem> items = field.listsLocalized.get(lang);
	            for (ListItem item : items) {
	                list.add(new OptionEntry(item.id, item.value));
	            }
	            options.put(lang, list);
	        }
	        ((SelectControl)ctrl).setOptions(options);
	        ((SelectControl)ctrl).setAllowFreeEntries(true);

		} else {
			ctrl = new TextControl();
			((TextControl)ctrl).setNumLines(1);
		}
		ctrl.setWidth("100");
		ctrl.setId(field.fieldKey);
		ctrl.setIsMandatory(false);
		ctrl.setIsVisible(ProfileMapper.IsVisible.OPTIONAL.getDbValue());
		ctrl.setIsLegacy(false);
		Map<String, String> labelMap = new HashMap<String, String>();
		labelMap.put(getCatalogLanguage(), field.name);
		ctrl.setLabel(labelMap);
		// no Help
		ctrl.setHelpMessage(new HashMap<String, String>());
		ctrl.setScriptedCswMapping("IDF.addAdditionalData(sourceRecord, idfDoc, igcProfileControlNode);");

		additionalFieldRubric.getControls().add(ctrl);
	}

	private void migrateAdditionalFieldData(AdditionalField field) throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Migrate DATA of additionalField id='" + field.id + "', name='" + field.name + "', type='" + field.type + "'");
		}

		String sql = "select * from t08_attr where attr_type_id = " + field.id;

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		int numData = 0;
		while (rs.next()) {
			String data = rs.getString("data");
			if (data == null || data.trim().length() == 0) {
				continue;
			}
			
			// determine list item id if selection list is set
			String listItemId = field.getListItemIdFromValue(data);
			if (listItemId != null) {
				listItemId = "'" + listItemId + "'";
			}
			sql = "INSERT INTO additional_field_data (id, obj_id, field_key, list_item_id, data) "
				+ "VALUES (" + getNextId() + ", " + rs.getLong("obj_id") + ", '" + field.fieldKey + "', " + listItemId + ", '" + data + "')";
			jdbc.executeUpdate(sql);
			numData++;
		}
		rs.close();
		st.close();

		if (log.isDebugEnabled()) {
			log.debug("Migrated " + numData + " data records to table additional_field_data.");
		}
	}

	private void cleanUpSysList() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Clean up sys_list...");
		}
		
		int numDeleted;
		if (log.isInfoEnabled()) {
			log.info("Remove entry 99999 ('No INSPIRE Theme') from syslist 6100 (Inspire Themen)...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = 6100 and entry_id = 99999";
		numDeleted = jdbc.executeUpdate(sqlStr);
		if (log.isDebugEnabled()) {
			log.debug("Removed " + numDeleted +	" entries (all languages).");
		}
		
		if (log.isInfoEnabled()) {
			log.info("Clean up sys_list... done");
		}
	}

	private void cleanUpDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Drop table 't08_attr_list' ...");
		}
		jdbc.getDBLogic().dropTable("t08_attr_list", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Drop table 't08_attr' ...");
		}
		jdbc.getDBLogic().dropTable("t08_attr", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Drop table 't08_attr_type' ...");
		}
		jdbc.getDBLogic().dropTable("t08_attr_type", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure... done");
		}
		if (log.isInfoEnabled()) {
			log.info("Drop table 'sys_gui' ...");
		}
		jdbc.getDBLogic().dropTable("sys_gui", jdbc);

	}

	/** Helper class encapsulating all needed data of a field DEFINITION ! */
	class AdditionalField {
		long id;
		String fieldKey;
		String name;
		String type;
		String listType;
		/** key = language code */
		Map<String, List<ListItem>> listsLocalized ;

		AdditionalField(long id, String name, String type, String listType) {
			this.id = id;
			this.fieldKey = "additionalField" + id;
			// remove new line etc. from label
			this.name = StringUtil.replaceLineFeeds(name, " ");
			this.type = type;
			this.listType = listType;
			this.listsLocalized = new HashMap<String, List<ListItem>>();
		}
		void addListItem(String locale, String itemValue) {
			List<ListItem> list = listsLocalized.get(locale);
			if (list == null) {
				list = new ArrayList<ListItem>();
				listsLocalized.put(locale, list);
			}
			String listItemId = new Integer(list.size()+1).toString();
			list.add(new ListItem(listItemId, itemValue));
		}
		/** Checks all lists beginning with default localized list
		 * returns -1 if lists are present and item not found. Returns null if no list present ! */
		String getListItemIdFromValue(String listItemValue) {
			String itemId = null;
			if (!listsLocalized.isEmpty()) {				
				// first check default locale
				String locale = getCatalogLanguage();
				itemId = getListItemIdFromValue(listsLocalized.get(locale), listItemValue);
				
				if (itemId == null) {
					// check all lists, maybe value in different language
					Iterator<List<ListItem>> itr = listsLocalized.values().iterator();
					while(itr.hasNext()) {
						itemId = getListItemIdFromValue(itr.next(), listItemValue);
						if (itemId != null) {
							break;
						}
					}
				}
				if (itemId == null) {
					// free Entry from ComboBox
					itemId = "-1";
				}
			}
			
			return itemId;
		}
		/** Checks passed list */
		String getListItemIdFromValue(List<ListItem> list, String listItemValue) {
			if (list != null) {
				for (ListItem item : list) {
					if (item.value.equals(listItemValue)) {
						return item.id;
					}
				}
			}
			return null;
		}
	}
	class ListItem {
		String id;
		String value;

		ListItem(String id, String value) {
			this.id = id;
			this.value = value;
		}
	}
	/** Helper class encapsulating statistics */
	class MigrationStatistics {
		int numMigrated = 0;
	}
}
