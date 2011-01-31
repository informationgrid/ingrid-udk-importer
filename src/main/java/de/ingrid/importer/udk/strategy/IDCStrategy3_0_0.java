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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;

/**
 * <p>
 * Changes InGrid 3.0:<p>
 * - Flexible data model: store default profile (xml) in IGC and migrate Additional Fields
 */
public class IDCStrategy3_0_0 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_0_0.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_0_0;

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
			log.info("Manipulate datastructure... done");
		}
	}

	private void storeDefaultProfile() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Store default Profile in database...");
		}

		if (log.isInfoEnabled()) {
			log.info("Store default Profile in database... done");
		}
	}

	private void migrateAdditionalFields() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Migrate Additional Fields to Profile / additional_field_data...");
		}

		// first migrate field definitions
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

		if (log.isInfoEnabled()) {
			log.info("Migrated " + stats.numMigrated + " additional fields.");
		}

		if (log.isInfoEnabled()) {
			log.info("Migrate Additional Fields to Profile / additional_field_data...done");
		}
	}

	protected void migrateAdditionalField(AdditionalField field, MigrationStatistics stats) throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Migrate additionalField id='" + field.id + "', name='" + field.name + "', type='" + field.type + "'");
		}

		// add to profile
		
		// TODO

		// migrate data
		migrateAdditionalFieldData(field);
		
		stats.numMigrated++;
	}

	protected void migrateAdditionalFieldData(AdditionalField field) throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Migrate DATA of additionalField id='" + field.id + "', name='" + field.name + "', type='" + field.type + "'");
		}

		String sql = "select * from t08_attr where attr_type_id = " + field.id;

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		int numData = 0;
		while (rs.next()) {
			String fieldKey = "additionalField" + field.id;
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
				+ "VALUES (" + getNextId() + ", " + rs.getLong("obj_id") + ", '" + fieldKey + "', " + listItemId + ", '" + data + "')";
			jdbc.executeUpdate(sql);
			numData++;
		}
		rs.close();
		st.close();

		if (log.isDebugEnabled()) {
			log.debug("Migrated " + numData + " data records to table additional_field_data.");
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
	}

	/** Helper class encapsulating all needed data of a field DEFINITION ! */
	class AdditionalField {
		long id;
		String name;
		String type;
		String listType;
		/** key = language code */
		HashMap<String, List<ListItem>> listsLocalized ;

		AdditionalField(long id, String name, String type, String listType) {
			this.id = id;
			this.name = name;
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
