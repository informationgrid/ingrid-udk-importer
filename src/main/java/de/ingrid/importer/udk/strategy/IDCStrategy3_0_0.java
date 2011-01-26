/**
 * 
 */
package de.ingrid.importer.udk.strategy;

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
			log.info("Migrate Additional Fields to Profile...");
		}
/*
		String sql = "select distinct id, idc_group_id " +
			"from idc_user";

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		int numMigrated = 0;
		while (rs.next()) {
			long userId = rs.getLong("id");
			long groupId = rs.getLong("idc_group_id");
			sql = "INSERT INTO idc_user_group (id, idc_user_id, idc_group_id) "
				+ "VALUES (" + getNextId() + ", " + userId + ", " + groupId + ")";
			jdbc.executeUpdate(sql);
			numMigrated++;

			if (log.isDebugEnabled()) {
				log.debug("Migrated user (id:" + userId + ") with group (id:" + groupId + ").");
			}
		}
		rs.close();
		st.close();

		if (log.isDebugEnabled()) {
			log.debug("Migrated " + numMigrated + " user group associations.");
		}
*/
		if (log.isInfoEnabled()) {
			log.info("Migrate Additional Fields to Profile... done");
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
}
