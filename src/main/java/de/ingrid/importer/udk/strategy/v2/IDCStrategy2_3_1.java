/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2017 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v2;

import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 2.3 for NI:<p>
 * - Connect user with multiple groups
 */
public class IDCStrategy2_3_1 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy2_3_1.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_2_3_1;

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

		System.out.println("  Migrate User Group Association...");
		migrateUserGroupAssociations();
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
			log.info("Create table 'idc_user_group'...");
		}
		jdbc.getDBLogic().createTableIdcUserGroup(jdbc);

		if (log.isInfoEnabled()) {
			log.info("Manipulate datastructure... done");
		}
	}

	protected void migrateUserGroupAssociations() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Migrate User Group Association...");
		}

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

		if (log.isInfoEnabled()) {
			log.info("Migrate User Group Association... done");
		}
	}

	private void cleanUpDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Drop column idc_group_id from table 'idc_user' ...");
		}
		jdbc.getDBLogic().dropColumn("idc_group_id", "idc_user", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure... done");
		}
	}
}
