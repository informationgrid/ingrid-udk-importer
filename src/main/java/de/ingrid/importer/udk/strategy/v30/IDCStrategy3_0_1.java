/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2015 wemove digital solutions GmbH
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * Changes InGrid 3.0.1:<p>
 * - change type of t02_address.institution to TEXT_NO_CLOB
 * - change type of t011_obj_project.leader and .member to TEXT_NO_CLOB (free text entry in IGE !)
 * - migrate referencesystem_key, referencesystem_value from t011_obj_geo to new table spatial_system, see https://dev.wemove.com/jira/browse/INGRID23-60
 */
public class IDCStrategy3_0_1 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_0_1.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_0_1;
	
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

		System.out.print("  Migrate Spatial System to new table (now n:1)...");
		migrateSpatialSystem();
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
			log.info("Modify column 't02_address.institution' to \"TEXT_NO_CLOB\" ...");
		}
		jdbc.getDBLogic().modifyColumn("institution", ColumnType.TEXT_NO_CLOB, "t02_address", false, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Modify column 't011_obj_project.leader' and 't011_obj_project.member' to \"TEXT_NO_CLOB\" ...");
		}
		jdbc.getDBLogic().modifyColumn("leader", ColumnType.TEXT_NO_CLOB, "t011_obj_project", false, jdbc);
		jdbc.getDBLogic().modifyColumn("member", ColumnType.TEXT_NO_CLOB, "t011_obj_project", false, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Create table 'spatial_system'...");
		}
		jdbc.getDBLogic().createTableSpatialSystem(jdbc);

		if (log.isInfoEnabled()) {
			log.info("Manipulate datastructure... done");
		}
	}

	protected void migrateSpatialSystem() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Migrate Spatial System to new table (now n:1)...");
		}

		// use PreparedStatement to avoid problems when value String contains "'" !!!
		String psSql = "INSERT INTO spatial_system (id, obj_id, line, referencesystem_key, referencesystem_value) " +
			"VALUES (?, ?, 1, ?, ?)";
		
		PreparedStatement ps = jdbc.prepareStatement(psSql);

		String sql = "select distinct id, obj_id, referencesystem_key, referencesystem_value " +
			"from t011_obj_geo";

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		int numMigrated = 0;
		while (rs.next()) {
			long objId = rs.getLong("obj_id");
			int referencesystemKey = rs.getInt("referencesystem_key");
			String referencesystemValue = rs.getString("referencesystem_value");
			
			if (referencesystemKey == 0) {
				referencesystemKey = -1;
			}
			if (referencesystemKey > 0 || referencesystemValue != null) {
				ps.setLong(1, getNextId());
				ps.setLong(2, objId);
				ps.setInt(3, referencesystemKey);
				ps.setString(4, referencesystemValue);
				ps.executeUpdate();

				numMigrated++;

				if (log.isDebugEnabled()) {
					log.debug("Migrated spatial system (key:" + referencesystemKey + ", value:'" + referencesystemValue +
						"') of object with id:" + objId + ").");
				}
			}
		}
		rs.close();
		st.close();
		ps.close();

		if (log.isDebugEnabled()) {
			log.debug("Migrated " + numMigrated + " spatial systems.");
		}

		if (log.isInfoEnabled()) {
			log.info("Migrate Spatial System to new table (now n:1)... done");
		}
	}

	private void cleanUpDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Drop columns 'referencesystem_value', 'referencesystem_key' from table 't011_obj_geo' ...");
		}
		jdbc.getDBLogic().dropColumn("referencesystem_value", "t011_obj_geo", jdbc);
		jdbc.getDBLogic().dropColumn("referencesystem_key", "t011_obj_geo", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure... done");
		}
	}
}
