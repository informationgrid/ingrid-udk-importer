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
package de.ingrid.importer.udk.strategy.v33;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 3.3<p>
 * <ul>
 *   <li>Add sys_list.data column, see INGRID33-5
 *   <li>Drop t02_address.descr column, migrate to address_comment, see INGRID33-10
 *   <li>Add t02_address.publish_id, initialize to 1 (Internet), see INGRID33-12
 * </ul>
 */
public class IDCStrategy3_3_0_a extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_3_0_a.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_3_0_a;

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

		System.out.print("  Migrating t02_address.descr...");
		migrateT02AddressDescr();
		System.out.println("done.");

		System.out.print("  Initialize t02_address.publish_id...");
		initializeT02AddressPublishId();
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
		log.info("\nExtending datastructure -> CAUSES COMMIT ! ...");

		log.info("Add column 'data' to table 'sys_list' ...");
		jdbc.getDBLogic().addColumn("data", ColumnType.TEXT_NO_CLOB, "sys_list", false, null, jdbc);

		log.info("Add column 'publish_id' to table 't02_address' ...");
		jdbc.getDBLogic().addColumn("publish_id", ColumnType.INTEGER, "t02_address", false, null, jdbc);

		log.info("Extending datastructure... done\n");
	}

	private void migrateT02AddressDescr() throws Exception {
		log.info("\nMigrate data from 't02_address.descr' to 'address_comment'...");

		// NOTICE: We do NOT update search index due to same values.

		// select all data from old tables
		String sqlSelectOldData = "SELECT id, descr " +
			"FROM t02_address " +
			"WHERE descr IS NOT NULL " +
			"ORDER BY id";
		
		// read current max line column of address
		PreparedStatement psSelectCommentLine = jdbc.prepareStatement(
				"SELECT line " +
				"FROM address_comment " +
				"WHERE addr_id = ? " +
				"ORDER BY line DESC");

		// insert into comment table
		PreparedStatement psInsert = jdbc.prepareStatement(
				"INSERT INTO address_comment " +
				"(id, addr_id, comment_, create_uuid, create_time , line) " +
				"VALUES (?,?,?,?,?,?)");

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sqlSelectOldData, st);
		int numProcessed = 0;
		String prefix = "[Dieser Eintrag wurde automatisch aus dem Feld Notiz überführt.]\n";
		String catAdminUuid = getCatalogAdminUuid();
		while (rs.next()) {

			long addrId = rs.getLong("id");
			String addrNote = rs.getString("descr");
			
			if (addrNote != null && addrNote.trim().length() > 0) {
				// read current max line column of address comments
				int lineValue = 1;
				psSelectCommentLine.setLong(1, addrId);
    			ResultSet rsLine = psSelectCommentLine.executeQuery();
    			if (rsLine.next()) {
    				lineValue = rsLine.getInt("line") + 1;
    			}
    			rsLine.close();


				psInsert.setLong(1, getNextId());
				psInsert.setLong(2, addrId);
				psInsert.setString(3, prefix + addrNote);
				psInsert.setString(4, catAdminUuid);
				String now = dateToTimestamp(new Date());
				psInsert.setString(5, now);
				psInsert.setInt(6, lineValue);
				psInsert.executeUpdate();

				numProcessed++;
				log.debug("Transferred entry from 't02_address.descr' to 'address_comment': " +
					"addrId=" + addrId + " -> " + addrNote + "/" + catAdminUuid + "/" + now + "/" + lineValue);
			}
		}
		rs.close();
		st.close();
		psSelectCommentLine.close();
		psInsert.close();

		log.info("Transferred " + numProcessed + " entries.");
		log.info("Migrate data from 't02_address.descr' to 'address_comment'... done\n");
	}

	private void initializeT02AddressPublishId() throws Exception {
		log.info("\nInitialize 't02_address.publish_id' to 1 (Internet)...");

		int numUpdated = jdbc.executeUpdate("UPDATE t02_address SET publish_id = 1");

		log.info("Initialized " + numUpdated + " addresses to publish_id=1 (Internet)");
		log.info("Initialize 't02_address.publish_id' to 1 (Internet)... done\n");
	}

	private void cleanUpDataStructure() throws Exception {
		log.info("\nCleaning up datastructure -> CAUSES COMMIT ! ...");

		log.info("Drop column 'descr' from table 't02_address' ...");
		jdbc.getDBLogic().dropColumn("descr", "t02_address", jdbc);

		log.info("Cleaning up datastructure... done\n");
	}
}
