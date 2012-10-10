/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;

/**
 * <p>
 * Changes InGrid 3.3<p>
 * <ul>
 *   <li>Add sys_list.data column, see INGRID33-5
 *   <li>Drop t02_address.descr column, migrate to address_comment, see INGRID33-10
 * </ul>
 */
public class IDCStrategy3_3_0 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_3_0.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_3_0;

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
		String prefix = "Dieser Eintrag wurde automatisch aus dem Feld Notiz überführt.\n";
		String catAdminUuid = getCatAdminUuid();
		while (rs.next()) {

			long addrId = rs.getLong("id");
			String addrNote = rs.getString("descr");
			
			if (addrNote != null && addrNote.trim().length() > 0) {
				// read current max line column of address comments
				int lineValue = 1;
				psSelectCommentLine.setLong(1, addrId);
    			ResultSet rsLine = psSelectCommentLine.executeQuery();
    			if (rsLine.next()) {
    				lineValue = rs.getInt("line") + 1;
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

		log.info("Transferred " + numProcessed + " entries... done");
		log.info("Migrate data from 't02_address.descr' to 'address_comment'... done\n");
	}

	private void cleanUpDataStructure() throws Exception {
		log.info("\nCleaning up datastructure -> CAUSES COMMIT ! ...");

		log.info("Drop column 'descr' from table 't02_address' ...");
		jdbc.getDBLogic().dropColumn("descr", "t02_address", jdbc);

		log.info("Cleaning up datastructure... done\n");
	}

	/** Format date to database timestamp. */
	public String dateToTimestamp(Date date) {
		try {
			String out = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(date);
			return out;
		} catch (Exception ex){
			log.warn("Problems formating date to timestamp: " + date, ex);
			return "";
		}
	}

	/** Return UUID of CatAdmin of catalog. */
	public String getCatAdminUuid() throws Exception {
		String catAdminUuid = null;

		Statement stUser = jdbc.createStatement();
		ResultSet rsUser = jdbc.executeQuery("SELECT addr_uuid FROM idc_user WHERE idc_role = 1", stUser);
		if (rsUser.next()) {
			catAdminUuid = rsUser.getString("addr_uuid");			
		}
		rsUser.close();
		stUser.close();
		
		return catAdminUuid;
	}

}
