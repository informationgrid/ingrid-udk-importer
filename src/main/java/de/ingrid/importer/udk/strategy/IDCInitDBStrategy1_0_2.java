/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.util.UuidGenerator;

/**
 * @author Administrator
 * 
 */
public class IDCInitDBStrategy1_0_2 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCInitDBStrategy1_0_2.class);

	private String IDC_VERSION = "1.0.2_dev";

	public void execute() {

		try {

			jdbc.setAutoCommit(false);

			// write version !
			setGenericKey(KEY_IDC_VERSION, IDC_VERSION);

			System.out.print("  Importing sys_list...");
			// must be processed first because other methods depend on that data
			processSysList();
			System.out.println("done.");
			
			System.out.print("  Importing default address/permission for admin...");
			sqlStr = "DELETE FROM t02_address";
			jdbc.executeUpdate(sqlStr);
			sqlStr = "DELETE FROM address_node";
			jdbc.executeUpdate(sqlStr);
			importDefaultUserdata();
			System.out.println("done.");
			System.out.print("  Creating default catalog...");
			importDefaultCatalogData();
			System.out.println("done.");
			jdbc.commit();
			
			jdbc.setAutoCommit(false);
			System.out.print("  Post processing...");
			postProcess_generic();
			// no post processing of spatial ref in catalogue !
			System.out.println("done.");
			System.out.print("  Set HI/LO table...");
			setHiLoGenerator();
			System.out.println("done.");
			jdbc.commit();
			System.out.println("Import finished successfully.");

		} catch (Exception e) {
			System.out.println("Error executing sql! See log file for further information.");
			log.error("Error executing SQL!", e);
			if (jdbc != null) {
				try {
					jdbc.rollback();
				} catch (SQLException e1) {
					log.error("Error rolling back transaction!", e);
				}
			}
		} finally {
			if (jdbc != null) {
				try {
					jdbc.close();
				} catch (SQLException e) {
					log.error("Error closing DB connection!", e);
				}
			}
		}
	}
	
	protected void importDefaultCatalogData() throws Exception {

		if (log.isInfoEnabled()) {
			log.info("Creating default catalog...");
		}

		pSqlStr = "INSERT INTO t03_catalogue (id, cat_uuid, cat_name, country_code,"
				+ "workflow_control, expiry_duration, create_time, mod_uuid, mod_time, language_code) VALUES "
				+ "( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t03_catalogue";
		jdbc.executeUpdate(sqlStr);

		int cnt = 1;
		dataProvider.setId(dataProvider.getId() + 1);
		p.setLong(cnt++, dataProvider.getId()); // id
		p.setString(cnt++, UuidGenerator.getInstance().generateUuid()); // cat_uuid
		p.setString(cnt++, "default catalog"); // cat_name
		p.setString(cnt++, IDCStrategyHelper.transCountryCode("D")); // country_code
		p.setString(cnt++, "N"); // workflow_control
		p.setNull(cnt++, Types.INTEGER); // expiry_duration
		p.setString(cnt++, IDCStrategyHelper.transDateTime("01.05.2008")); // create_time
		
		String modId = null;
		
		String sql = "SELECT adr_uuid FROM t02_address;";
		ResultSet rs = jdbc.executeQuery(sql);
		if (rs.next()) {
			modId = rs.getString("adr_uuid");
		}
		rs.close();
		
		if (modId == null) {
			modId = "";
		}
		p.setString(cnt++, modId); // mod_uuid,
		p.setString(cnt++, IDCStrategyHelper.transDateTime("01.05.2008")); // mod_time
		p.setString(cnt++, "de"); // language_code
		try {
			p.executeUpdate();
		} catch (Exception e) {
			log.error("Error executing SQL: " + p.toString(), e);
			throw e;
		}

		if (log.isInfoEnabled()) {
			log.info("Creating default catalog... done.");
		}
	}	

}
