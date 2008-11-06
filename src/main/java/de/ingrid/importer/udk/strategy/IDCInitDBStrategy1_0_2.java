/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.util.UuidGenerator;

/**
 * @author Administrator
 * 
 */
public class IDCInitDBStrategy1_0_2 extends IDCStrategyDefault1_0_2 {

	private static Log log = LogFactory.getLog(IDCInitDBStrategy1_0_2.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_102;
	
	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// write IDC structure version !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

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

		String currentTime = IDCStrategyHelper.transDateTime(new Date());

		int cnt = 1;
		dataProvider.setId(dataProvider.getId() + 1);
		p.setLong(cnt++, dataProvider.getId()); // id
		p.setString(cnt++, UuidGenerator.getInstance().generateUuid()); // cat_uuid
		p.setString(cnt++, "default catalog"); // cat_name
		p.setString(cnt++, IDCStrategyHelper.transCountryCode("D")); // country_code
		p.setString(cnt++, "N"); // workflow_control
		p.setNull(cnt++, Types.INTEGER); // expiry_duration
		p.setString(cnt++, currentTime); // create_time
		
		String modId = null;
		String modTime = null;
		
		String sql = "SELECT adr_uuid FROM t02_address;";
		ResultSet rs = jdbc.executeQuery(sql);
		if (rs.next()) {
			modId = rs.getString("adr_uuid");
			if (modId != null) {
				modTime = currentTime;
			}
		}
		rs.close();
		
		p.setString(cnt++, modId); // mod_uuid,
		p.setString(cnt++, modTime); // mod_time
		p.setString(cnt++, getCatalogLanguage()); // language_code
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
