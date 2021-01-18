/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2021 wemove digital solutions GmbH
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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyHelper;
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
				+ "( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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
		
		String sql = "SELECT adr_uuid FROM t02_address";
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		if (rs.next()) {
			modId = rs.getString("adr_uuid");
			if (modId != null) {
				modTime = currentTime;
			}
		}
		rs.close();
		st.close();
		
		p.setString(cnt++, modId); // mod_uuid,
		p.setString(cnt++, modTime); // mod_time
		p.setString(cnt++, getCatalogLanguageFromDescriptor()); // language_code
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
