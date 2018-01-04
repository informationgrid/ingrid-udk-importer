/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2018 wemove digital solutions GmbH
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

import de.ingrid.importer.udk.ImportDescriptorHelper;
import de.ingrid.importer.udk.strategy.IDCStrategyHelper;
import de.ingrid.importer.udk.util.UuidGenerator;

/**
 * Strategy 1.0.2_clean for generating initial empty catalog without passing UDK data.
 * NOTICE: Operates on catalog already including all tables, execute ingrid-igc-schema_102*.sql before !
 */
public class IDCStrategy1_0_2_clean extends IDCStrategyDefault1_0_2 {

	private static Log log = LogFactory.getLog(IDCStrategy1_0_2_clean.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_102;
	
	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// write IDC structure version !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		System.out.print("  Importing sys_list...");
		// no files passed via command line, we set the data files to import
		ImportDescriptorHelper.addDataFile("/1_0_2_clean_data.zip", getImportDescriptor());
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
		postProcess_specific();
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

		pSqlStr = "INSERT INTO t03_catalogue (id, cat_uuid, cat_name, partner_name , provider_name, country_code,"
				+ "workflow_control, expiry_duration, create_time, mod_uuid, mod_time, language_code) VALUES "
				+ "( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t03_catalogue";
		jdbc.executeUpdate(sqlStr);

		String currentTime = IDCStrategyHelper.transDateTime(new Date());

		int cnt = 1;
		dataProvider.setId(dataProvider.getId() + 1);
		p.setLong(cnt++, dataProvider.getId()); // id
		p.setString(cnt++, UuidGenerator.getInstance().generateUuid()); // cat_uuid
		p.setString(cnt++, getImportDescriptor().getIdcCatalogueName()); // cat_name
		p.setString(cnt++, getImportDescriptor().getIdcPartnerName()); // partner_name
		p.setString(cnt++, getImportDescriptor().getIdcProviderName()); // provider_name
		p.setString(cnt++, getImportDescriptor().getIdcCatalogueCountry()); // country_code
		p.setString(cnt++, "N"); // workflow_control
		p.setNull(cnt++, Types.INTEGER); // expiry_duration
		p.setString(cnt++, currentTime); // create_time
		
		String modUuid = null;
		String modTime = null;
		
		String sql = "SELECT adr_uuid FROM t02_address";
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		if (rs.next()) {
			modUuid = rs.getString("adr_uuid");
			if (modUuid != null) {
				modTime = currentTime;
			}
		}
		rs.close();
		st.close();
		
		p.setString(cnt++, modUuid); // mod_uuid,
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

	protected void postProcess_specific() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Post processing specific stuff ...");
		}

		// set default entries in sys_lists
		// --------------------------------
		if (log.isInfoEnabled()) {
			log.info("set default language in syslist 99999999 from descriptor ...");
		}

		// set default language of metadata entities (=default entry in sys_list 99999999)

		// was read from descriptor
		String catLang = getCatalogLanguageFromDescriptor();

		if ("de".equals(catLang)) {
			jdbc.executeUpdate("UPDATE sys_list SET is_default = 'N' WHERE lst_id=99999999");
			// default is german (=121)
			jdbc.executeUpdate("UPDATE sys_list SET is_default = 'Y' WHERE lst_id=99999999 AND entry_id=121");
			log.info("default language set to GERMAN");
			
		} else if ("en".equals(catLang)) {
			jdbc.executeUpdate("UPDATE sys_list SET is_default = 'N' WHERE lst_id=99999999");
			// default is english (=94)
			jdbc.executeUpdate("UPDATE sys_list SET is_default = 'Y' WHERE lst_id=99999999 AND entry_id=94");
			log.info("default language set to ENGLISH");
		}

		if (log.isInfoEnabled()) {
			log.info("Post processing specific stuff ... done.");
		}
	}
}
