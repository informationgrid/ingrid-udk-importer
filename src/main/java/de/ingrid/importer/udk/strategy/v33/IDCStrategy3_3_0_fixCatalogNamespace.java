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
package de.ingrid.importer.udk.strategy.v33;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 3.3<p>
 * <ul>
 *   <li>Katalog-Namespace set default value if empty, see INGRID-2221
 * </ul>
 */
public class IDCStrategy3_3_0_fixCatalogNamespace extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_3_0_fixCatalogNamespace.class);

    /**
     * Deliver NO Version, this strategy should NOT trigger a strategy workflow (of missing former
     * versions) and can be executed on its own !
     * NOTICE: BUT may be executed in workflow (part of workflow array) !
     * @see de.ingrid.importer.udk.strategy.IDCStrategy#getIDCVersion()
     */
	public String getIDCVersion() {
		return null;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// PERFORM DATA MANIPULATIONS !
		// ----------------------------

		System.out.print("  Update t03_catalogue.cat_namespace...");
		updateT03CatalogueNamespace();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void updateT03CatalogueNamespace() throws Exception {
		log.info("\nSet 't03_catalogue.cat_namespace' to default if empty...");

		// select current namespace
		String sqlSelectOldData = "SELECT cat_namespace " +
			"FROM t03_catalogue";
		
		// update
		PreparedStatement psUpdate = jdbc.prepareStatement(
			"UPDATE t03_catalogue SET cat_namespace = ?");

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sqlSelectOldData, st);
		while (rs.next()) {

			String catNamespace = rs.getString("cat_namespace");
			if (catNamespace == null || catNamespace.trim().isEmpty()) {
				// get name of catalog in database !
				String catName = jdbc.getCatalog();
				if (catName == null) {
					String errMsg = "Problems fetching name of catalog from JDBC connection !!!";
					log.error(errMsg);
					throw new Exception(errMsg);
				}

				String myNamespace = "http://portalu.de/" + catName;
				psUpdate.setString(1, myNamespace);				
				int numProcessed = psUpdate.executeUpdate();

				log.debug("Upated " + numProcessed + " 't03_catalogue.cat_namespace' to '" +
					myNamespace + "'");
			}
		}
		rs.close();
		st.close();
		psUpdate.close();

		log.info("\nSet 't03_catalogue.cat_namespace' to default if empty... done\n");
	}
}

