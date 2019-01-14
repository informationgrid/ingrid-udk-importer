/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2019 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v331;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 3.3.1<p>
 * <ul>
 *   <li>Add columns t017_url_ref.datatype_key / datatype_value, see REDMINE-118
 * </ul>
 * Writes NEW Catalog Schema Version to catalog !
 */
public class IDCStrategy3_3_1_b extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_3_1_b.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_3_1_b;

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

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void extendDataStructure() throws Exception {
		log.info("\nExtending datastructure -> CAUSES COMMIT ! ...");

		log.info("Add column 'datatype_key' to table 't017_url_ref' ...");
		jdbc.getDBLogic().addColumn("datatype_key", ColumnType.INTEGER, "t017_url_ref", false, null, jdbc);
		log.info("Add column 'datatype_value' to table 't017_url_ref' ...");
		jdbc.getDBLogic().addColumn("datatype_value", ColumnType.VARCHAR255, "t017_url_ref", false, null, jdbc);


		log.info("Extending datastructure... done\n");
	}
}
