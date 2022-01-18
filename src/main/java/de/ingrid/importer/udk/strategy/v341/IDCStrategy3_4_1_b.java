/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2022 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v341;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 3.4.1 (or 3.5, if no branch release)<p>
 * <ul>
 *   <li>Update and insert entries (iso) in codelists accessConstraints (List-ID 6010) + useConstraints (List-ID 6020), see https://redmine.wemove.com/issues/557
 * </ul>
 */
public class IDCStrategy3_4_1_b extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_4_1_b.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_4_1_b;

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		// THEN EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit (e.g. on MySQL)
		// ---------------------------------

		// THEN PERFORM DATA MANIPULATIONS !
		// ---------------------------------
		System.out.print("  Updating sys_list ...");
		updateSysList();
		System.out.println("done.");

		// FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause commit (e.g. on MySQL)
		// ---------------------------------

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	protected void updateSysList() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("\nUpdating sys_list...");
		}

// ---------------------------
		if (log.isInfoEnabled()) {
			log.info("Updating syslist 6010 (ISO: accessConstraints)...");
		}
 
		int numUpdated = jdbc.executeUpdate("UPDATE sys_list SET name = 'Es gelten keine Bedingungen' WHERE lst_id = 6010 and (name = 'keine' OR name = 'Keine')");
		numUpdated += jdbc.executeUpdate("UPDATE sys_list SET name = 'Bedingungen unbekannt' WHERE lst_id = 6010 and name = 'unbekannt'");
		log.debug("Updated " + numUpdated +	" entry(ies) ...");

		numUpdated = jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", 6010, 1, 'iso', 'Es gelten keine Bedingungen', 1, 'Y')");
		numUpdated += jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", 6010, 10, 'iso', 'Bedingungen unbekannt', 1, 'N')");

		if (log.isInfoEnabled()) {
			log.info("Inserted " + numUpdated + " new iso entry(ies) ...");
		}
// ---------------------------
		if (log.isInfoEnabled()) {
			log.info("Updating syslist 6020 (ISO: useConstraints)...");
		}

		numUpdated = jdbc.executeUpdate("UPDATE sys_list SET name = 'Es gelten keine Bedingungen' WHERE lst_id = 6020 and (name = 'keine' OR name = 'Keine')");
		log.debug("Updated " + numUpdated +	" entry(ies) ...");

		numUpdated = jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", 6020, 1, 'iso', 'Es gelten keine Bedingungen', 1, 'N')");

		if (log.isInfoEnabled()) {
			log.info("Inserted " + numUpdated + " new iso entry(ies) ...");
		}
// ---------------------------

		if (log.isInfoEnabled()) {
			log.info("Updating sys_list... done\n");
		}
	}
}
