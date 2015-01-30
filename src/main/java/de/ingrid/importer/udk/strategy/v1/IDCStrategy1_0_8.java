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
package de.ingrid.importer.udk.strategy.v1;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * InGrid 2.3, statische Änderungen:
 * - Trennung der Felder Zugangsbeschränkung und Nutzungsbedingung (new table object_use)
 */
public class IDCStrategy1_0_8 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy1_0_8.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_108;

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// then write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		// THEN EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit (e.g. on MySQL)
		// ---------------------------------
		System.out.print("  Extend datastructure...");
		extendDataStructure();
		System.out.println("done.");

		// THEN PERFORM DATA MANIPULATIONS !
		// ---------------------------------

		System.out.print("  Updating object_use...");
		updateObjectUse();
		System.out.println("done.");

/*
// NOT needed, only optional fields have to be added to sys_gui, we have only new MANDATORY field !
		System.out.print("  Updating sys_gui...");
		updateSysGui();
		System.out.println("done.");
*/
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
			log.info("Create table 'object_use'...");
		}
		jdbc.getDBLogic().createTableObjectUse(jdbc);

		if (log.isInfoEnabled()) {
			log.info("Manipulate datastructure... done");
		}
	}

	protected void updateObjectUse() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating object_use...");
		}

		if (log.isInfoEnabled()) {
			log.info("Migrate object_access.terms_of_use to table object_use...");
		}

		String sql = "select object_access.obj_id, object_access.line, object_access.terms_of_use " +
			"from object_access";

		// Node may contain multiple equal entries for same object ! 
		// we track written data in hash maps to avoid multiple writing for same object
		HashMap<Long, List<String>> processedObjIds = new HashMap<Long, List<String>>();

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		int numMigrated = 0;
		while (rs.next()) {
			long objId = rs.getLong("obj_id");
			int line = rs.getInt("line");
			String termsOfUse = rs.getString("terms_of_use");
			termsOfUse = (termsOfUse == null) ? "" : termsOfUse.trim();

			// check whether value already written
			boolean writeNewValue = false;
			if (termsOfUse.length() > 0) {
				List<String> valueList = processedObjIds.get(objId);
				if (valueList == null) {
					valueList = new ArrayList<String>();
					processedObjIds.put(objId, valueList);
				}
				if (!valueList.contains(termsOfUse)) {
					writeNewValue = true;
				}
			}

			// write value if not written yet !
			if (writeNewValue) {
				jdbc.executeUpdate("INSERT INTO object_use (id, obj_id, line, terms_of_use) "
					+ "VALUES (" + getNextId() + ", " + objId + ", " + line + ", '" + termsOfUse + "')");

				if (log.isDebugEnabled()) {
					log.debug("object_use: migrated (obj_id, line, terms_of_use) (" + objId + ", " + line + ", '" + termsOfUse + "')");
				}

				List<String> valueList = processedObjIds.get(objId);
				valueList.add(termsOfUse);
				numMigrated++;

				// extend object index (index contains only data of working versions !)
				// Not necessary, contents are the same ! just restructured !
			}
		}
		rs.close();
		st.close();

		if (log.isInfoEnabled()) {
			log.info("Migrated " + numMigrated + " terms_of_use from table object_access to new table object_use");
		}

		if (log.isInfoEnabled()) {
			log.info("Updating object_use... done");
		}
	}

	protected void cleanUpDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Drop 'object_access.terms_of_use' ...");
		}
		jdbc.getDBLogic().dropColumn("terms_of_use", "object_access", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure... done");
		}
	}
}
