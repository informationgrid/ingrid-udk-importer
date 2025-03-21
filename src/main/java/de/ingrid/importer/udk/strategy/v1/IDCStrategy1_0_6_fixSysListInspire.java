/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2025 wemove digital solutions GmbH
 * ==================================================
 * Licensed under the EUPL, Version 1.2 or – as soon they will be
 * approved by the European Commission - subsequent versions of the
 * EUPL (the "Licence");
 * 
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 * 
 * https://joinup.ec.europa.eu/software/page/eupl
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.utils.udk.UtilsLanguageCodelist;

/**
 * Independent Strategy for fixing wrong syslist VALUES (INSPIRE syslists etc.)
 * NOTICE: Also updates according values in object data if present !
 * see http://dev.wemove.com/jira/browse/INGRID-1860
 */
public class IDCStrategy1_0_6_fixSysListInspire extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy1_0_6_fixSysListInspire.class);

	/**
	 * Deliver NO Version, this strategy should NOT trigger a strategy workflow and
	 * can be executed on its own ! NOTICE: BUT may be executed in workflow (part of workflow array) !
	 * @see de.ingrid.importer.udk.strategy.IDCStrategy#getIDCVersion()
	 */
	public String getIDCVersion() {
		return null;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		System.out.print("  Fixing syslist 5100 values ...");
		fixSysList5100();
		System.out.println("done.");
		System.out.print("  Fixing syslist 5200 values ...");
		fixSysList5200();
		System.out.println("done.");
		System.out.print("  Fixing syslist 528 values ...");
		fixSysList528();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	protected void fixSysList5100() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Fixing syslist 5100 values in sys_list and t011_obj_serv ...");
		}

		// new syslist 5100 values
		HashMap<Integer, String> mapKeyToNewValueList = new HashMap<Integer, String>();
		mapKeyToNewValueList.put(1, "Suchdienste");
		mapKeyToNewValueList.put(2, "Darstellungsdienste");
		mapKeyToNewValueList.put(3, "Download-Dienste");
		mapKeyToNewValueList.put(4, "Transformationsdienste");
		mapKeyToNewValueList.put(5, "Dienste zum Abrufen von Geodatendiensten");
		mapKeyToNewValueList.put(6, "Sonstige Dienste");

		Iterator<Entry<Integer,String>> entryIt = mapKeyToNewValueList.entrySet().iterator();
		while (entryIt.hasNext()) {
			Entry<Integer,String> entry = entryIt.next();

			// fix sys_list
			int numUpdated = jdbc.executeUpdate(
				"UPDATE sys_list SET " +
					"name = '" + entry.getValue() + "' " +
				"where " +
					"lst_id = 5100" +
					" and lang_id = 'de'" +
					" and entry_id = " + entry.getKey());
			if (log.isDebugEnabled()) {
				log.debug("sys_list 5100: updated " + numUpdated + " rows -> entry_id(" + entry.getKey() + "), " +
					"new name(" +	entry.getValue() + ")");
			}

			// fix data: existing keys with wrong value ! ONLY IF GERMAN !
			if ("de".equals(UtilsLanguageCodelist.getShortcutFromCode(readCatalogLanguageKey()))) {
				numUpdated = jdbc.executeUpdate(
						"UPDATE t011_obj_serv SET " +
							"type_value = '" + entry.getValue() + "' " +
						"where " +
							"type_key = " + entry.getKey());
				if (log.isDebugEnabled()) {
					log.debug("t011_obj_serv: updated " + numUpdated + " rows -> existing type_key(" + entry.getKey() + "), " +
						"new type_value(" +	entry.getValue() + ")");
				}				
			}
		}
	}

	protected void fixSysList5200() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Fixing syslist 5200 values in sys_list and t011_obj_serv_type ...");
		}

		// new syslist 5200 values
		HashMap<Integer, String> mapKeyToNewValueListDE = new HashMap<Integer, String>();
		mapKeyToNewValueListDE.put(101, "Katalogdienst (Viewer)");
		mapKeyToNewValueListDE.put(207, "Katalogdienst (Service)");
		
		HashMap<Integer, String> mapKeyToNewValueListEN = new HashMap<Integer, String>();
		mapKeyToNewValueListEN.put(415, "Feature generalisation service (spatial)");
		mapKeyToNewValueListEN.put(513, "Multiband image manipulation");

		String[] langToProcess = new String[] { "de", "en" };
		for (String lang : langToProcess) {
			Iterator<Entry<Integer,String>> entryIt = null;
			if (lang.equals("de")) {
				entryIt = mapKeyToNewValueListDE.entrySet().iterator();
			} else if (lang.equals("en")) {
				entryIt = mapKeyToNewValueListEN.entrySet().iterator();
			}
			String catLang = UtilsLanguageCodelist.getShortcutFromCode(readCatalogLanguageKey());

			while (entryIt.hasNext()) {
				Entry<Integer,String> entry = entryIt.next();

				// fix sys_list
				int numUpdated = jdbc.executeUpdate(
					"UPDATE sys_list SET " +
						"name = '" + entry.getValue() + "' " +
					"where " +
						"lst_id = 5200" +
						" and lang_id = '" + lang + "'" +
						" and entry_id = " + entry.getKey());
				if (log.isDebugEnabled()) {
					log.debug("sys_list 5100: updated " + numUpdated + " rows -> entry_id(" + entry.getKey() + "), " +
						"new name(" +	entry.getValue() + ")");
				}

				// fix data: existing keys with wrong value ! ONLY IF CATALOGLANGUAGE MAPS !
				if (lang.equals(catLang)) {
					numUpdated = jdbc.executeUpdate(
							"UPDATE t011_obj_serv_type SET " +
								"serv_type_value = '" + entry.getValue() + "' " +
							"where " +
								"serv_type_key = " + entry.getKey());
					if (log.isDebugEnabled()) {
						log.debug("t011_obj_serv_type: updated " + numUpdated + " rows -> existing serv_type_key(" + entry.getKey() + "), " +
							"new serv_type_value(" +	entry.getValue() + ")");
					}				
				}
			}
		}
	}

	protected void fixSysList528() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Fixing syslist 528 values in sys_list ...");
		}

		HashMap<Integer, String> mapKeyToNewValueListDE = new HashMap<Integer, String>();
		mapKeyToNewValueListDE.put(1, "Geometrie ohne Topologie");
		mapKeyToNewValueListDE.put(2, "Linien");
		mapKeyToNewValueListDE.put(3, "geschlossene Linien eben");
		mapKeyToNewValueListDE.put(4, "Flächen");
		mapKeyToNewValueListDE.put(5, "geschlossene Linien flächendeckend");
		mapKeyToNewValueListDE.put(6, "Flächen flächendeckend");
		mapKeyToNewValueListDE.put(7, "Körper");
		mapKeyToNewValueListDE.put(8, "3D-Oberfläche");
		mapKeyToNewValueListDE.put(9, "topologisches Gebilde ohne geometrischen Raumbezug");

		Iterator<Entry<Integer,String>> entryIt = mapKeyToNewValueListDE.entrySet().iterator();

		while (entryIt.hasNext()) {
			Entry<Integer,String> entry = entryIt.next();

			// fix sys_list
			int numUpdated = jdbc.executeUpdate(
				"UPDATE sys_list SET " +
					"name = '" + entry.getValue() + "' " +
				"where " +
					"lst_id = 528" +
					" and lang_id = 'de'" +
					" and entry_id = " + entry.getKey());
			if (log.isDebugEnabled()) {
				log.debug("sys_list 528: updated " + numUpdated + " rows -> entry_id(" + entry.getKey() + "), " +
					"new name(" +	entry.getValue() + ")");
			}
			
			// NO fixing of data values. In object data only entry id is stored !
		}
	}
}
