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
package de.ingrid.importer.udk.strategy.v30;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategy;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * Changes InGrid 3.0:<p>
 * - fixing Syslist 510 (ISO MD_CharacterSetCode) see https://dev.wemove.com/jira/browse/INGRID23-58
 */
public class IDCStrategy3_0_0_fixSyslist extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_0_0_fixSyslist.class);

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

		System.out.print("  Updating sys_list ...");
		updateSysList();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	protected void updateSysList() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating sys_list...");
		}

// ---------------------------
		int lstId = 510;
		if (log.isInfoEnabled()) {
			log.info("Changing/Updating syslist " + lstId +	"(ISO MD_CharacterSetCode)...");
		}
		// map from old (map-key) to new value (map-value)
		Map<String, String> updateEntryMap = new HashMap<String, String>();
		updateEntryMap.put("Jis", "jis");
		updateEntryMap.put("ShiftJIS", "shiftJIS");
		updateEntryMap.put("EucJP", "eucJP");
		updateEntryMap.put("UsAscii", "usAscii");
		updateEntryMap.put("Ebcdic", "ebcdic");
		updateEntryMap.put("EucKR", "eucKR");

		Iterator<String> itrOldValue = updateEntryMap.keySet().iterator();
		while (itrOldValue.hasNext()) {
			String oldValue = itrOldValue.next();
			String newValue = updateEntryMap.get(oldValue);

			int numUpdated = jdbc.executeUpdate("UPDATE sys_list" +
					" SET name = '" + newValue + "'" +  
					" WHERE lst_id = " + lstId + 
					" AND name = '" + oldValue + "'");

			if (log.isDebugEnabled()) {
				log.debug("Updated " + numUpdated +	" entries from '" + oldValue +
					"' to '" + newValue + "' in syslist " + lstId + " (de + en)...");
			}
		}

		// insert new entry with entry id (map-key) and entry value (map-value)
		Map<Integer, String> newEntryMap = new LinkedHashMap<Integer, String>();
		newEntryMap.put(25, "8859part10");
		newEntryMap.put(26, "8859part13");
		newEntryMap.put(27, "8859part16");
		newEntryMap.put(28, "GB2312");

		Map<Integer, String> newDescriptionMap_de = new HashMap<Integer, String>();
		newDescriptionMap_de.put(25, "ISO/IEC 8859-10, IT - 8-Bit Einzelbyte codierter grafischer Zeichensatz - Teil 10: Lateinisches Alphabet Nr. 6");
		newDescriptionMap_de.put(26, "ISO/IEC 8859-13, IT - 8-Bit Einzelbyte codierter grafischer Zeichensatz - Teil 13: Lateinisches Alphabet Nr. 7");
		newDescriptionMap_de.put(27, "ISO/IEC 8859-16, IT - 8-Bit Einzelbyte codierter grafischer Zeichensatz - Teil 16: Lateinisches Alphabet Nr. 10");
		newDescriptionMap_de.put(28, "vereinfachter chinesischer Zeichensatz");

		Map<Integer, String> newDescriptionMap_en = new HashMap<Integer, String>();
		newDescriptionMap_en.put(25, "ISO/IEC 8859-10, Information technology - 8-bit single-byte coded graphic character sets - Part 10: Latin alphabet No. 6");
		newDescriptionMap_en.put(26, "ISO/IEC 8859-13, Information technology - 8-bit single-byte coded graphic character sets - Part 13: Latin alphabet No. 7");
		newDescriptionMap_en.put(27, "ISO/IEC 8859-16, Information technology - 8-bit single-byte coded graphic character sets - Part 16: Latin alphabet No. 10");
		newDescriptionMap_en.put(28, "simplified Chinese code set");

		Iterator<Integer> itrId = newEntryMap.keySet().iterator();
		while (itrId.hasNext()) {
			int entryId = itrId.next();
			String entryValue = newEntryMap.get(entryId);
			// german version
			int numUpdated = 0;
			numUpdated += jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, description, maintainable, is_default) VALUES ("
					+ getNextId() + ", " + lstId + ", " + entryId + ", 'de', '" + entryValue + "', '" + newDescriptionMap_de.get(entryId) + "', 0, 'N')");
			// english version
			numUpdated += jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, description, maintainable, is_default) VALUES ("
					+ getNextId() + ", " + lstId + ", " + entryId + ", 'en', '" + entryValue + "', '" + newDescriptionMap_en.get(entryId) + "', 0, 'N')");

			if (log.isDebugEnabled()) {
				log.debug("Inserted " + numUpdated + " new entry(ies) '" + entryValue +
					"' in syslist " + lstId + " (de + en)...");
			}
		}
// ---------------------------

		if (log.isInfoEnabled()) {
			log.info("Updating sys_list... done");
		}
	}
}
