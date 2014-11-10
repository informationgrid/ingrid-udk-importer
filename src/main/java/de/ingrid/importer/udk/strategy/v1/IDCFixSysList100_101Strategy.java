/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 wemove digital solutions GmbH
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

import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategy;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * Single Strategy for fixing wrong syslist 100, 101 KEYS/VALUES,
 * see http://88.198.11.89/jira/browse/INGRIDII-245 
 * @author martin
 */
public class IDCFixSysList100_101Strategy extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCFixSysList100_101Strategy.class);

	// maps mapping wrong syslist entryIds to correct entryIds <wrongEntryId, rightEntryId>
	private HashMap<Integer, Integer> mapWrongKeyToRightKeyList100;
	private HashMap<Integer, Integer> mapWrongKeyToRightKeyList101;

	public IDCFixSysList100_101Strategy() {
		super();

		// set up map to fix syslist 100
		mapWrongKeyToRightKeyList100 = new HashMap<Integer, Integer>();
		mapWrongKeyToRightKeyList100.put(1, 3068);
		mapWrongKeyToRightKeyList100.put(2, 4178);
		mapWrongKeyToRightKeyList100.put(3, 4230);
		mapWrongKeyToRightKeyList100.put(4, 4258);
		mapWrongKeyToRightKeyList100.put(5, 4284);
		mapWrongKeyToRightKeyList100.put(6, 4314);
		mapWrongKeyToRightKeyList100.put(7, 4326);
		mapWrongKeyToRightKeyList100.put(8, 23031);
		mapWrongKeyToRightKeyList100.put(9, 23032);
		mapWrongKeyToRightKeyList100.put(10, 23033);
		mapWrongKeyToRightKeyList100.put(11, 32631);
		mapWrongKeyToRightKeyList100.put(12, 32632);
		mapWrongKeyToRightKeyList100.put(13, 32633);
		mapWrongKeyToRightKeyList100.put(14, 25831);
		mapWrongKeyToRightKeyList100.put(15, 25832);
		mapWrongKeyToRightKeyList100.put(16, 25833);
		mapWrongKeyToRightKeyList100.put(17, 25834);
		mapWrongKeyToRightKeyList100.put(18, 28462);
		mapWrongKeyToRightKeyList100.put(19, 28463);
		mapWrongKeyToRightKeyList100.put(20, 31466);
		mapWrongKeyToRightKeyList100.put(21, 31467);
		mapWrongKeyToRightKeyList100.put(22, 31468);
		mapWrongKeyToRightKeyList100.put(23, 31469);
		mapWrongKeyToRightKeyList100.put(24, 9000001);
		mapWrongKeyToRightKeyList100.put(25, 9000002);
		mapWrongKeyToRightKeyList100.put(26, 9000007);
		mapWrongKeyToRightKeyList100.put(27, 9000008);
		mapWrongKeyToRightKeyList100.put(28, 9000009);
		mapWrongKeyToRightKeyList100.put(29, 9000010);
		mapWrongKeyToRightKeyList100.put(30, 9000011);
		mapWrongKeyToRightKeyList100.put(31, 9000012);
		mapWrongKeyToRightKeyList100.put(32, 9000003);
		mapWrongKeyToRightKeyList100.put(33, 9000005);
		mapWrongKeyToRightKeyList100.put(34, 9000006);
		mapWrongKeyToRightKeyList100.put(35, 9000013);

		// set up map to fix syslist 101
		mapWrongKeyToRightKeyList101 = new HashMap<Integer, Integer>();
		mapWrongKeyToRightKeyList101.put(1, 900002);
		mapWrongKeyToRightKeyList101.put(2, 900003);
		mapWrongKeyToRightKeyList101.put(3, 900005);
		mapWrongKeyToRightKeyList101.put(4, 900006);
		mapWrongKeyToRightKeyList101.put(5, 900007);
		mapWrongKeyToRightKeyList101.put(6, 900008);
		mapWrongKeyToRightKeyList101.put(7, 900004);
		mapWrongKeyToRightKeyList101.put(8, 900009);
		mapWrongKeyToRightKeyList101.put(9, 5129);
		mapWrongKeyToRightKeyList101.put(10, 5105);
		mapWrongKeyToRightKeyList101.put(11, 900010);
	}

	/**
	 * Write NO Version, this strategy should be executed on its own on chosen catalogues
	 * @see de.ingrid.importer.udk.strategy.IDCStrategy#getIDCVersion()
	 */
	public String getIDCVersion() {
		return null;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		System.out.print("  Fixing syslist 100 codes/values ...");
		fixSysList100();
		System.out.println("done.");
		System.out.print("  Fixing syslist 101 codes ...");
		fixSysList101();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Fix finished successfully.");
	}

	protected void fixSysList100() throws Exception {
		// fix keys and values in sys_list and t011_obj_geo
		if (log.isInfoEnabled()) {
			log.info("Fixing syslist 100 in sys_list and t011_obj_geo ...");
		}

		Iterator<Integer> oldEntryIdIt = mapWrongKeyToRightKeyList100.keySet().iterator();
		while (oldEntryIdIt.hasNext()) {
			Integer oldEntryId = oldEntryIdIt.next();
			Integer newEntryId = mapWrongKeyToRightKeyList100.get(oldEntryId);
			String newEntryValue = mapNewKeyToNewValueList100.get(newEntryId);

			// fix sys_list
			int numUpdated = jdbc.executeUpdate(
				"UPDATE sys_list SET " +
					"entry_id = " + newEntryId + ", " +
					"name = '" + newEntryValue + "' " +
				"where " +
					"lst_id = 100 " +
					"and entry_id = " + oldEntryId);
			if (log.isDebugEnabled()) {
				log.debug("sys_list 100: updated " + numUpdated + " rows -> old entry_id(" + oldEntryId + "), " +
					"new entry_id(" + newEntryId + "), new name(" +	newEntryValue + ")");
			}

			// fix data: existing new keys with wrong value !
			numUpdated = jdbc.executeUpdate(
					"UPDATE t011_obj_geo SET " +
						"referencesystem_value = '" + newEntryValue + "' " +
					"where " +
						"referencesystem_key = " + newEntryId);
			if (log.isDebugEnabled()) {
				log.debug("t011_obj_geo: updated " + numUpdated + " rows -> existing new referencesystem_key(" + newEntryId + "), " +
					"new referencesystem_value(" +	newEntryValue + ")");
			}

			// fix data: existing old keys with wrong value !
			numUpdated = jdbc.executeUpdate(
				"UPDATE t011_obj_geo SET " +
					"referencesystem_key = " + newEntryId + ", " +
					"referencesystem_value = '" + newEntryValue + "' " +
				"where " +
					"referencesystem_key = " + oldEntryId);
			if (log.isDebugEnabled()) {
				log.debug("t011_obj_geo: updated " + numUpdated + " rows -> old referencesystem_key(" + oldEntryId + "), " +
					"new referencesystem_key(" + newEntryId + "), new referencesystem_value(" +	newEntryValue + ")");
			}
		}
	}

	protected void fixSysList101() throws Exception {
		// fix keys and values in sys_list and t01_object
		if (log.isInfoEnabled()) {
			log.info("Fixing syslist 101 in sys_list and t01_object ...");
		}

		Iterator<Integer> oldEntryIdIt = mapWrongKeyToRightKeyList101.keySet().iterator();
		while (oldEntryIdIt.hasNext()) {
			Integer oldEntryId = oldEntryIdIt.next();
			Integer newEntryId = mapWrongKeyToRightKeyList101.get(oldEntryId);
			String newEntryValue = mapNewKeyToNewValueList101.get(newEntryId);

			// fix sys_list
			int numUpdated = jdbc.executeUpdate(
				"UPDATE sys_list SET " +
					"entry_id = " + newEntryId + ", " +
					"name = '" + newEntryValue + "' " +
				"where " +
					"lst_id = 101 " +
					"and entry_id = " + oldEntryId);
			if (log.isDebugEnabled()) {
				log.debug("sys_list 101: updated " + numUpdated + " rows -> old entry_id(" + oldEntryId + "), " +
					"new entry_id(" + newEntryId + "), new name(" +	newEntryValue + ")");
			}

			// fix data: existing old keys !
			numUpdated = jdbc.executeUpdate(
				"UPDATE t01_object SET " +
					"vertical_extent_vdatum = " + newEntryId +
				" where " +
					"vertical_extent_vdatum = " + oldEntryId);
			if (log.isDebugEnabled()) {
				log.debug("t01_object: updated " + numUpdated + " rows -> old vertical_extent_vdatum(" + oldEntryId + "), " +
					"new vertical_extent_vdatum(" + newEntryId + ")");
			}
		}
	}
}
