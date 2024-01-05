/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2024 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v33;

import java.sql.PreparedStatement;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * @author Administrator
 * 
 */
public abstract class IDCStrategyDefault3_3 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategyDefault3_3.class);

	/**
	 * Also drops all old values (if syslist already exists) !
	 * @param listId id of syslist
	 * @param deleteOldValues pass true if all old syslist values should be deleted before adding new ones !
	 * @param syslistMap_de german entries
	 * @param syslistMap_en english entries
	 * @param defaultEntry_de pass key of GERMAN default entry or -1 if no default entry !
	 * @param defaultEntry_en pass key of ENGLISH default entry or -1 if no default entry !
	 * @param syslistMap_descr_de pass null if no GERMAN description available
	 * @param syslistMap_descr_en pass null if no ENGLISH description available
	 * @throws Exception
	 */
	protected void writeNewSyslist(int listId,
			boolean deleteOldValues,
			LinkedHashMap<Integer, String> syslistMap_de,
			LinkedHashMap<Integer, String> syslistMap_en,
			int defaultEntry_de,
			int defaultEntry_en,
			LinkedHashMap<Integer, String> syslistMap_descr_de,
			LinkedHashMap<Integer, String> syslistMap_descr_en) throws Exception {
		
		if (syslistMap_descr_de == null) {
			syslistMap_descr_de = new LinkedHashMap<Integer, String>();
		}
		if (syslistMap_descr_en == null) {
			syslistMap_descr_en = new LinkedHashMap<Integer, String>();
		}

		if (deleteOldValues) {
			// clean up, to guarantee no old values !
			sqlStr = "DELETE FROM sys_list where lst_id = " + listId;
			jdbc.executeUpdate(sqlStr);
		}

		String psSql = "INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default, description) " +
				"VALUES (?,?,?,?,?,?,?,?)";		
		PreparedStatement psInsert = jdbc.prepareStatement(psSql);

		Iterator<Integer> itr = syslistMap_de.keySet().iterator();
		while (itr.hasNext()) {
			int key = itr.next();
			// german version
			String isDefault = "N";
			if (key == defaultEntry_de) {
				isDefault = "Y";				
			}
			psInsert.setLong(1, getNextId());
			psInsert.setInt(2, listId);
			psInsert.setInt(3, key);
			psInsert.setString(4, "de");
			psInsert.setString(5, syslistMap_de.get(key));
			psInsert.setInt(6, 0);
			psInsert.setString(7, isDefault);
			psInsert.setString(8, syslistMap_descr_de.get(key));
			psInsert.executeUpdate();

			// english version
			isDefault = "N";
			if (key == defaultEntry_en) {
				isDefault = "Y";				
			}
			psInsert.setLong(1, getNextId());
			psInsert.setString(4, "en");
			psInsert.setString(5, syslistMap_en.get(key));
			psInsert.setString(7, isDefault);
			psInsert.setString(8, syslistMap_descr_en.get(key));
			psInsert.executeUpdate();
		}

		psInsert.close();
	}
}
