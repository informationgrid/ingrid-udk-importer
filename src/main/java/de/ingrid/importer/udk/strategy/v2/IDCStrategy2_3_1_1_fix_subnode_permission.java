/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2020 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * FIX InGrid 2.3 for NI:<p>
 * - Rename "write_subtree" permission to "write-subnode"
 */
public class IDCStrategy2_3_1_1_fix_subnode_permission extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy2_3_1_1_fix_subnode_permission.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_2_3_1_1_FIX_SUBNODE_PERMISSION;

    public String getIDCVersion() {
		return MY_VERSION;
    }

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		System.out.print("  Fix subnode permission...");
		fixPermission();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void fixPermission() throws Exception {
		int numUpdated = jdbc.executeUpdate(
			"UPDATE permission SET " +
				"action = 'write-subnode' " +
			"where " +
				"action = 'write-subtree'");
		if (log.isDebugEnabled()) {
			log.debug("Rename permission 'write-subtree' to 'write-subnode': updated " + numUpdated + " records");
		}
	}
}
