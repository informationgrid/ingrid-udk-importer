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
package de.ingrid.importer.udk.strategy.v2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategy;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 2.3 for NI:<p>
 * - Add new subtree permission type
 */
public class IDCStrategy2_3_1_add_subtree_permission extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy2_3_1_add_subtree_permission.class);

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

		System.out.print("  Add subtree permission...");
		addTreewritePermission();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void addTreewritePermission() throws Exception {
        jdbc.executeUpdate("INSERT INTO permission (id, class_name, name, action) VALUES ("
                + getNextId() + ", 'IdcEntityPermission', 'entity', 'write-subtree')");
	}

}
