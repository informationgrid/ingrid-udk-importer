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
package de.ingrid.importer.udk.strategy.v440;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategy;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.importer.udk.strategy.v431.IDCStrategy4_3_1_fixSearchtermReferences;

/**
 * <p>
 * Changes InGrid 4.4.0
 * <ul>
 *   <li>Fix references to removed topic 'Kein INSPIRE-Thema' in <b>dev branch</b>
 *   see <a href="https://redmine.informationgrid.eu/issues/816">#816</a> 
 *   <li>This is the fix for 4.4 dev branch, calls 4_3_1_fixSearchtermReferences strategy (merged from 4.3.x branch) !
 *   <li>We do NOT write a new version to the catalog BUT execute all strategies in the workflow up to this strategy (if catalog has older version) !
 * </ul>
 */
public class IDCStrategy4_4_0_c extends IDCStrategyDefault {

    private static final Log LOG = LogFactory.getLog(IDCStrategy4_4_0_c.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_4_4_0_c;

	public String getIDCVersion() {
	    // we return version, so all versions up to this version are executed on catalog, if older catalog version !
	    // return null if only this version (strategy) should be executed
		return MY_VERSION;
	}

	public void execute() throws Exception {
		// NOTICE: Writes NO version to catalog ! ("fix strategy", just data fixed)

		// NOTICE: This strategy does NOT write version to database cause is fix strategy !
		IDCStrategy strategyToExecute = new IDCStrategy4_3_1_fixSearchtermReferences();
		strategyToExecute.setJDBCConnectionProxy( jdbc );
		strategyToExecute.execute();

        LOG.info("Update finished successfully.");
	}
}
