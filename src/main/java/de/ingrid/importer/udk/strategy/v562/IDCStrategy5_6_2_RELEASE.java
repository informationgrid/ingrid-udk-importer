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
package de.ingrid.importer.udk.strategy.v562;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 *     RELEASE InGrid 5.6.2
 *     Only remove timestamp for codelists to force update.
 * </p>
 */
public class IDCStrategy5_6_2_RELEASE extends IDCStrategyDefault {

	private static final String MY_VERSION = VALUE_IDC_VERSION_5_6_2_RELEASE;

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		// delete time stamp of last update of syslists to reload all syslists
		// (reload from initial codelist file from codelist service if no repo connected).
		// Thus we guarantee syslists are up to date !
		deleteGenericKey("lastModifiedSyslist");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}
}
