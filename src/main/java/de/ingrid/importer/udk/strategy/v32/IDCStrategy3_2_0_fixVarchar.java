/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2021 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v32;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.importer.udk.strategy.IDCStrategy;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * Independent Strategy !
 * Change several fields from VARCHAR 255 to TEXT !
 */
public class IDCStrategy3_2_0_fixVarchar extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_2_0_fixVarchar.class);

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

		// EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit (e.g. on MySQL)
		// ---------------------------------
		System.out.print("  Extend datastructure...");
		extendDataStructure();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void extendDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Extending datastructure -> CAUSES COMMIT ! ...");
		}

		// change VARCHAR 255 to TEXT !
		List<String[]> tableColumns = new ArrayList<String[]>();
		tableColumns.add(new String[] { "t011_obj_geo_supplinfo", "feature_type" });
		tableColumns.add(new String[] { "t011_obj_literature", "author" });
		tableColumns.add(new String[] { "t011_obj_literature", "publisher" });
		tableColumns.add(new String[] { "t011_obj_literature", "publish_in" });
		tableColumns.add(new String[] { "t011_obj_literature", "publish_loc" });
		tableColumns.add(new String[] { "t011_obj_literature", "loc" });
		tableColumns.add(new String[] { "t011_obj_literature", "doc_info" });
		tableColumns.add(new String[] { "t011_obj_literature", "publishing" });
		tableColumns.add(new String[] { "t011_obj_data", "base" });
		
		for (String[] tC : tableColumns) {
			if (log.isInfoEnabled()) {
				log.info("Change field type of '" + tC[0] + "." + tC[1] + "' to TEXT ...");
			}
			jdbc.getDBLogic().modifyColumn(tC[1], ColumnType.TEXT_NO_CLOB, tC[0], false, jdbc);
		}

		if (log.isInfoEnabled()) {
			log.info("Extending datastructure... done");
		}
	}
}
