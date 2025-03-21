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
package de.ingrid.importer.udk.strategy.v540;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * <p>
 * Changes InGrid 5.4.0_b
 * <p>
 * Add indices to the fields "parent_id" and "key_id" in the table "additional_field_data" to speed up the creation of statistics
 */

public class IDCStrategy5_4_0_b extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy5_4_0_b.class );

    private static final String MY_VERSION = VALUE_IDC_VERSION_5_4_0_b;

    public String getIDCVersion() {
        // Returning version here enables strategy workflow !
        // So all former versions in IDCStrategy.STRATEGY_WORKFLOW are executed !
        // Returning null disables version tracking ...
        // Well we keep version here having a special strategy:
        // - no version written to catalog
        // - but all former versions in workflow are executed, if catalog version is below this one !
        // - return null here if you want to execute this one on its own without strategy workflow (can be changed later on when higher strategy added !)
        return MY_VERSION;
    }

    public void execute() throws Exception {
        jdbc.setAutoCommit( false );

        // NOTICE:
        // This is a "fix strategy" writing no version !

        // do write version of IGC structure, since migration shall only be run once!
        setGenericKey(KEY_IDC_VERSION, MY_VERSION);

        // THEN PERFORM DATA MANIPULATIONS !
        // ---------------------------------

        log.info( "Adding index to 'parent_field_id' in table 'additional_field_data'" );
        addIndex("additional_field_data", "parent_field_id", "idx_ParentId");

        log.info( "Adding index to 'field_key' in table 'additional_field_data'" );
        addIndex("additional_field_data", "field_key", "idx_FieldKey");

        jdbc.commit();
        System.out.println( "Operation finished successfully." );
    }

    private void addIndex(String tableName, String colName, String indexName) throws Exception {
        int indexExists;

        indexExists = jdbc.getDBLogic().checkIndexExists( jdbc, tableName, indexName );

        if (indexExists == 0) {
            jdbc.getDBLogic().addIndex( colName, tableName, indexName, jdbc );
            log.info( "Done." );
        } else {
            log.warn( "Index already exists" );
        }
    }
}
