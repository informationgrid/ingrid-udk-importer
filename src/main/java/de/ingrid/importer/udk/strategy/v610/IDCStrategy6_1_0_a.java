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
package de.ingrid.importer.udk.strategy.v610;

import de.ingrid.importer.udk.jdbc.DBLogic;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * <p>
 * Changes InGrid 6.1.0_a
 * <p>
 * <ul>
 * <li>Update column types for UUID</li>
 * </ul>
 */
public class IDCStrategy6_1_0_a extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy6_1_0_a.class );

    private static final String MY_VERSION = VALUE_IDC_VERSION_6_1_0_a;

    public String getIDCVersion() {
        return MY_VERSION;
    }

    public void execute() throws Exception {
        jdbc.setAutoCommit( false );

        // write version of IGC structure !
        setGenericKey( KEY_IDC_VERSION, MY_VERSION );

        // delete time stamp of last update of syslists to reload all syslists
        // (reload from initial codelist file from codelist service if no repo connected).
        // Thus we guarantee syslists are up to date !
        deleteGenericKey( "lastModifiedSyslist" );

        migrateData();

        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private void migrateData() {
        try {
            
            jdbc.getDBLogic().modifyColumn("obj_uuid", DBLogic.ColumnType.VARCHAR255, "object_node", true, jdbc);
            jdbc.getDBLogic().modifyColumn("fk_obj_uuid", DBLogic.ColumnType.VARCHAR255, "object_node", false, jdbc);

            jdbc.getDBLogic().modifyColumn("addr_uuid", DBLogic.ColumnType.VARCHAR255, "address_node", true, jdbc);
            jdbc.getDBLogic().modifyColumn("fk_addr_uuid", DBLogic.ColumnType.VARCHAR255, "address_node", false, jdbc);

            jdbc.getDBLogic().modifyColumn("obj_uuid", DBLogic.ColumnType.VARCHAR255, "t01_object", true, jdbc);
            jdbc.getDBLogic().modifyColumn("adr_uuid", DBLogic.ColumnType.VARCHAR255, "t02_address", true, jdbc);
            
            jdbc.getDBLogic().modifyColumn("addr_uuid", DBLogic.ColumnType.VARCHAR255, "idc_user", true, jdbc);
            jdbc.getDBLogic().modifyColumn("obj_to_uuid", DBLogic.ColumnType.VARCHAR255, "object_reference", true, jdbc);
            
            log.info( "Migrated database successfully" );
        } catch (Exception ex) {
            log.warn( "Problems migrating database: ", ex );
        }
    }
}

