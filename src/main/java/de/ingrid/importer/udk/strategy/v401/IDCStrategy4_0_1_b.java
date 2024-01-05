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
package de.ingrid.importer.udk.strategy.v401;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 4.0.1
 * <p>
 * <ul>
 * <li>Add column 'lastexpiry_time' to tables 'object_metadata' and 'address_metadata' for saving date of sent expiry email, see https://dev.informationgrid.eu/redmine/issues/107
 * </ul>
 */
public class IDCStrategy4_0_1_b extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy4_0_1_b.class );

    private static final String MY_VERSION = VALUE_IDC_VERSION_4_0_1_b;

    public String getIDCVersion() {
        return MY_VERSION;
    }

    public void execute() throws Exception {
        jdbc.setAutoCommit( false );

        // write version of IGC structure !
        setGenericKey( KEY_IDC_VERSION, MY_VERSION );

        // THEN EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit
        // (e.g. on MySQL)
        // ---------------------------------

        System.out.print( "  Extend datastructure..." );
        extendDataStructure();
        System.out.println( "done." );

        // FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause
        // commit (e.g. on MySQL)
        // ---------------------------------

        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private void extendDataStructure() throws Exception {
        log.info( "\nExtending datastructure -> CAUSES COMMIT ! ..." );

        log.info( "Add column 'lastexpiry_time' to table 'object_metadata' ..." );
        jdbc.getDBLogic().addColumn( "lastexpiry_time", ColumnType.VARCHAR17, "object_metadata", false, null, jdbc );

        log.info( "Add column 'lastexpiry_time' to table 'address_metadata' ..." );
        jdbc.getDBLogic().addColumn( "lastexpiry_time", ColumnType.VARCHAR17, "address_metadata", false, null, jdbc );

        log.info( "Extending datastructure... done\n" );
    }

}
