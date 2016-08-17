/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2016 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v400;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.codelists.model.CodeList;
import de.ingrid.codelists.model.CodeListEntry;
import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.importer.udk.util.InitialCodeListServiceFactory;

/**
 * <p>
 * Changes InGrid 3.6.1
 * <p>
 * <ul>
 * <li>replace t011_obj_serv_version.serv_version with version_key/_value for
 * new syslists, see https://redmine.wemove.com/issues/724
 * (https://dev.informationgrid.eu/redmine/issues/47)
 * </ul>
 */
public class IDCStrategy4_0_0_a extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy4_0_0_a.class );

    private static final String MY_VERSION = VALUE_IDC_VERSION_4_0_0_a;

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

        log.info( "Add column 'hours_of_service' to table 't02_address' ..." );
        jdbc.getDBLogic().addColumn( "hours_of_service", ColumnType.VARCHAR255, "t02_address", false, null, jdbc );

        log.info( "Extending datastructure... done\n" );
    }

}
