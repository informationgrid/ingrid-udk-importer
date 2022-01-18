/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2022 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v404;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 4.0.4
 * <p>
 * <ul>
 * <li>Add table object_data_language for multiple data languages + migrate from
 * t01_object.data_language_key/_value + delete old columns, see
 * https://dev.informationgrid.eu/redmine/issues/199
 * </ul>
 */
public class IDCStrategy4_0_4_a extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy4_0_4_a.class );

    private static final String MY_VERSION = VALUE_IDC_VERSION_4_0_4_a;

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

        // THEN PERFORM DATA MANIPULATIONS !
        // ---------------------------------

        System.out.print( "  Migrating t01_object.data_language_key/_value to new object_data_language..." );
        migrateObjectDataLanguage();
        System.out.println( "done." );

        // FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause
        // commit (e.g. on MySQL)
        // ---------------------------------

        System.out.print( "  Clean up datastructure..." );
        cleanUpDataStructure();
        System.out.println( "done." );

        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private void extendDataStructure() throws Exception {
        log.info( "\nExtending datastructure -> CAUSES COMMIT ! ..." );

        log.info( "Create table 'object_data_language'..." );
        jdbc.getDBLogic().createTableObjectDataLanguage( jdbc );

        log.info( "Extending datastructure... done\n" );
    }

    private void migrateObjectDataLanguage() throws Exception {
        log.info( "\nMigrating to object_data_language..." );

        log.info( "Transfer data_language_key/_value from t01_object to object_data_language ..." );

        PreparedStatement psInsertObjDataLang = jdbc.prepareStatement( "INSERT INTO object_data_language " + "(id, obj_id, line, data_language_key, data_language_value) "
                + "VALUES (?,?,?,?,?)" );

        String sql = "select id, data_language_key, data_language_value from t01_object";

        Statement st = jdbc.createStatement();
        ResultSet rs = jdbc.executeQuery( sql, st );
        int numTransferred = 0;
        while (rs.next()) {
            long objId = rs.getLong( "id" );
            int myKey = rs.getInt( "data_language_key" );
            String myValue = rs.getString( "data_language_value" );

            if (myKey > 0 && myValue != null) {
                psInsertObjDataLang.setLong( 1, getNextId() );
                psInsertObjDataLang.setLong( 2, objId );
                psInsertObjDataLang.setInt( 3, 1 ); // line
                psInsertObjDataLang.setInt( 4, myKey );
                psInsertObjDataLang.setString( 5, myValue );
                int numUpdated = psInsertObjDataLang.executeUpdate();
                if (numUpdated > 0) {
                    log.info( "Insert object_data_language [key:" + myKey + "/value:'" + myValue + "'] to OBJECT [id:" + objId + "] !" );
                    numTransferred++;
                } else {
                    log.warn( "PROBLEMS inserting object_data_language [key:" + myKey + "/value:'" + myValue + "'] to OBJECT [id:" + objId + "] !" );
                }
            }
        }
        rs.close();
        st.close();
        psInsertObjDataLang.close();

        log.info( "Migrated " + numTransferred + " data_language_key/_value from t01_object to object_data_language." );

        log.info( "Migrating to object_data_language... done\n" );
    }

    private void cleanUpDataStructure() throws Exception {
        log.info( "\nCleaning up datastructure -> CAUSES COMMIT ! ..." );

        log.info( "Drop columns 'data_language_key/_value' from table 't01_object' ..." );
        jdbc.getDBLogic().dropColumn( "data_language_key", "t01_object", jdbc );
        jdbc.getDBLogic().dropColumn( "data_language_value", "t01_object", jdbc );

        log.info( "Cleaning up datastructure... done\n" );
    }
}
