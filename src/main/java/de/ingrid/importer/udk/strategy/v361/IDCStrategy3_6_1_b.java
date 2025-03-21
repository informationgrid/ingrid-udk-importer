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
package de.ingrid.importer.udk.strategy.v361;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 3.6.1
 * <p>
 * <ul>
 * <li>Add object_use_constraint table with license_key/_value for editing
 * ISO useConstraints (syslist 6500), see
 * https://dev.informationgrid.eu/redmine/issues/13 (part 2.)
 * </ul>
 */
public class IDCStrategy3_6_1_b extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy3_6_1_b.class );

    private static final String MY_VERSION = VALUE_IDC_VERSION_3_6_1_b;

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

        System.out.print( "  Updating sys_list..." );
        addSysListsFromInitial( new int[] { 6500 } );
        System.out.println( "done." );

        System.out.print( "  Migrating object_use to new object_use_constraint..." );
        migrateObjectUse();
        System.out.println( "done." );

        // FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause
        // commit (e.g. on MySQL)
        // ---------------------------------

        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private void extendDataStructure() throws Exception {
        log.info( "\nExtending datastructure -> CAUSES COMMIT ! ..." );

        log.info( "Create table 'object_use_constraint'..." );
        jdbc.getDBLogic().createTableObjectUseConstraint( jdbc );

        log.info( "Extending datastructure... done\n" );
    }

    private void migrateObjectUse() throws Exception {
        log.info( "\nUpdating object_use and object_use_constraint..." );

        log.info( "Transfer license from object_use to object_use_constraint ..." );

        // object_use may contain entries from license syslist if the object is
        // open data.
        // Then entries from syslist 6500 could be selected and transferred to
        // text field.
        // The object_use.terms_of_use_key then is the key of the syslist entry
        // !

        PreparedStatement psSelectIsOpenData = jdbc.prepareStatement( "SELECT is_open_data FROM t01_object WHERE id = ?" );

        PreparedStatement psInsertObjUseConstr = jdbc.prepareStatement( "INSERT INTO object_use_constraint " + "(id, obj_id, line, license_key, license_value) "
                + "VALUES (?,?,?,?,?)" );

        String sql = "select id, obj_id, terms_of_use_key, terms_of_use_value from object_use";

        Statement st = jdbc.createStatement();
        ResultSet rs = jdbc.executeQuery( sql, st );
        int numTransferred = 0;
        while (rs.next()) {
            long objId = rs.getLong( "obj_id" );
            int useKey = rs.getInt( "terms_of_use_key" );
            String useValue = rs.getString( "terms_of_use_value" );

            if (useKey > 0 && useValue != null && useValue.trim().length() > 0) {
                // no free entry ! Content was selected from syslist !

                // check Open data
                psSelectIsOpenData.setLong( 1, objId );
                ResultSet rs2 = psSelectIsOpenData.executeQuery();
                String isOpenData = "";
                if (rs2.next()) {
                    isOpenData = rs2.getString( "is_open_data" );
                }
                rs2.close();

                // If "Open Data" then value is license from syslist 6500
                if ("Y".equals( isOpenData )) {
                    // is license, we transfer
                    psInsertObjUseConstr.setLong( 1, getNextId() ); // id
                    psInsertObjUseConstr.setLong( 2, objId ); // obj_id
                    psInsertObjUseConstr.setInt( 3, 1 ); // line
                    psInsertObjUseConstr.setInt( 4, useKey ); // license_key
                    psInsertObjUseConstr.setString( 5, useValue ); // license_value
                    int numUpdated = psInsertObjUseConstr.executeUpdate();
                    if (numUpdated > 0) {
                        log.info( "Insert object_use_constraint [key:" + useKey + "/value:'" + useValue + "'] to OBJECT [id:" + objId + "] !" );
                        numTransferred++; 
                    } else {
                        log.warn( "PROBLEMS inserting object_use_constraint [key:" + useKey + "/value:'" + useValue + "'] to OBJECT [id:" + objId + "] !" );
                    }
                }
            }
        }
        rs.close();
        st.close();
        psSelectIsOpenData.close();
        psInsertObjUseConstr.close();

        log.info( "Copied " + numTransferred + " licenses from object_use to object_use_constraint." );

        // finally set all object_use entries to free text !
        int numUpdated = jdbc.executeUpdate( "UPDATE object_use SET terms_of_use_key = -1" );
        log.info( "Updated " + numUpdated + " entries in object_use to free text (terms_of_use_key = -1)." );

        log.info( "Updating object_use and object_use_constraint... done\n" );
    }
}
