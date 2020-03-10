/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2020 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v540;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 5.4.0_d
 * <p>
 * <ul>
 * <li>Add new field metadata_time for metadata date and transfer mod_time to metadata_time
 * see https://redmine.informationgrid.eu/issues/1084 (part 3.)
 * </ul>
 */
public class IDCStrategy5_4_0_d extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy5_4_0_d.class );

    private static final String MY_VERSION = VALUE_IDC_VERSION_5_4_0_d;

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

        System.out.println( "  Extend datastructure..." );
        extendDataStructure();
        System.out.println( "done." );

        // THEN PERFORM DATA MANIPULATIONS !
        // ---------------------------------

        System.out.println( "  Migrating mod_time to new metadata_time..." );
        migrateData();
        System.out.println( "done." );

        // FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause
        // commit (e.g. on MySQL)
        // ---------------------------------

        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private void extendDataStructure() throws Exception {
        log.info( "Extending datastructure t01_object add column metadata_time -> CAUSES COMMIT ! ..." );

        jdbc.getDBLogic().addColumn( "metadata_time", ColumnType.VARCHAR17, "t01_object", false, null, jdbc );

        log.info( "Extending datastructure... done" );
    }

    private void migrateData() throws SQLException {
        log.info( "Updating t01_object..." );
        log.info( "Transfer mod_time to metadata_time ..." );

        PreparedStatement psSelect = jdbc.prepareStatement("SELECT id, mod_time FROM t01_object");
        PreparedStatement psUpdate = jdbc.prepareStatement("UPDATE t01_object SET metadata_time=? WHERE id=?");

        ResultSet rs = psSelect.executeQuery();
        int numTransferred = 0;
        while (rs.next()) {
            long id = rs.getLong("id");
            String modTime = rs.getString( "mod_time" );

            psUpdate.setString(1, modTime);
            psUpdate.setLong(2, id);

            int numUpdated = psUpdate.executeUpdate();
            if (numUpdated > 0) {
                numTransferred++; 
            } else {
                log.warn( "PROBLEMS transferring [mod_time:'" + modTime + "'] in t01_object [id:" + id + "] !" );
            }
        }
        rs.close();
        psSelect.close();
        psUpdate.close();

        log.info( "Transferred " + numTransferred + " mod_time to metadata_time." );
        log.info( "Updating t01_object... done" );
    }
}
