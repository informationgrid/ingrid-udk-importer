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
package de.ingrid.importer.udk.strategy.v361;

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
public class IDCStrategy3_6_1_a extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy3_6_1_a.class );

    private static final String MY_VERSION = VALUE_IDC_VERSION_3_6_1_a;

    // service type key from syslist 5100
    private static final int CSW_TYPE_KEY = 1;
    private static final int WMS_TYPE_KEY = 2;
    private static final int WFS_TYPE_KEY = 3;
    private static final int WCTS_TYPE_KEY = 4;

    // new syslists containing OGC like versions of service dependent from
    // service type
    private static final int CSW_SYSLIST_ID = 5151;
    private static final int WMS_SYSLIST_ID = 5152;
    private static final int WFS_SYSLIST_ID = 5153;
    private static final int WCTS_SYSLIST_ID = 5154;

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
        addSysListsFromInitial(new int[] { CSW_SYSLIST_ID, WMS_SYSLIST_ID, WFS_SYSLIST_ID, WCTS_SYSLIST_ID });
        System.out.println( "done." );

        System.out.print( "  Migrating t011_obj_serv_version.serv_version..." );
        migrateT011ObjServVersion();
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

        log.info( "Add columns 'version_key/_value' to table 't011_obj_serv_version' ..." );
        jdbc.getDBLogic().addColumn( "version_key", ColumnType.INTEGER, "t011_obj_serv_version", false, null, jdbc );
        jdbc.getDBLogic().addColumn( "version_value", ColumnType.VARCHAR255, "t011_obj_serv_version", false, null, jdbc );

        log.info( "Extending datastructure... done\n" );
    }

    private void migrateT011ObjServVersion() throws Exception {
        log.info( "\nUpdating t011_obj_serv_version..." );

        log.info( "Transfer old 'serv_version' to new 'version_key/_value' via syslists " + "..." );

        String psSql = "SELECT type_key FROM t011_obj_serv WHERE id = ?";
        PreparedStatement psSelectServiceType = jdbc.prepareStatement( psSql );

        psSql = "UPDATE t011_obj_serv_version SET " + "version_key = ?, " + "version_value = ? " + "WHERE id = ?";
        PreparedStatement psUpdate = jdbc.prepareStatement( psSql );

        String sql = "select id, obj_serv_id, serv_version from t011_obj_serv_version";

        Statement st = jdbc.createStatement();
        ResultSet rs = jdbc.executeQuery( sql, st );
        int numMapped = 0;
        int numFree = 0;
        while (rs.next()) {
            long id = rs.getLong( "id" );
            long servId = rs.getLong( "obj_serv_id" );
            String oldVersion = rs.getString( "serv_version" );

            // NOTICE: Syslist is dependent from "Art des Dienstes", read this
            // one first (T011_obj_serv.type_key)
            psSelectServiceType.setLong( 1, servId );
            ResultSet rs2 = psSelectServiceType.executeQuery();
            Integer serviceTypeKey = 0;
            if (rs2.next()) {
                serviceTypeKey = rs2.getInt( "type_key" );
            }
            rs2.close();

            // read according syslist
            CodeList mySyslist = null;
            if (serviceTypeKey == CSW_TYPE_KEY) {
                mySyslist = InitialCodeListServiceFactory.instance().getCodeList( Integer.toString( CSW_SYSLIST_ID ) );
            } else if (serviceTypeKey == WMS_TYPE_KEY) {
                mySyslist = InitialCodeListServiceFactory.instance().getCodeList( Integer.toString( WMS_SYSLIST_ID ) );
            } else if (serviceTypeKey == WFS_TYPE_KEY) {
                mySyslist = InitialCodeListServiceFactory.instance().getCodeList( Integer.toString( WFS_SYSLIST_ID ) );
            } else if (serviceTypeKey == WCTS_TYPE_KEY) {
                mySyslist = InitialCodeListServiceFactory.instance().getCodeList( Integer.toString( WCTS_SYSLIST_ID ) );
            }

            // default is free entry (no entry found in syslist)
            int syslistKey = -1;
            String syslistValue = oldVersion;

            if (mySyslist != null && oldVersion != null && !oldVersion.trim().isEmpty()) {
                // try to map former value to syslist entry !
                boolean found = false;

                for (CodeListEntry entry : mySyslist.getEntries()) {
                    Map<String, String> entryLocalisations = entry.getLocalisations();
                    for (String entryLangId : entryLocalisations.keySet()) {
                        // we check every "language" not only "iso" ! Should all
                        // be the same !
                        String entryValue = entryLocalisations.get( entryLangId );

                        if (entryValue.toLowerCase().contains( oldVersion.toLowerCase().trim() )) {
                            syslistKey = Integer.decode( entry.getId() );
                            syslistValue = entryLocalisations.get( entryLangId );
                            found = true;
                            break;
                        }
                    }
                    if (found) {
                        break;
                    }
                }
            }

            // update
            psUpdate.setInt( 1, syslistKey );
            psUpdate.setString( 2, syslistValue );
            psUpdate.setLong( 3, id );
            psUpdate.executeUpdate();

            if (syslistKey != -1) {
                // syslist entry found
                numMapped++;
                log.info( "Updated service type '" + serviceTypeKey + "' version to syslist entry: '" + oldVersion + "' --> '" + syslistKey + "'/'" + syslistValue + "'" );
            } else {
                // syslist entry NOT found, add as free entry
                numFree++;
                log.info( "Updated service type '" + serviceTypeKey + "' version to free entry: '" + oldVersion + "' --> '" + syslistKey + "'/'" + syslistValue + "'" );
            }

        }
        rs.close();
        st.close();
        psSelectServiceType.close();
        psUpdate.close();

        log.info( "Mapped " + numMapped + " versions to new syslist entries." );
        log.info( "Mapped " + numFree + " versions to FREE entries." );
        log.info( "Updating t011_obj_serv_version... done\n" );
    }

    private void cleanUpDataStructure() throws Exception {
        log.info( "\nCleaning up datastructure -> CAUSES COMMIT ! ..." );

        log.info( "Drop column 'serv_version' from table 't011_obj_serv_version' ..." );
        jdbc.getDBLogic().dropColumn( "serv_version", "t011_obj_serv_version", jdbc );

        log.info( "Cleaning up datastructure... done\n" );
    }
}
