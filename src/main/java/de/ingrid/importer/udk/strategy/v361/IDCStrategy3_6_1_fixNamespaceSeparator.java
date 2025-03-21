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
 * <li>Separate namespace and resource identifier with "/" instead of "#", see
 * https://dev.informationgrid.eu/redmine/issues/13
 * </ul>
 * Writes NO Catalog Schema Version to catalog and can be executed on its own !
 */
public class IDCStrategy3_6_1_fixNamespaceSeparator extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy3_6_1_fixNamespaceSeparator.class );

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
        jdbc.setAutoCommit( false );

        // THEN PERFORM DATA MANIPULATIONS !
        // ---------------------------------

        System.out.print( "  Migrating t03_catalogue.cat_namespace..." );
        migrateT03CatalogueCatNamespace();
        System.out.println( "done." );

        System.out.print( "  Migrating t011_obj_geo.datasource_uuid..." );
        migrateT011ObjGeoDatasourceUuid();
        System.out.println( "done." );

        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private void migrateT03CatalogueCatNamespace() throws Exception {
        log.info( "\nUpdating t03_catalogue.cat_namespace..." );

        log.info( "Replace old \"#\" with new \"/\" separator ..." );

        String psSql = "UPDATE t03_catalogue SET cat_namespace = ? WHERE id = ?";
        PreparedStatement psUpdate = jdbc.prepareStatement( psSql );

        String sql = "select id, cat_namespace from t03_catalogue";

        Statement st = jdbc.createStatement();
        ResultSet rs = jdbc.executeQuery( sql, st );
        int numUpdated = 0;
        while (rs.next()) {
            long id = rs.getLong( "id" );
            String oldNamespace = rs.getString( "cat_namespace" );

            if (oldNamespace != null && oldNamespace.contains( "#" )) {
                String newNamespace = oldNamespace.replace( "#", "/" );

                // update
                psUpdate.setString( 1, newNamespace );
                psUpdate.setLong( 2, id );
                psUpdate.executeUpdate();

                numUpdated++;
                log.info( "Updated cat_namespace '" + oldNamespace + "' to '" + newNamespace + "'." );
            }
        }
        rs.close();
        st.close();
        psUpdate.close();

        log.info( "Updated " + numUpdated + " objects." );
        log.info( "Updating t03_catalogue.cat_namespace... done\n" );
    }

    private void migrateT011ObjGeoDatasourceUuid() throws Exception {
        log.info( "\nUpdating t011_obj_geo.datasource_uuid..." );

        log.info( "Replace old \"#\" with new \"/\" separator ..." );

        String psSql = "UPDATE t011_obj_geo SET datasource_uuid = ? WHERE id = ?";
        PreparedStatement psUpdate = jdbc.prepareStatement( psSql );

        String sql = "select id, obj_id, datasource_uuid from t011_obj_geo";

        Statement st = jdbc.createStatement();
        ResultSet rs = jdbc.executeQuery( sql, st );
        int numUpdated = 0;
        while (rs.next()) {
            long id = rs.getLong( "id" );
            long objId = rs.getLong( "obj_id" );
            String oldDatasourceUuid = rs.getString( "datasource_uuid" );

            if (oldDatasourceUuid != null && oldDatasourceUuid.contains( "#" )) {
                String newDatasourceUuid = oldDatasourceUuid.replace( "#", "/" );

                // update
                psUpdate.setString( 1, newDatasourceUuid );
                psUpdate.setLong( 2, id );
                psUpdate.executeUpdate();

                numUpdated++;
                log.info( "Updated datasource_uuid '" + oldDatasourceUuid + "' to '" + newDatasourceUuid + "' in object with id " + objId );
            }
        }
        rs.close();
        st.close();
        psUpdate.close();

        log.info( "Updated " + numUpdated + " objects." );
        log.info( "Updating t011_obj_geo.datasource_uuid... done\n" );
    }
}
