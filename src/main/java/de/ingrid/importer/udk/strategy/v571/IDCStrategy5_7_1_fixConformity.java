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
package de.ingrid.importer.udk.strategy.v571;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * <p>
 * Changes InGrid 5.7.1
 * <p>
 * Fix isInspire state of conformity values (#2337)
 */
public class IDCStrategy5_7_1_fixConformity extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy5_7_1_fixConformity.class );

    private static final String MY_VERSION = VALUE_IDC_VERSION_5_7_1_fixConformity;

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

        // do not write version of IGC structure, since migration can be done multiple times !
        // setGenericKey(KEY_IDC_VERSION, MY_VERSION);

        // THEN PERFORM DATA MANIPULATIONS !
        // ---------------------------------

        System.out.println( "Fix isInspire value for migrated free entries ..." );
        migrateToFreeEntries();
        System.out.println( "done." );

        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private void migrateToFreeEntries() throws SQLException {
        String sql = "SELECT oc.id, oc.specification_value, obj_uuid, obj_name FROM t01_object JOIN object_conformity oc on t01_object.id = oc.obj_id " +
                "WHERE oc.specification_key=-1 AND oc.is_inspire!='N'";
        PreparedStatement psUpdateConformityToFree = jdbc.prepareStatement(
                "UPDATE object_conformity SET is_inspire='N' WHERE id=?");

        PreparedStatement psFreeEntries = jdbc.prepareStatement(sql);

        ResultSet resultSet = psFreeEntries.executeQuery();
        while (resultSet.next()) {
            log.info("Update isInspire value of conformity '" + resultSet.getString("specification_value") + "' (" + resultSet.getLong("id") + ") to 'No' for dataset '" + resultSet.getString("obj_name") + "' (" + resultSet.getString("obj_uuid") + ").");
            psUpdateConformityToFree.setLong(1, resultSet.getLong("id"));
            psUpdateConformityToFree.executeUpdate();
        }

        psUpdateConformityToFree.close();
        psFreeEntries.close();
    }
}
