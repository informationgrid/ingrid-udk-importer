/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2021 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v521;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * <p>
 * Changes InGrid 5.2.1_c
 * <p>
 * Add column spatial_scope
 */
public class IDCStrategy5_2_1_c extends IDCStrategyDefault {

    private static final String REGION_ID = "885989663";
    private static Log log = LogFactory.getLog(IDCStrategy5_2_1_c.class);

    private static final String MY_VERSION = VALUE_IDC_VERSION_5_2_1_c;

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
        jdbc.setAutoCommit(false);

        // NOTICE:
        // This is a "fix strategy" writing no version !

        // do write version of IGC structure, since migration shall only be run once!
        setGenericKey(KEY_IDC_VERSION, MY_VERSION);

        // THEN PERFORM DATA MANIPULATIONS !
        // ---------------------------------

        System.out.print( "  Add column spatial_scope and migrate data ..." );
        extendDataStructure();
        migrateData();

        System.out.println("done.");

        jdbc.commit();
        System.out.println("Update finished successfully.");
    }

    private void extendDataStructure() throws Exception {
        log.info( "\nExtending datastructure -> CAUSES COMMIT ! ..." );

        log.info( "Add column 'spatial_scope'..." );
        jdbc.getDBLogic().addColumn("spatial_scope", ColumnType.INTEGER, "t01_object", false, null, jdbc);

        log.info( "Extending datastructure... done\n" );
    }

    private void migrateData() throws SQLException {
        PreparedStatement psClasses1AndInspire = jdbc
                .prepareStatement("SELECT * FROM t01_object WHERE obj_class='1' AND is_inspire_relevant='Y'");

        PreparedStatement psUpdate = jdbc
                .prepareStatement("UPDATE t01_object SET spatial_scope='" + REGION_ID + "' WHERE id=?");

        ResultSet inspireResult = psClasses1AndInspire.executeQuery();
        while (inspireResult.next()) {
            long id = inspireResult.getLong("id");
            psUpdate.setLong(1, id);
            psUpdate.executeUpdate();
        }

        psClasses1AndInspire.close();
        psUpdate.close();
    }
}
