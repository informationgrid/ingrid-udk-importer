/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2019 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v510;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * <p>
 * Changes InGrid 5.1.0_a
 * <p>
 * Migrate licenses with value "keine" to new entry "Es gelten keine Bedingungen" (Redmine #1218), see https://redmine.informationgrid.eu/issues/1218<br>
 */
public class IDCStrategy5_1_0_a extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy5_1_0_a.class );

    private static final String MY_VERSION = VALUE_IDC_VERSION_5_1_0_a;

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

        System.out.print( "Migrate license data 'keine' ..." );
        migrateLicenseData();
        System.out.println( "done." );

        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private void migrateLicenseData() throws Exception {

        PreparedStatement psLicensesToMigrate = jdbc.prepareStatement( "SELECT * FROM object_use_constraint WHERE license_value='keine' OR license_value='Keine'" );
        PreparedStatement psUpdateLicense = jdbc.prepareStatement( "UPDATE object_use_constraint SET license_key=26, license_value='Es gelten keine Bedingungen' WHERE id=?" );

        ResultSet resultSet = psLicensesToMigrate.executeQuery();
        while (resultSet.next()) {
            long id = resultSet.getLong("id");
            psUpdateLicense.setLong(1, id);
            psUpdateLicense.executeUpdate();
        }

        resultSet.close();
        psUpdateLicense.close();

    }
}