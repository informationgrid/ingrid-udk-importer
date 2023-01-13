/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2023 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v470;

import java.sql.PreparedStatement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 4.7.0_a
 * <p>
 * Remove Codelist 6020 (Redmine #564), see https://redmine.informationgrid.eu/issues/564<br>
 * NOTICE: Writes NO Version to catalog but triggers workflow
 */
public class IDCStrategy4_7_0_a extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy4_7_0_a.class );

    private static final String MY_VERSION = VALUE_IDC_VERSION_4_7_0_a;

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

        System.out.print( "  Remove Codelist 6020 ..." );
        removeCodelist6020();
        System.out.println( "done." );

        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private void removeCodelist6020() throws Exception {
        log.info( "Remove Codelist 6020 ..." );

        PreparedStatement psDeleteCodelist6020 = jdbc.prepareStatement( "DELETE FROM sys_list WHERE lst_id = 6020 " );

        int numDeleted = psDeleteCodelist6020.executeUpdate();

        psDeleteCodelist6020.close();

        log.info( "Deleted Codelist 6020 with " + numDeleted + " entries from sys_list." );

        log.info( "Remove Codelist 6020 ... done" );
    }
}
