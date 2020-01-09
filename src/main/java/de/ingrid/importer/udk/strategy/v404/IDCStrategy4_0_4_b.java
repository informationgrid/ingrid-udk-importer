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
/**
 * 
 */
package de.ingrid.importer.udk.strategy.v404;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 4.0.4
 * <p>
 * <ul>
 * <li>Update syslist 6010 (accessConstraints), will be moved to codelist-repo.
 *      Now NOT maintainable and entries change, see https://dev.informationgrid.eu/redmine/issues/563
 * </ul>
 */
public class IDCStrategy4_0_4_b extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy4_0_4_b.class );

    private static final String MY_VERSION = VALUE_IDC_VERSION_4_0_4_b;

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

        // THEN PERFORM DATA MANIPULATIONS !
        // ---------------------------------

        System.out.print( "  Updating syslist..." );
        updateSysList();
        System.out.println( "done." );

        System.out.print( "  Migrating data..." );
        migrateData();
        System.out.println( "done." );

        // FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause
        // commit (e.g. on MySQL)
        // ---------------------------------

        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private void updateSysList() throws Exception {
        log.info("\nUpdate syslist 6010...");

        // We do not delete syslist 6010 and wait for sync with repo.
        // Instead we update existing syslist in catalog according to the new syslist in the repo, so time delays of sync do not matter !

        int numUpdated = jdbc.executeUpdate("UPDATE sys_list SET maintainable = 0 WHERE lst_id = 6010");
        log.debug("Set " + numUpdated + " entries to NOT maintainable = 0 (all languages).");

        numUpdated = jdbc.executeUpdate("UPDATE sys_list SET name = 'Es gelten keine Zugriffsbeschränkungen' WHERE lst_id = 6010 and name = 'Es gelten keine Bedingungen'");
        numUpdated += jdbc.executeUpdate("UPDATE sys_list SET name = 'no limitations to public access' WHERE lst_id = 6010 and name = 'no conditions apply'");
        log.debug("Modified " + numUpdated + " entry(ies).");

        // delete "Bedingungen unbekannt"
        numUpdated = jdbc.executeUpdate("DELETE FROM sys_list where lst_id = 6010 and entry_id = 10");
        log.debug("Deleted " + numUpdated + " entries (all languages).");

        log.info("\nUpdate syslist 6010...done\n" );
    }

    private void migrateData() throws Exception {
        log.info("\nMigrate object_access to updated syslist 6010...");

        int numUpdated = jdbc.executeUpdate("UPDATE object_access SET restriction_key = -1 WHERE restriction_key = 10");
        log.debug("Set  " + numUpdated + " entry(ies) to free entries cause syslist entry 10 removed");

        log.info("\nMigrate object_access to updated syslist 6010...done\n" );
    }
}
