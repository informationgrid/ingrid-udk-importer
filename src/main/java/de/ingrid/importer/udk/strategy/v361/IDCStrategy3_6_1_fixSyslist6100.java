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
package de.ingrid.importer.udk.strategy.v361;

import java.sql.PreparedStatement;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.codelists.model.CodeList;
import de.ingrid.codelists.model.CodeListEntry;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.importer.udk.util.InitialCodeListServiceFactory;
import de.ingrid.utils.udk.UtilsLanguageCodelist;

/**
 * <p>
 * Changes InGrid 3.6.1
 * <p>
 * <ul>
 * <li>Migrate searchterm_value data due to update of syslist 6100 (INSPIRE
 * Themen), see https://dev.informationgrid.eu/redmine/issues/13 3.)
 * </ul>
 * Writes NO Catalog Schema Version to catalog and can be executed on its own !
 */
public class IDCStrategy3_6_1_fixSyslist6100 extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy3_6_1_fixSyslist6100.class );

    /**
     * Deliver NO Version, this strategy should NOT trigger a strategy workflow
     * (of missing former versions) and can be executed on its own ! NOTICE: BUT
     * may be executed in workflow (part of workflow array) !
     * 
     * @see de.ingrid.importer.udk.strategy.IDCStrategy#getIDCVersion()
     */
    public String getIDCVersion() {
        return null;
    }

    public void execute() throws Exception {
        jdbc.setAutoCommit( false );

        // THEN PERFORM DATA MANIPULATIONS !
        // ---------------------------------

        // No update or adding of syslist (addSysListsFromInitial) in database !
        // Syslists should be updated from portal after installation and restart
        // (generic key "lastModifiedSyslist" is removed in release strategy) !

        // but migrate data
        System.out.print( "  Migrating  searchterm_value.entry_id / term..." );
        migrateSearchtermValueINSPIRE();
        System.out.println( "done." );

        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private void migrateSearchtermValueINSPIRE() throws Exception {
        log.info( "\nUpdating searchterm_value.entry_id/term of INSPIRE themes..." );

        PreparedStatement psUpdate = jdbc.prepareStatement( "UPDATE searchterm_value SET entry_id=?, term=? WHERE type='I' AND entry_id=?" );

        // read newest codelist from codelist service
        CodeList newSyslist = InitialCodeListServiceFactory.instance().getCodeList( Integer.toString( 6100 ) );
        List<CodeListEntry> listEntries = newSyslist.getEntries();

        // update term in the language of the catalogue
        String catalogLang = UtilsLanguageCodelist.getShortcutFromCode( readCatalogLanguageKey() );
        if (catalogLang == null) {
            catalogLang = "de";
        }

        int totalNumUpdated = 0;
        for (CodeListEntry entry : listEntries) {
            Long entryId = Long.decode( entry.getId() );
            String entryValue = entry.getLocalisedEntry( catalogLang );

            // First generic update: All searchterm_values with given ID are
            // updated with new term !
            psUpdate.setLong( 1, entryId );
            psUpdate.setString( 2, entryValue );
            psUpdate.setLong( 3, entryId );
            int numUpdated = psUpdate.executeUpdate();
            if (numUpdated > 0) {
                totalNumUpdated += numUpdated;
                log.info( "Updated " + numUpdated + " INSPIRE searchterm_value(s) to " + entryId + "/" + entryValue );
            }

            // Then update removed entries ! 314 was removed and is now 313 !
            if (entryId == 313) {
                psUpdate.setLong( 1, entryId );
                psUpdate.setString( 2, entryValue );
                psUpdate.setLong( 3, 314 );
                numUpdated = psUpdate.executeUpdate();
                if (numUpdated > 0) {
                    totalNumUpdated += numUpdated;
                    log.info( "Replaced " + numUpdated + " INSPIRE searchterm_value(s) from ID 314 to " + entryId + "/" + entryValue );
                }
            }
        }
        psUpdate.close();

        log.info( "Updated " + totalNumUpdated + " searchterm_value(s)." );
        log.info( "\nUpdating searchterm_value.entry_id/term of INSPIRE themes... done\n" );
    }
}
