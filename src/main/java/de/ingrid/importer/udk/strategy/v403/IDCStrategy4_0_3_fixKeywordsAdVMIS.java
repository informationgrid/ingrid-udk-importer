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
package de.ingrid.importer.udk.strategy.v403;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 4.0.3
 * <p>
 * Migrate keyword _ADVMIS_ to selected checkbox "AdV compatible" (#369)
 * 
 * </ul>
 */
public class IDCStrategy4_0_3_fixKeywordsAdVMIS extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy4_0_3_fixKeywordsAdVMIS.class );

    /**
     * Deliver NO Version, this strategy should NOT trigger a strategy workflow (of missing former versions) and can be executed on its own
     * ! NOTICE: BUT may be executed in workflow (part of workflow array) !
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

        System.out.print( "  Migrate keywords _ADVMIS_ to checked AdV compatible state ..." );
        migrateKeywords();
        System.out.println( "done." );

        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private void migrateKeywords() throws Exception {
        log.info( "Migrate keywords _ADVMIS_ to checked AdV compatible state ..." );

        PreparedStatement psSelectAdVMISKeywords = jdbc.prepareStatement( "SELECT id FROM searchterm_value WHERE term = '_ADVMIS_'" );
        PreparedStatement psDeleteAdVMISKeyword = jdbc.prepareStatement( "DELETE FROM searchterm_value WHERE id = ?" );
        PreparedStatement psCheckAdvCompatible = jdbc.prepareStatement( "UPDATE t01_object SET is_adv_compatible = 'Y' WHERE id = (SELECT obj_id FROM searchterm_obj WHERE searchterm_id = ?)" );
        PreparedStatement psDeleteAdVMISKeywordConnection = jdbc.prepareStatement( "DELETE FROM searchterm_obj WHERE searchterm_id = ?" );
        
        List<Long> ids = new ArrayList<Long>();

        ResultSet resultADVMISKeywords = psSelectAdVMISKeywords.executeQuery();
        while (resultADVMISKeywords.next()) {
            Long id = resultADVMISKeywords.getLong( "id" );
            ids.add( id );
            psDeleteAdVMISKeyword.setLong( 1, id );
            if (psDeleteAdVMISKeyword.executeUpdate() == 0) {
                log.error( "Could not delete row with _ADVMIS_ keyword with id: " + id );
            }
        }

        // add for all filtered IDs the checkbox AdV compatible
        int allUpdates = 0;
        for (Long id : ids) {
            psCheckAdvCompatible.setLong( 1,  id );
            int updates = psCheckAdvCompatible.executeUpdate();
            allUpdates += updates;
            if (updates == 0) {
                log.warn( "No object could be found to check AdV compatible" );
            }
            psDeleteAdVMISKeywordConnection.setLong( 1,  id );
            if (psDeleteAdVMISKeywordConnection.executeUpdate() == 0) {
                log.warn( "Could not delete keyword<->object connection for searchterm id: " + id );
            }
        }

        resultADVMISKeywords.close();
        psSelectAdVMISKeywords.close();
        psDeleteAdVMISKeyword.close();
        psCheckAdvCompatible.close();
        psDeleteAdVMISKeywordConnection.close();
       
        log.info( "Migrate keywords _ADVMIS_ to checked AdV compatible state ... done" );
        log.info( "Updated number of objects: " + allUpdates );
    }

}
