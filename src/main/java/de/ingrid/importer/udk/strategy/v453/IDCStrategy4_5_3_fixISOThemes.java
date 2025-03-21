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
package de.ingrid.importer.udk.strategy.v453;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 4.5.3
 * <p>
 * Remove ISO Themes from all metadata if not Geo-Dataset (#1099)<br>
 * NOTICE: Writes NO Version to catalog but triggers workflow
 */
public class IDCStrategy4_5_3_fixISOThemes extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy4_5_3_fixISOThemes.class );

    private static final String MY_VERSION = VALUE_IDC_VERSION_4_5_3_fixISOThemes;

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

        System.out.print( "  Remove ISO themes from non Geo-Dataset ..." );
        removeISOThemes();
        System.out.println( "done." );

        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private void removeISOThemes() throws Exception {
        log.info( "Remove ISO themes from non Geo-Dataset ..." );

        PreparedStatement psSelectISOThemes = jdbc.prepareStatement( "SELECT id, obj_id FROM t011_obj_topic_cat" );
        PreparedStatement psSelectObjClass = jdbc.prepareStatement( "SELECT obj_class FROM t01_object WHERE id = ?" );
        PreparedStatement psDeleteISOTheme = jdbc.prepareStatement( "DELETE FROM t011_obj_topic_cat WHERE id = ?" );

        int numDeletedTotal = 0;
        ResultSet resultISOThemes = psSelectISOThemes.executeQuery();
        while (resultISOThemes.next()) {
            Long isoId = resultISOThemes.getLong( "id" );
            Long objId = resultISOThemes.getLong( "obj_id" );
            
            // check object class
            psSelectObjClass.setLong( 1,  objId );
            ResultSet resultObjClass = psSelectObjClass.executeQuery();
            int objClass = -1;
            if (resultObjClass.next()) {
                objClass = resultObjClass.getInt( "obj_class" );
            }
            resultObjClass.close();
            
            if (objClass > -1 && objClass != 1) {
                // No Geo-Dataset, we delete ISO theme
                psDeleteISOTheme.setLong( 1, isoId );
                int numDeleted = psDeleteISOTheme.executeUpdate();
                if (numDeleted > 0) {
                    log.info( "OBJECT [id:" + objId + "]: Deleted ISO theme from t011_obj_topic_cat [id:" + isoId + "] !" );
                    numDeletedTotal++; 
                } else {
                    log.warn( "OBJECT [id:" + objId + "]: PROBLEMS deleting ISO theme from t011_obj_topic_cat [id:" + isoId + "'] !" );
                }            
            }
        }

        resultISOThemes.close();
        psSelectISOThemes.close();
        psSelectObjClass.close();
        psDeleteISOTheme.close();

        log.info( "Deleted " + numDeletedTotal + " ISO themes from t011_obj_topic_cat." );

        log.info( "Remove ISO themes from non Geo-Dataset ... done" );
    }
}
