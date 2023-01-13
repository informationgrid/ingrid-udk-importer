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
package de.ingrid.importer.udk.strategy.v362;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 3.6.2
 * <p>
 * Fix migration of constrains (IDCStrategy3_6_1_b) for LGV_HH see https://redmine.wemove.com/issues/911
 * <ul>
 * <li>- wenn "OpenData" oder "Veröffentlichung gemäß HmbTG" gesetzt
 * <li>- ODER wenn "Anwendungseinschränkungen" (object_use) beginnt mit "Datenlizenz Deutschland - Namensnennung - Version 2.0;"
 * <li>- dann übernehme Inhalt aus "Anwendungseinschränkungen" (object_use) in das neue Feld "Nutzungsbedingungen" (object_use_constraint), wenn noch nicht vorhanden
 * <li>- und lösche Inhalt von "Anwendungseinschränkungen" (object_use)
 * </ul>
 */
public class IDCStrategy3_6_2_fixConstraintsHH extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy3_6_2_fixConstraintsHH.class );

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

        System.out.print( "  Fix migration of object_use to object_use_constraint..." );
        migrateObjectUse();
        System.out.println( "done." );

        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private void migrateObjectUse() throws Exception {
        log.info( "\nUpdating object_use and object_use_constraint..." );

        log.info( "Transfer license from object_use to object_use_constraint ..." );

        // Unchanged object_use entries from syslist 6500 (license) already transferred to object_use_constraint (IDCStrategy3_6_1_b) !
        // But changed entries from syslist were not transferred !
        // We transfer content of object_use to object_use_constraint:
        // - if "open data" or "Veröffentlichung gemäß HmbTG" set
        // - OR if content starts with "Datenlizenz Deutschland - Namensnennung - Version 2.0;"
        // - and content not already in object_use_constraint
        // The result in object_use_constraint is always a free entry with key -1.

        PreparedStatement psSelectIsOpenData = jdbc.prepareStatement( "SELECT is_open_data FROM t01_object WHERE id = ?" );
        
        // HH specific: set to "true" or "false"
        PreparedStatement psSelectIsHmbTG = jdbc.prepareStatement(
                "SELECT data " +
                "FROM additional_field_data " +
                "WHERE obj_id = ? " +
                "AND field_key = 'publicationHmbTG'");

        // select existing object_use_constraint
        PreparedStatement psSelectLicenses = jdbc.prepareStatement(
                "SELECT line, license_key, license_value " +
                "FROM object_use_constraint " +
                "WHERE obj_id = ? " +
                "ORDER BY line");


        PreparedStatement psInsertObjUseConstr = jdbc.prepareStatement( "INSERT INTO object_use_constraint " + "(id, obj_id, line, license_key, license_value) "
                + "VALUES (?,?,?,?,?)" );

        String sql = "select id, obj_id, terms_of_use_key, terms_of_use_value from object_use";

        Statement st = jdbc.createStatement();
        ResultSet rs = jdbc.executeQuery( sql, st );
        int numInsertedTotal = 0;
        int numAlreadyTransferredTotal = 0;
        int numDeletedTotal = 0;
        while (rs.next()) {
            long useId = rs.getLong("id");
            long objId = rs.getLong( "obj_id" );
            int useKey = rs.getInt( "terms_of_use_key" );
            String useValue = rs.getString( "terms_of_use_value" );
            
            if (useValue == null || useValue.trim().length() == 0) {
//                log.info( "Ignore OBJECT [id:" + objId + "]: object_use = '" + useValue + "' !");
                continue;
            }

            // check Open data
            psSelectIsOpenData.setLong( 1, objId );
            ResultSet rs2 = psSelectIsOpenData.executeQuery();
            String isOpenData = "";
            if (rs2.next()) {
                isOpenData = rs2.getString( "is_open_data" );
            }
            rs2.close();

            // check HmbTG
            psSelectIsHmbTG.setLong( 1, objId );
            rs2 = psSelectIsHmbTG.executeQuery();
            String isHmbTG = "";
            if (rs2.next()) {
                isHmbTG = rs2.getString( "data" );
            }
            rs2.close();

            // check whether starting with prefix
            String prefixLicense = "Datenlizenz Deutschland - Namensnennung - Version 2.0;";
            boolean startsWithPrefix = useValue.trim().startsWith( prefixLicense);

            // if NOT OpenData and NOT HmbTG and NOT starting with prefix then skip 
            if (!("Y".equals( isOpenData )) &&
                    !("true".equals( isHmbTG )) &&
                    !startsWithPrefix) {
//                log.info( "Ignore OBJECT [id:" + objId + "]: NOT OpenData and NOT HmbTG and NOT license (object_use = '" + useValue + "')");
                continue;
            }

            // check whether already transferred
            psSelectLicenses.setLong(1, objId);
            rs2 = psSelectLicenses.executeQuery();
            boolean licenseAlreadyTransferred = false;
            int line = 1;
            while (rs2.next()) {
                String licenseValue = rs2.getString("license_value");
                if (licenseValue != null &&
                        removeLineFeedsAndSpaces(licenseValue).equals( removeLineFeedsAndSpaces(useValue) )) {
                    licenseAlreadyTransferred = true;
                    numAlreadyTransferredTotal++;
                    log.info( "OBJECT [id:" + objId + "]: License already transferred '" + useValue + "'");
                    break;
                }
                line++;
            }
            rs2.close();

            // TRANSFER if new license !
            if (!licenseAlreadyTransferred) {
                psInsertObjUseConstr.setLong( 1, getNextId() ); // id
                psInsertObjUseConstr.setLong( 2, objId ); // obj_id
                psInsertObjUseConstr.setInt( 3, line ); // line
                psInsertObjUseConstr.setInt( 4, -1 ); // license_key
                psInsertObjUseConstr.setString( 5, useValue ); // license_value
                int numUpdated = psInsertObjUseConstr.executeUpdate();
                if (numUpdated > 0) {
                    log.info( "OBJECT [id:" + objId + "]: Inserted license to object_use_constraint [key:-1/value:'" + useValue + "'] !" );
                    numInsertedTotal++; 
                } else {
                    log.warn( "OBJECT [id:" + objId + "]: PROBLEMS inserting license to object_use_constraint [key:-1/value:'" + useValue + "'] !" );
                }                
            }

            // delete license from object_use !
            int numDeleted = jdbc.executeUpdate("DELETE FROM object_use WHERE id=" + useId);
            if (numDeleted > 0) {
                log.info( "OBJECT [id:" + objId + "]: Deleted license from object_use [id:" + useId + "/key:" + useKey + "/value:'" + useValue + "'] !" );
                numDeletedTotal++; 
            } else {
                log.warn( "OBJECT [id:" + objId + "]: PROBLEMS deleting license from object_use [id:" + useId + "/key:" + useKey + "/value:'" + useValue + "'] !" );
            }            
        }
        rs.close();
        st.close();
        psSelectIsOpenData.close();
        psSelectIsHmbTG.close();
        psSelectLicenses.close();
        psInsertObjUseConstr.close();

        log.info( "Kept " + numAlreadyTransferredTotal + " licenses already transferred to object_use_constraint." );
        log.info( "Inserted " + numInsertedTotal + " licenses to object_use_constraint." );
        log.info( "Deleted " + numDeletedTotal + " licenses from object_use." );

        log.info( "Updating object_use and object_use_constraint... done\n" );
    }
    
    String removeLineFeedsAndSpaces(String input) {
        String replaced = input.replace("\r", "").replace("\n", "").replace(" ", "");
        return replaced;
    }
}
