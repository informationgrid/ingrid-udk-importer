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
package de.ingrid.importer.udk.strategy.v465;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 4.6.5
 * <p>
 * Fix MetaVer licenses: Transfer "Quellenvermerk" to new field in database and
 * migrate license to default license from service (#1166)<br>
 */
public class IDCStrategy4_6_5_fixMetaVerLicenses extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy4_6_5_fixMetaVerLicenses.class );

    private static final int LICENSE_SYSLIST_ID = 6500;

    /**
     * Deliver NO Version, this strategy should NOT trigger a strategy workflow
     * (of missing former versions) and can be executed on its own ! NOTICE: BUT
     * may be executed in workflow (part of workflow array) !
     * 
     * @see de.ingrid.importer.udk.strategy.IDCStrategy#getIDCVersion()
     */
    public String getIDCVersion() {
        // Returning null disables strategy workflow just executing this
        // strategy
        return null;
    }

    public void execute() throws Exception {
        jdbc.setAutoCommit( false );

        // NOTICE:
        // This is a "fix strategy" writing no version !
        // setGenericKey(KEY_IDC_VERSION, MY_VERSION);

        // THEN PERFORM DATA MANIPULATIONS !
        // ---------------------------------

        System.out.print( "  Check whether MetaVer catalog ..." );
        boolean isMetaVer = isMetaVer();
        System.out.println( "done. isMetaVer: " + isMetaVer );

        if (isMetaVer) {
            System.out.print( "  Migrate MetaVer licenses ..." );
            migrateLicenses();
            System.out.println( "done." );

            System.out.print( "  Updating license codelist " + LICENSE_SYSLIST_ID + " ..." );
            updateLicenseCodelist();
            System.out.println( "done." );
        }

        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private boolean isMetaVer() throws Exception {
        log.info( "\nCheck MetaVer" );

        boolean isMetaVer = false;

        int maxLicenseId = getMaxLicenseIdFromCodeLists();
        if (maxLicenseId == 8) {
            isMetaVer = true;
        }

        log.info( "isMetaVer: " + isMetaVer + "\n" );

        return isMetaVer;
    }

    /** Return max entry_id of licenses in license codelist */
    private int getMaxLicenseIdFromCodeLists() throws Exception {
        log.info( "Read max license id from codelist " + LICENSE_SYSLIST_ID );

        int maxId = 0;

        // NOTICE: No DISTINCT, does not work with Oracle (CLOB)
        PreparedStatement psSelectLicenses = jdbc.prepareStatement( "SELECT MAX(entry_id) FROM sys_list WHERE lst_id = " + LICENSE_SYSLIST_ID );

        ResultSet resultLicenses = psSelectLicenses.executeQuery();
        while (resultLicenses.next()) {
            maxId = resultLicenses.getInt( 1 );
        }

        resultLicenses.close();
        psSelectLicenses.close();

        log.info( "maxId: " + maxId );

        return maxId;
    }

    private void migrateLicenses() throws Exception {
        log.info( "\nMigrate Licenses and 'Quellenvermerk' ..." );

        // NOTICE: Do NOT select column source, is CLOB on Oracle so DISTINCT
        // does not work !
        PreparedStatement psSelectLicenses = jdbc.prepareStatement( "SELECT DISTINCT id, license_key, license_value FROM object_use_constraint" );
        PreparedStatement psUpdateLicense = jdbc.prepareStatement( "UPDATE object_use_constraint SET license_key = ?, license_value = ?, source = ? WHERE id = ?" );

        int totalNumUpdated = 0;
        ResultSet resultLicenses = psSelectLicenses.executeQuery();
        while (resultLicenses.next()) {
            long id = resultLicenses.getLong( "id" );
            int licenseKey = resultLicenses.getInt( "license_key" );
            String licenseValue = resultLicenses.getString( "license_value" );

            int newLicenseKey = -1;
            String newLicenseValue = null;
            String newSource = null;

            // convert license
            if (licenseKey == 1 || licenseKey == 6 || licenseKey == 7 || licenseKey == 8) {
                newLicenseKey = 1;
                newLicenseValue = "Datenlizenz Deutschland - Namensnennung – Version 2.0";
            } else if (licenseKey == 2) {
                newLicenseKey = -1;
                newLicenseValue = "Keine";
            } else if (licenseKey == 3) {
                newLicenseKey = 25;
                newLicenseValue = "Datenlizenz Deutschland – Zero – Version 2.0";
            } else if (licenseKey == 4) {
                newLicenseKey = 4;
                newLicenseValue = "Creative Commons Namensnennung (CC-BY)";
            } else if (licenseKey == -1) {
                if (licenseValue != null) {
                    if (licenseValue.contains( "dl-de-by-2.0" )) {
                        newLicenseKey = 1;
                        newLicenseValue = "Datenlizenz Deutschland - Namensnennung – Version 2.0";
                    } else if (licenseValue.contains( "dl-de-zero-2.0" )) {
                        newLicenseKey = 25;
                        newLicenseValue = "Datenlizenz Deutschland – Zero – Version 2.0";
                    } else if (licenseValue.contains( "CC BY" ) || licenseValue.contains( "CC-BY" )) {
                        newLicenseKey = 4;
                        newLicenseValue = "Creative Commons Namensnennung (CC-BY)";
                    } else {
                        newLicenseKey = -1;
                        newLicenseValue = licenseValue;
                    }
                }
            }

            // determine "Quellenvermerk" from "Namensnennung:"
            newSource = getSourceFromLicenseText( licenseValue );

            psUpdateLicense.setInt( 1, newLicenseKey );
            psUpdateLicense.setString( 2, newLicenseValue );
            psUpdateLicense.setString( 3, newSource );
            psUpdateLicense.setLong( 4, id );

            int numUpdated = psUpdateLicense.executeUpdate();
            totalNumUpdated += numUpdated;
            if (numUpdated == 0) {
                log.warn( "LICENSE [id:" + id + "]: PROBLEMS UPDATING from [" + licenseKey + ", '" + licenseValue + "']\n    TO [" + newLicenseKey + ", '" + newLicenseValue
                        + "',\n        '" + newSource + "']" );
            } else {
                log.info( "LICENSE [id:" + id + "]: Updated from [" + licenseKey + ", '" + licenseValue + "']\n    TO [" + newLicenseKey + ", '" + newLicenseValue + "',\n        '"
                        + newSource + "']" );
            }
        }

        resultLicenses.close();
        psSelectLicenses.close();
        psUpdateLicense.close();

        log.info( "Updated " + totalNumUpdated + " licenses from object_use_constraint." );

        log.info( "Migrate Licenses and 'Quellenvermerk' ... done\n" );
    }

    private String getSourceFromLicenseText(String licenseText) {
        String retSource = null;

        final String SOURCE_LABEL = "Namensnennung:";

        if (licenseText != null) {
            int index = licenseText.lastIndexOf( SOURCE_LABEL );
            if (index != -1) {
                retSource = licenseText.substring( index + SOURCE_LABEL.length() ).trim();
                if (retSource.startsWith( "\"" )) {
                    // check whether we have an opening quote with a closing
                    // quote at end, then remove quotes
                    if (retSource.indexOf( "\"", 1 ) == retSource.length() - 1) {
                        retSource = retSource.substring( 1 );
                        retSource = retSource.substring( 0, retSource.length() - 1 );
                    }
                } else if (retSource.startsWith( "'" )) {
                    // check whether we have an opening quote with a closing
                    // quote at end, then remove opening quote
                    if (retSource.indexOf( "'", 1 ) == retSource.length() - 1) {
                        retSource = retSource.substring( 1 );
                        retSource = retSource.substring( 0, retSource.length() - 1 );
                    }
                }
            }
        }

        return retSource;
    }

    /** Update codelist to default one from codelist service ! */
    private void updateLicenseCodelist() throws Exception {
        log.info( "\nUpdate license codelist to default list from codelist-service (" + LICENSE_SYSLIST_ID + ")..." );

        log.info( "Delete former license codelist" );
        PreparedStatement psDeleteLicenseCodelist = jdbc.prepareStatement( "DELETE FROM sys_list WHERE lst_id = ?" );
        psDeleteLicenseCodelist.setInt( 1, LICENSE_SYSLIST_ID );
        int numDeleted = psDeleteLicenseCodelist.executeUpdate();
        if (numDeleted > 0) {
            log.info( "Deleted " + numDeleted + " entries from license codelist." );
        } else {
            log.warn( "PROBLEMS deleting license codelist ! No entries deleted !???" );
        }

        log.info( "Add new license codelist from codelist service." );
        addSysListsFromInitial( new int[] { LICENSE_SYSLIST_ID } );

        log.info( "Update license codelist to default list from codelist-service...done\n" );
    }
}
