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
package de.ingrid.importer.udk.strategy.v570;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashMap;

/**
 * <p>
 * Changes InGrid 5.7.0_a
 * <p>
 * <ul>
 * <li>Change english localisation to german in 'administrative_area' syslist 6250 (specific to catalog), see https://redmine.informationgrid.eu/issues/967#note-45</li>
 * </ul>
 */
public class IDCStrategy5_7_0_a extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy5_7_0_a.class );

    private static final String MY_VERSION = VALUE_IDC_VERSION_5_7_0_a;

    public String getIDCVersion() {
        return MY_VERSION;
    }

    public void execute() throws Exception {
        jdbc.setAutoCommit( false );

        // write version of IGC structure !
        setGenericKey( KEY_IDC_VERSION, MY_VERSION );

        try {
            log.info( "Updating syslist '6250' ..." );
            updateSyslist();
        } catch (Exception ex) {
            log.warn("Problems Updating syslist 6250: ", ex);
        }
        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }


    private void updateSyslist() throws Exception {
        // get all german entries
        String getSql = "SELECT entry_id, name FROM sys_list WHERE lst_id=? AND lang_id=?";
        PreparedStatement psGet = jdbc.prepareStatement(getSql);
        psGet.setInt(1, 6250);
        psGet.setString(2, "de");

        String updateSql = "UPDATE sys_list SET name=? WHERE lst_id=? AND lang_id=? AND entry_id=?";
        PreparedStatement psUpdate = jdbc.prepareStatement(updateSql);

        // iterate over all german entries
        ResultSet rsGet = psGet.executeQuery();
        while (rsGet.next()) {
            int entry_id = rsGet.getInt("entry_id");
            String germanName = rsGet.getString("name");

            // update english entry to match german entry
            psUpdate.setString(1, germanName);
            psUpdate.setInt(2, 6250);
            psUpdate.setString(3, "en");
            psUpdate.setInt(4, entry_id);
            psUpdate.executeUpdate();
        }
        psUpdate.close();
    }
}
