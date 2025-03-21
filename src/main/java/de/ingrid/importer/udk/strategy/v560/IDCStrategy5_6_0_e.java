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
package de.ingrid.importer.udk.strategy.v560;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * <p>
 * Changes InGrid 5.6.0_e
 * <p>
 * Migrate time_type from 'am' to 'von' #1215
 */
public class IDCStrategy5_6_0_e extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog(IDCStrategy5_6_0_e.class);

    private static final String MY_VERSION = VALUE_IDC_VERSION_5_6_0_e;

    public String getIDCVersion() {
        return MY_VERSION;
    }

    public void execute() throws Exception {
        jdbc.setAutoCommit(false);

        // write version of IGC structure !
        setGenericKey(KEY_IDC_VERSION, MY_VERSION);

        // THEN PERFORM DATA MANIPULATIONS !
        // ---------------------------------

        System.out.print( "  Migrate Data  ..." );
        migrateData();
        System.out.println("done.");

        jdbc.commit();
        System.out.println("Update finished successfully.");
    }

    private void migrateData() throws SQLException {
        PreparedStatement psAllRowsWithAtDateType = jdbc.prepareStatement("SELECT * FROM t01_object WHERE time_type='am'");

        ResultSet rs = psAllRowsWithAtDateType.executeQuery();
        int numTransferred = 0;
        while (rs.next()) {
            PreparedStatement pstmt = jdbc.prepareStatement("UPDATE t01_object SET time_type='von', time_to=? WHERE id=?");
            pstmt.setString(1, rs.getString("time_from"));
            pstmt.setLong(2, rs.getLong("id"));
            pstmt.execute();
            numTransferred++;
        }
        log.info("Migrated datasets: " + numTransferred);
    }

}
