/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2021 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v560;

import de.ingrid.importer.udk.jdbc.DBLogic;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * <p>
 * Changes InGrid 5.6.0_d
 * <p>
 * Add new table t011_obj_geo_data_bases #1429
 */
public class IDCStrategy5_6_0_d extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog(IDCStrategy5_6_0_d.class);

    private static final String MY_VERSION = VALUE_IDC_VERSION_5_6_0_d;

    public String getIDCVersion() {
        return MY_VERSION;
    }

    public void execute() throws Exception {
        jdbc.setAutoCommit(false);

        // write version of IGC structure !
        setGenericKey(KEY_IDC_VERSION, MY_VERSION);

        // THEN PERFORM DATA MANIPULATIONS !
        // ---------------------------------

        System.out.print( "  Add new table t011_obj_geo_data_bases  ..." );
        extendDataStructure();
        System.out.println("done.");

        System.out.print( "  Migrate Data  ..." );
        migrateData();
        System.out.println("done.");

        System.out.print( "  Remove old field  ..." );
        removeOldField();
        System.out.println("done.");

        jdbc.commit();
        System.out.println("Update finished successfully.");
    }

    private void extendDataStructure() throws Exception {
        log.info( "\nExtending datastructure -> CAUSES COMMIT ! ..." );

        log.info( "Add database 't011_obj_geo_data_bases'..." );
        jdbc.getDBLogic().createTableT011ObjGeoDataBases(jdbc);

        log.info( "Extending datastructure... done\n" );
    }

    private void migrateData() throws SQLException {
        PreparedStatement psAllRowsWithDimensionData = jdbc.prepareStatement("SELECT * FROM t011_obj_geo WHERE data_base is not null");

        ResultSet rs = psAllRowsWithDimensionData.executeQuery();
        int numTransferred = 0;
        while (rs.next()) {
            long id = rs.getLong("id");
            String data_base = rs.getString("data_base");
            PreparedStatement pstmt = jdbc.prepareStatement("INSERT INTO t011_obj_geo_data_bases (id, obj_geo_id, data_base) VALUES (?, ?, ? )");
            pstmt.setInt(1,numTransferred+1);
            pstmt.setLong(2,id);
            pstmt.setString(3, data_base);
            pstmt.execute();
            numTransferred++;
        }
        log.info("Migrated data bases: " + numTransferred);
    }

    private void removeOldField() throws SQLException {
        jdbc.getDBLogic().dropColumn("data_base", "t011_obj_geo", jdbc);
    }
}
