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

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Create new table t011_obj_geo_axisdim and migrate data
 */
public class IDCStrategy5_6_0_b extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog(IDCStrategy5_6_0_b.class);

    private static final String MY_VERSION = VALUE_IDC_VERSION_5_6_0_b;

    public String getIDCVersion() {
        return MY_VERSION;
    }

    public void execute() throws Exception {
        jdbc.setAutoCommit(false);

        // write version of IGC structure !
        setGenericKey(KEY_IDC_VERSION, MY_VERSION);

        // delete time stamp of last update of syslists to reload all syslists
        // (reload from initial codelist file from codelist service if no repo connected).
        // Thus we guarantee syslists are up to date !
        deleteGenericKey("lastModifiedSyslist");

        System.out.println("  Extend datastructure...");
         extendDataStructure();
        System.out.println("done.");

        System.out.println("  Migrating ref1AxisDimName and ref1AxisDimSize to new table ...");
        migrateData();
        System.out.println("done.");

        System.out.println("  Removing old fields ref1AxisDimName and ref1AxisDimSize ...");
        removeOldFields();
        System.out.println("done.");


        jdbc.commit();
        System.out.println("Update finished successfully.");
    }

    private void extendDataStructure() throws Exception {
        log.info("Add table t011_obj_geo_axisdim -> CAUSES COMMIT ! ...");

        jdbc.getDBLogic().createTableT011ObjGeoAxisDim(jdbc);
    }

    private void migrateData() throws SQLException {
        PreparedStatement psAllRowsWithDimensionData = jdbc.prepareStatement("SELECT * FROM t011_obj_geo WHERE axis_dim_name is not null OR axis_dim_size is not null");

        ResultSet rs = psAllRowsWithDimensionData.executeQuery();
        int numTransferred = 0;
        while (rs.next()) {
            long id = rs.getLong("id");
            String name = rs.getString("axis_dim_name");
            String size = rs.getString("axis_dim_size");

            jdbc.executeUpdate(String.format("INSERT INTO t011_obj_geo_axisdim (id, obj_geo_id, name, count) VALUES (%d, %d, %s, %s )", numTransferred + 1, id, name, size));
            numTransferred++;
        }
        log.info("Migrated axis dimensions: " + numTransferred);
    }

    private void removeOldFields() throws SQLException {
        jdbc.getDBLogic().dropColumn("axis_dim_name", "t011_obj_geo", jdbc);
        jdbc.getDBLogic().dropColumn("axis_dim_size", "t011_obj_geo", jdbc);
    }

}
