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
package de.ingrid.importer.udk.strategy.v560;

import de.ingrid.importer.udk.jdbc.DBLogic;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.utils.ige.profile.MdekProfileUtils;
import de.ingrid.utils.ige.profile.ProfileMapper;
import de.ingrid.utils.ige.profile.beans.ProfileBean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 */
public class IDCStrategy5_6_0_c extends IDCStrategyDefault {

    private static final Log log = LogFactory.getLog(IDCStrategy5_6_0_c.class);

    private static final String MY_VERSION = VALUE_IDC_VERSION_5_6_0_c;

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

        System.out.println("  Migrating vector_topology_level to table t011_obj_geo_vector ...");
        migrateData();
        System.out.println("done.");

        System.out.println("  Removing old field vector_topology_level from t011_obj_geo ...");
        removeOldFields();
        System.out.println("done.");

        System.out.println("  Removing old field uiElement5063 from profile ...");
        removeFromProfile();
        System.out.println("done.");



        jdbc.commit();
        System.out.println("Update finished successfully.");
    }

    private void removeFromProfile() throws Exception {
        // read profile
        String profileXml = readGenericKey(KEY_PROFILE_XML);
        if (profileXml == null) {
            throw new Exception("igcProfile not set !");
        }
        ProfileMapper profileMapper = new ProfileMapper();
        ProfileBean profileBean = profileMapper.mapStringToBean(profileXml);

        MdekProfileUtils.removeControl(profileBean, "uiElement5063");

        // write Profile !
        profileXml = profileMapper.mapBeanToXmlString(profileBean);
        setGenericKey(KEY_PROFILE_XML, profileXml);
    }

    private void extendDataStructure() throws Exception {
        log.info("Add new column 'vector_topology_level' to t011_obj_geo_vector -> CAUSES COMMIT ! ...");

        jdbc.getDBLogic().addColumn("vector_topology_level", DBLogic.ColumnType.INTEGER, "t011_obj_geo_vector", false, null, jdbc);
    }

    private void migrateData() throws SQLException {
        PreparedStatement psAllRowsWithTopologyLevel = jdbc.prepareStatement("SELECT * FROM t011_obj_geo WHERE vector_topology_level is not null");

        ResultSet rs = psAllRowsWithTopologyLevel.executeQuery();
        int numTransferred = 0;
        while (rs.next()) {
            long id = rs.getLong("id");
            String level = rs.getString("vector_topology_level");

            jdbc.executeUpdate(String.format("UPDATE t011_obj_geo_vector SET vector_topology_level=%s WHERE obj_geo_id=%d", level, id));

            numTransferred++;
        }
        log.info("Migrated vector topology levels: " + numTransferred);
    }

    private void removeOldFields() throws SQLException {
        jdbc.getDBLogic().dropColumn("vector_topology_level", "t011_obj_geo", jdbc);
    }

}
