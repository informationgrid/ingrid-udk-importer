/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2018 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v440;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * <p>
 * Changes InGrid 4.4.0
 * <p>
 * <ul>
 * <li>migrate UVP checkbox, see
 * https://redmine.informationgrid.eu/issues/881
 * </ul>
 */
public class IDCStrategy4_4_0_b extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy4_4_0_b.class );

    private static final String MY_VERSION = VALUE_IDC_VERSION_4_4_0_b;

    public String getIDCVersion() {
        return MY_VERSION;
    }

    public void execute() throws Exception {
        jdbc.setAutoCommit( false );

        // write version of IGC structure !
        setGenericKey( KEY_IDC_VERSION, MY_VERSION );

        // THEN EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit
        // (e.g. on MySQL)
        // ---------------------------------

        System.out.print( "  Migrate UVP data ..." );
        migrateUVP();
        System.out.println( "done." );
        
        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private void migrateUVP() throws Exception {
        PreparedStatement psNeedsExamination = jdbc.prepareStatement(
                "SELECT data " +
                        "FROM additional_field_data " +
                        "WHERE obj_id = ? " +
                        "AND field_key = 'uvpNeedsExamination'");

        String sql = "select id from t01_object";

        Statement st = jdbc.createStatement();
        ResultSet rs = jdbc.executeQuery(sql, st);
        while (rs.next()) {
            long objId = rs.getLong("id");

            psNeedsExamination.setLong(1, objId);
            ResultSet resultSet = psNeedsExamination.executeQuery();
            String valueNeedsExamination;
            if (resultSet.next()) {
                valueNeedsExamination = resultSet.getString("data");

                if ("true".equals(valueNeedsExamination)) {
                    log.debug("migrate UVP checkbox");
                    sql = "INSERT INTO additional_field_data (id, obj_id, field_key, list_item_id, data) "
                            + "VALUES (" + getNextId() + ", " + objId + ", 'uvpPreExaminationAccomplished', NULL, 'true')";
                    jdbc.executeUpdate(sql);
                }
            }
            resultSet.close();
        }
    }
}
