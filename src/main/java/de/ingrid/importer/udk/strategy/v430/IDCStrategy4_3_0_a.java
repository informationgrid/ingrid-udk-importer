/*-
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
package de.ingrid.importer.udk.strategy.v430;

import de.ingrid.importer.udk.jdbc.DBLogic;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * Changes InGrid 4.3.0
 * <p>
 * <ul>
 * <li>Adds new columns to the conformity table to be able to specify custom
 * conformity specifications, see <a href="https://redmine.informationgrid.eu/issues/859">#859</a> 
 * </ul>
 */
public class IDCStrategy4_3_0_a extends IDCStrategyDefault {

    private static final Log LOG = LogFactory.getLog( IDCStrategy4_3_0_a.class );

    private static final String MY_VERSION = VALUE_IDC_VERSION_4_3_0_a;

    @Override
    public String getIDCVersion() {
        return MY_VERSION;
    }

    @Override
    public void execute() throws Exception {
        jdbc.setAutoCommit( false );

        // write version of IGC structure !
        setGenericKey( KEY_IDC_VERSION, MY_VERSION );

        // THEN EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit
        // (e.g. on MySQL)
        // ---------------------------------

        LOG.info( "  Extend data structure..." );
        updateObjectConformityTable();
        createFreeConformitySyslist();
        LOG.info( "done." );

        jdbc.commit();
        LOG.info( "Update finished successfully." );
    }

    private void updateObjectConformityTable() throws Exception {
        LOG.info( "\nExtending datastructure object_conformity." );

        extendObjectConformityTable();
        insertNewObjectConformityValues();

        LOG.info( "Finished extending datastructure.\n" );
    }

    private void extendObjectConformityTable() throws SQLException {
        jdbc.getDBLogic().addColumn( "is_inspire", DBLogic.ColumnType.VARCHAR1, "object_conformity", false, "'Y'", jdbc );
        jdbc.getDBLogic().addColumn( "publication_date", DBLogic.ColumnType.VARCHAR17, "object_conformity", false, null, jdbc );
    }

    private void insertNewObjectConformityValues() throws SQLException {
        // Set all existing values as INSPIRE conformities
        LOG.info("Setting default value of object_conformity.is_inpire to 'Y'.");
        String sql = "UPDATE object_conformity SET is_inspire = 'Y'";
        jdbc.executeUpdate(sql);
        LOG.info("Finished setting default value of object_conformity.is_inpire.");

        // Copy dates from the sys_list table
        Map<Integer, String> entries = new HashMap<>();
        sql = "SELECT entry_id AS entry_id, data AS the_date " +
                "FROM sys_list WHERE lst_id = 6005 " +
                "GROUP BY entry_id, data";
        try (
                Statement statement = jdbc.createStatement();
                ResultSet rs = jdbc.executeQuery(sql, statement)) {
            while (rs.next()) {
                entries.put(rs.getInt("entry_id"), rs.getString("the_date"));
            }
        }

        LOG.info("Copying values from sys_list.data to object_conformity.publication_date.");
        sql = "SELECT id AS id, specification_key, publication_date FROM object_conformity";
        try (
                Statement statement = jdbc.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                int key = rs.getInt("specification_key");
                if (key < 0) continue;

                String s = entries.get(key).replaceAll("-", "") + "000000000";
                rs.updateString("publication_date", s);
                rs.updateRow();
            }
        }
        LOG.info("Finished copying values from sys_list.data to object_conformity.publication_date.");
    }

    private void createFreeConformitySyslist() throws Exception {
        LOG.info("Adding values to table sys_list for free entries.");

        String sqlMaxId = "SELECT MAX(id) AS maxid FROM sys_list";
        String sqlInsert = "INSERT INTO sys_list " +
                "(id, version, lst_id, entry_id, lang_id, name, description, maintainable, is_default, line, data) VALUES " +
                "(?,  0,       6006,   1,        ?,       ?,    NULL,        1,            'N',        0,    '2018-02-22')";
        try (Statement maxIdStm = jdbc.createStatement();
             PreparedStatement insertStm = jdbc.prepareStatement(sqlInsert);
             ResultSet resultSet = jdbc.executeQuery(sqlMaxId, maxIdStm)) {
            resultSet.next();
            long id = resultSet.getLong("maxid");


            insertCodelistEntry(insertStm, ++id, "de", "Konformität - Freier Eintrag");
            insertCodelistEntry(insertStm, ++id, "en", "Conformity - Free entry");
        }

        LOG.info("Finished adding values to sys_list.");
    }

    private void insertCodelistEntry(
            PreparedStatement stm,
            long id,
            String lang,
            String name) throws Exception {
        int idx = 1;
        stm.setLong(idx++, id);
        stm.setString(idx++, lang);
        stm.setString(idx, name);

        stm.executeUpdate();
        stm.clearParameters();
    }
}
