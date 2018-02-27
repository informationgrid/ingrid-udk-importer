/*-
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
package de.ingrid.importer.udk.strategy.v430;

import de.ingrid.importer.udk.jdbc.DBLogic;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;

import static java.lang.Long.getLong;

/**
 * Changes InGrid 4.3.0
 *
 * Adds new columns to the conformity table to be able to specify custom
 * conformity specifications.
 *
 * @see <a href="https://redmine.informationgrid.eu/issues/859" />
 *
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

        System.out.print( "  Extend data structure..." );
        extendDataStructure();
        createFreeConformitySyslist();
        System.out.println( "done." );

        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private void extendDataStructure() throws Exception {
        LOG.info( "\nExtending datastructure object_conformity." );

        jdbc.getDBLogic().addColumn( "is_inspire", DBLogic.ColumnType.VARCHAR1, "object_conformity", false, "'Y'", jdbc );
        jdbc.getDBLogic().addColumn( "publication_date", DBLogic.ColumnType.VARCHAR17, "object_conformity", false, null, jdbc ); // TODO default value?

        String sql = "UPDATE object_conformity SET is_inspire = 'Y'";
        jdbc.executeUpdate(sql);

        LOG.info( "Finished extending datastructure.\n" );
    }

    private void createFreeConformitySyslist() throws Exception {
        LOG.info("Adding values to table sys_list for free entries.");

        String sql = "SELECT MAX(id) AS maxid FROM sys_list";
        Statement maxIdStm = jdbc.createStatement();
        ResultSet resultSet = jdbc.executeQuery(sql, maxIdStm);
        resultSet.next();
        long id = resultSet.getLong("maxid");

        PreparedStatement insertStm = jdbc.prepareStatement("INSERT INTO sys_list " +
                "(id, version, lst_id, entry_id, lang_id, name, description, maintainable, is_default, line, data) VALUES " +
                "(?,  0,       6006,   1,        ?,       ?,    NULL,        1,            'N',        0,    '2018-02-22')");

        insertCodelistEntry(insertStm, ++id,"de", "Konformität - Freier Eintrag");
        insertCodelistEntry(insertStm, ++id,"en","Conformity - Free entry");

        maxIdStm.close();
        insertStm.close();

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
        stm.setString(idx++, name);

        stm.executeUpdate();
        stm.clearParameters();
    }
}
