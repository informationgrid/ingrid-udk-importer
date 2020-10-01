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
package de.ingrid.importer.udk.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class implements the database logic for Oracle
 *
 * @author joachim@wemove.com
 */
public class OracleLogic implements DBLogic {

    /**
     * The logging object
     */
    private static Log log = LogFactory.getLog( OracleLogic.class );

    public void setSchema(Connection connection, String schema) throws Exception {
        if (connection == null) {
            throw new IllegalArgumentException( "Connection parameter can't be null" );
        }
        if (schema == null) {
            throw new IllegalArgumentException( "Schema parameter can't be null" );
        }

        if (schema.trim().equals( "" )) {
            if (log.isDebugEnabled()) {
                log.debug( "Using the default schema/tablespace for the current user "
                        + (connection.getMetaData() != null ? connection.getMetaData().getUserName() : "") );
            }
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug( "Setting schema to " + schema );
        }

        Statement statement = connection.createStatement();
        String changeSchema = "alter session set current_schema = " + schema;
        statement.executeUpdate( changeSchema );
        statement.close();
    }

    public void addColumn(String colName, ColumnType colType, String tableName, boolean notNull, Object defaultValue,
            JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "ALTER TABLE " + tableName + " ADD " + colName + " " + mapColumnTypeToSQL( colType );

        if (notNull) {
            sql += " NOT NULL";
            // NOTICE: adding default value causes ERROR (at least when type
            // string) ! is added by jdbc automatically !
        }
        if (defaultValue != null) {
            sql += " DEFAULT " + defaultValue;
        }

        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    public void renameColumn(String oldColName, String newColName, ColumnType colType,
            String tableName, boolean notNull, JDBCConnectionProxy jdbc) throws SQLException {
        // TODO !

        if (log.isWarnEnabled()) {
            log.warn( "ORACLE renameColumn not implemented yet !!!" );
        }

        // NOTICE: Only used on MySQL to adapt Column to Oracle name.
        // In Oracle column names are "correct" with initial schema of tables !
        // Implement if needed on Oracle ;)

        // see sql = "ALTER TABLE " + tableName + " RENAME COLUMN " + tmpColName + " TO " + colName;
        // below in modifyColumn

        throw new SQLException( "ORACLE renameColumn not implemented yet !!!" );
    }

    public void modifyColumn(String colName, ColumnType colType, String tableName, boolean notNull,
            JDBCConnectionProxy jdbc) throws SQLException {
        String newColTypeOracle = mapColumnTypeToSQL( colType );

        // CLOB needs different handling !
        if ("CLOB".equals( newColTypeOracle )) {
            String tmpColName = colName + "2";
            String sql = "ALTER TABLE " + tableName + " ADD " + tmpColName + " CLOB";
            if (notNull) {
                sql += " NOT NULL";
                // NOTICE: CAUSES ERROR ON ORACLE if already NOT NULL ! (ORA-01442: Die auf NOT NULL zu ändernde Spalte ist bereits NOT NULL)
                // NOTICE: adding default value causes ERROR ! is added by jdbc automatically !
            }
            jdbc.executeUpdate( sql );

            sql = "UPDATE " + tableName + " SET " + tmpColName + " = " + colName;
            jdbc.executeUpdate( sql );

            sql = "ALTER TABLE " + tableName + " DROP COLUMN " + colName;
            jdbc.executeUpdate( sql );

            sql = "ALTER TABLE " + tableName + " RENAME COLUMN " + tmpColName + " TO " + colName;
            jdbc.executeUpdate( sql );

        } else {
            String sql = "ALTER TABLE " + tableName + " MODIFY " + colName + " " + mapColumnTypeToSQL( colType );
            if (notNull) {
                sql += " NOT NULL";
                // NOTICE: CAUSES ERROR ON ORACLE if already NOT NULL ! (ORA-01442: Die auf NOT NULL zu ändernde Spalte ist bereits NOT NULL)
                // NOTICE: adding default value causes ERROR ! is added by jdbc automatically !
            }
            jdbc.executeUpdate( sql );
        }

        jdbc.commit();
    }

    public void dropColumn(String colName, String tableName, JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "ALTER TABLE " + tableName + " DROP COLUMN " + colName;
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    public void dropTable(String tableName, JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "DROP TABLE " + tableName;
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    public void addIndex(String colName, String tableName, String indexName, JDBCConnectionProxy jdbc)
            throws SQLException {
        String sql = "CREATE INDEX " + indexName + " ON " + tableName + " (" + colName + ")";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    public void createTableObjectConformity(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE object_conformity ( id NUMBER(24,0) NOT NULL,"
                + "  version NUMBER(10,0) DEFAULT '0' NOT NULL, obj_id NUMBER(24,0),"
                + "  line NUMBER(10,0) DEFAULT '0', specification CLOB, degree_key NUMBER(10,0),"
                + "  degree_value VARCHAR2(255 CHAR), publication_date VARCHAR2(17 CHAR))";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE object_conformity ADD CONSTRAINT PRIMARY_10 PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        sql = "CREATE INDEX idxObjConf_ObjId ON object_conformity ( obj_id )";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    public void createTableObjectAccess(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE object_access ( id NUMBER(24,0) NOT NULL,"
                + "  version NUMBER(10,0) DEFAULT '0' NOT NULL, obj_id NUMBER(24,0),"
                + "  line NUMBER(10,0) DEFAULT '0', restriction_key NUMBER(10,0),"
                + "  restriction_value VARCHAR2(255 CHAR), terms_of_use CLOB )";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE object_access ADD CONSTRAINT PRIMARY_8 PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        sql = "CREATE INDEX idxObjAccess_ObjId ON object_access ( obj_id )";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    public void createTableT011ObjServType(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE t011_obj_serv_type ( id NUMBER(24,0) NOT NULL,"
                + "  version NUMBER(10,0) DEFAULT '0' NOT NULL, obj_serv_id NUMBER(24,0),"
                + "  line NUMBER(10,0) DEFAULT '0', serv_type_key NUMBER(10,0), serv_type_value CLOB)";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE t011_obj_serv_type ADD CONSTRAINT PRIMARY_51 PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        sql = "CREATE INDEX idxOSerType_OSerId ON t011_obj_serv_type ( obj_serv_id )";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    public void createTableT011ObjServScale(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE t011_obj_serv_scale ( id NUMBER(24,0) NOT NULL,"
                + "  version NUMBER(10,0) DEFAULT '0' NOT NULL, obj_serv_id NUMBER(24,0),"
                + "  line NUMBER(10,0) DEFAULT '0', scale NUMBER(10,0), resolution_ground FLOAT,"
                + "  resolution_scan FLOAT)";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE t011_obj_serv_scale ADD CONSTRAINT PRIMARY_50 PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        sql = "CREATE INDEX idxOSrvScal_OSrvId ON t011_obj_serv_scale ( obj_serv_id )";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    @Override
    public void createTableT011ObjGeoAxisDim(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE t011_obj_geo_axisdim ( id NUMBER(24,0) NOT NULL,"
                + "  version NUMBER(10,0) DEFAULT '0' NOT NULL, obj_geo_id NUMBER(24,0),"
                + "  line NUMBER(10,0) DEFAULT '0', name VARCHAR2(255 CHAR), count NUMBER(10,0),"
                + "  axis_resolution FLOAT)";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE t011_obj_geo_axisdim ADD CONSTRAINT PRIMARY_52 PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        sql = "CREATE INDEX idxOGeoAxisDim_OGeoId ON t011_obj_geo_axisdim ( obj_geo_id )";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    @Override
    public void createTableT011ObjGeoDataBases(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE t011_obj_geo_data_bases ( id NUMBER(24,0) NOT NULL,"
                + "  version NUMBER(10,0) DEFAULT '0' NOT NULL, obj_geo_id NUMBER(24,0),"
                + "  line NUMBER(10,0) DEFAULT '0', data_base VARCHAR2(1024 CHAR)";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE t011_obj_geo_data_basis ADD CONSTRAINT PRIMARY_52 PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        sql = "CREATE INDEX idxOGeoDataBases_OGeoId ON t011_obj_geo_data_basis ( obj_geo_id )";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    public void createTableSysGui(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE sys_gui ( id NUMBER(24,0) NOT NULL,"
                + "  version NUMBER(10,0) DEFAULT '0' NOT NULL, gui_id VARCHAR2(100 CHAR) NOT NULL,"
                + "  behaviour NUMBER(10,0) DEFAULT '-1' NOT NULL)";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE sys_gui ADD CONSTRAINT PRIMARY_25 PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        sql = "CREATE UNIQUE INDEX gui_id ON sys_gui ( gui_id )";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    public void createTablesMetadata(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE object_metadata ( id NUMBER(24,0) NOT NULL,"
                + "  version NUMBER(10,0) DEFAULT '0' NOT NULL, expiry_state NUMBER(10,0) DEFAULT '0',"
                + "  lastexport_time VARCHAR2(17 CHAR), mark_deleted CHAR(1 CHAR) DEFAULT 'N',"
                + "  assigner_uuid VARCHAR2(40 CHAR), assign_time VARCHAR2(17 CHAR),"
                + "  reassigner_uuid VARCHAR2(40 CHAR), reassign_time VARCHAR2(17 CHAR))";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE object_metadata ADD CONSTRAINT PRIMARY_11 PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );

        sql = "CREATE TABLE address_metadata ( id NUMBER(24,0) NOT NULL,"
                + "  version NUMBER(10,0) DEFAULT '0' NOT NULL, expiry_state NUMBER(10,0) DEFAULT '0',"
                + "  lastexport_time VARCHAR2(17 CHAR), mark_deleted CHAR(1 CHAR) DEFAULT 'N',"
                + "  assigner_uuid VARCHAR2(40 CHAR), assign_time VARCHAR2(17 CHAR),"
                + "  reassigner_uuid VARCHAR2(40 CHAR), reassign_time VARCHAR2(17 CHAR))";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE address_metadata ADD CONSTRAINT PRIMARY_1 PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    public void createTableSysJobInfo(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE sys_job_info ( id NUMBER(24,0) NOT NULL,"
                + "  version NUMBER(10,0) DEFAULT '0' NOT NULL, job_type VARCHAR2(50 CHAR),"
                + "  user_uuid VARCHAR2(40 CHAR), start_time VARCHAR2(17 CHAR),"
                + "  end_time VARCHAR2(17 CHAR), job_details CLOB)";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE sys_job_info ADD CONSTRAINT PRIMARY_26 PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    public void createTableSysGenericKey(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE sys_generic_key ( id NUMBER(24,0) NOT NULL,"
                + "  version NUMBER(10,0) DEFAULT '0' NOT NULL, key_name VARCHAR2(255 CHAR) NOT NULL,"
                + "  value_string VARCHAR2(255 CHAR) )";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE sys_generic_key ADD CONSTRAINT PRIMARY_24 PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        sql = "CREATE UNIQUE INDEX key_name ON sys_generic_key ( key_name ) ";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    public void createTableObjectUse(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE object_use ( id NUMBER(24,0) NOT NULL, " +
                "version NUMBER(10,0) DEFAULT '0' NOT NULL, obj_id NUMBER(24,0), " +
                "line NUMBER(10,0) DEFAULT '0', terms_of_use CLOB )";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE object_use ADD CONSTRAINT PRIMARY_OBJECT_USE PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        sql = "CREATE INDEX idxObjUse_ObjId ON object_use ( obj_id )";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    public void createTableT011ObjServUrl(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE t011_obj_serv_url ( " +
                "id NUMBER(24,0) NOT NULL, " +
                "version NUMBER(10,0) DEFAULT '0' NOT NULL, " +
                "obj_serv_id NUMBER(24,0), " +
                "line NUMBER(10,0) DEFAULT '0', " +
                "name VARCHAR2(1024 CHAR), " +
                "url VARCHAR2(1024 CHAR), " +
                "description VARCHAR2(4000 CHAR))";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE t011_obj_serv_url ADD CONSTRAINT PRIMARY_T011ObjServUrl PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        sql = "CREATE INDEX idxOSerUrl_OSerId ON t011_obj_serv_url ( obj_serv_id )";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    public void createTableObjectDataQuality(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE object_data_quality ( " +
                "id NUMBER(24,0) NOT NULL, " +
                "version NUMBER(10,0) DEFAULT '0' NOT NULL, " +
                "obj_id NUMBER(24,0), " +
                "dq_element_id NUMBER(10,0), " +
                "line NUMBER(10,0) DEFAULT '0', " +
                "name_of_measure_key NUMBER(10,0), " +
                "name_of_measure_value VARCHAR2(255 CHAR), " +
                "result_value VARCHAR2(255 CHAR), " +
                "measure_description VARCHAR2(4000 CHAR))";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE object_data_quality ADD CONSTRAINT PRIMARY_ObjectDataQuality PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        sql = "CREATE INDEX idxObjDq_ObjId ON object_data_quality ( obj_id )";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    public void createTableObjectFormatInspire(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE object_format_inspire ( " +
                "id NUMBER(24,0) NOT NULL, " +
                "version NUMBER(10,0) DEFAULT '0' NOT NULL, " +
                "obj_id NUMBER(24,0), " +
                "line NUMBER(10,0) DEFAULT '0', " +
                "format_key NUMBER(10,0), " +
                "format_value VARCHAR2(255 CHAR))";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE object_format_inspire ADD CONSTRAINT PRIMARY_ObjectFormatInspire PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        sql = "CREATE INDEX idxObjFormatInsp_ObjId ON object_format_inspire ( obj_id )";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    public void createTableIdcUserGroup(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE idc_user_group ( " +
                "id NUMBER(24,0) NOT NULL, " +
                "version NUMBER(10,0) DEFAULT '0' NOT NULL, " +
                "idc_user_id NUMBER(24,0), " +
                "idc_group_id NUMBER(24,0))";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE idc_user_group ADD CONSTRAINT PRIMARY_IdcUserGroup PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    public void createTableAdditionalFieldData(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE additional_field_data ( " +
                "id NUMBER(24,0) NOT NULL, " +
                "version NUMBER(10,0) DEFAULT '0' NOT NULL, " +
                "obj_id NUMBER(24,0), " +
                "sort NUMBER(10,0) DEFAULT '0', " +
                "field_key VARCHAR2(255 CHAR), " +
                "list_item_id VARCHAR2(255 CHAR), " +
                "data CLOB, " +
                "parent_field_id NUMBER(24,0))";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE additional_field_data ADD CONSTRAINT PRIMARY_AddFieldData PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        sql = "CREATE INDEX idxAddFieldData_ObjId ON additional_field_data ( obj_id )";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    public void createTableSpatialSystem(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE spatial_system (" +
                "id NUMBER(24,0) NOT NULL, " +
                "version NUMBER(10,0) DEFAULT '0' NOT NULL, " +
                "obj_id NUMBER(24,0) NOT NULL, " +
                "line NUMBER(10,0) DEFAULT '0' NOT NULL, " +
                "referencesystem_key NUMBER(10,0), " +
                "referencesystem_value VARCHAR2(255 CHAR))";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE spatial_system ADD CONSTRAINT PRIMARY_SpatialSystem PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        sql = "CREATE INDEX idxSSys_ObjId ON spatial_system ( obj_id )";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    public void createTableObjectTypesCatalogue(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE object_types_catalogue (" +
                "id  NUMBER(24,0) NOT NULL, " +
                "version NUMBER(10,0) DEFAULT '0' NOT NULL, " +
                "obj_id NUMBER(24,0) NOT NULL, " +
                "line NUMBER(10,0) DEFAULT '0' NOT NULL, " +
                "title_key NUMBER(10,0), " +
                "title_value VARCHAR2(255 CHAR), " +
                "type_date VARCHAR2(17 CHAR), " +
                "type_version VARCHAR2(255 CHAR))";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE object_types_catalogue ADD CONSTRAINT PRIMARY_ObjectTypesCatalogue PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        sql = "CREATE INDEX idxOTypCat_ObjId ON object_types_catalogue ( obj_id )";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    public void createTableObjectOpenDataCategory(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE object_open_data_category (" +
                "id NUMBER(24,0) NOT NULL, " +
                "version NUMBER(10,0) DEFAULT '0' NOT NULL, " +
                "obj_id NUMBER(24,0), " +
                "line NUMBER(10,0) DEFAULT '0', " +
                "category_key NUMBER(10,0), " +
                "category_value VARCHAR2(4000 CHAR))";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE object_open_data_category ADD CONSTRAINT PRIMARY_ObjectOpenDataCategory PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        sql = "CREATE INDEX idxObjODCategory_ObjId ON object_open_data_category ( obj_id )";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    public void createTableObjectUseConstraint(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE object_use_constraint ("
                + "id NUMBER(24,0) NOT NULL, "
                + "version NUMBER(10,0) DEFAULT '0' NOT NULL, "
                + "obj_id NUMBER(24,0), "
                + "line NUMBER(10,0) DEFAULT '0', "
                + "license_key NUMBER(10,0), "
                + "license_value VARCHAR2(4000 CHAR))";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE object_use_constraint ADD CONSTRAINT PRIMARY_ObjectUseConstraint PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        sql = "CREATE INDEX idxObjUConstr_ObjId ON object_use_constraint ( obj_id )";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    private String mapColumnTypeToSQL(ColumnType colType) {
        String sql = "";

        if (colType == ColumnType.TEXT) {
            sql = "CLOB";
        } else if (colType == ColumnType.TEXT_NO_CLOB) {
            sql = "VARCHAR2(4000 CHAR)";
        } else if (colType == ColumnType.MEDIUMTEXT) {
            sql = "CLOB";
        } else if (colType == ColumnType.VARCHAR1) {
            sql = "VARCHAR2(1 CHAR)";
        } else if (colType == ColumnType.VARCHAR17) {
            sql = "VARCHAR2(17 CHAR)";
        } else if (colType == ColumnType.VARCHAR50) {
            sql = "VARCHAR2(50 CHAR)";
        } else if (colType == ColumnType.VARCHAR255) {
            sql = "VARCHAR2(255 CHAR)";
        } else if (colType == ColumnType.VARCHAR1024) {
            sql = "VARCHAR2(1024 CHAR)";
        } else if (colType == ColumnType.INTEGER) {
            sql = "NUMBER(10,0)";
        } else if (colType == ColumnType.BIGINT) {
            sql = "NUMBER(24,0)";
        } else if (colType == ColumnType.DOUBLE) {
            sql = "FLOAT";
        } else if (colType == ColumnType.DATE) {
            sql = "DATE";
        }

        return sql;
    }

    @Override
    public void createTableAdvProductGroup(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE adv_product_group ("
                + "id NUMBER(24,0) NOT NULL, "
                + "version NUMBER(10,0) DEFAULT '0' NOT NULL, "
                + "obj_id NUMBER(24,0), "
                + "line NUMBER(10,0) DEFAULT '0', "
                + "product_key NUMBER(10,0), "
                + "product_value VARCHAR2(255 CHAR))";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE adv_product_group ADD CONSTRAINT PRIMARY_AdvProductGroup PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        sql = "CREATE INDEX idxAdvPG_ObjId ON adv_product_group ( obj_id )";
        jdbc.executeUpdate( sql );
        jdbc.commit();

    }

    @Override
    public void createTableObjectDataLanguage(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE object_data_language ("
                + "id NUMBER(24,0) NOT NULL, "
                + "version NUMBER(10,0) DEFAULT '0' NOT NULL, "
                + "obj_id NUMBER(24,0), "
                + "line NUMBER(10,0) DEFAULT '0', "
                + "data_language_key NUMBER(10,0), "
                + "data_language_value VARCHAR2(255 CHAR))";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE object_data_language ADD CONSTRAINT PRIMARY_ObjectDataLanguage PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        sql = "CREATE INDEX idxObjDLang_ObjId ON object_data_language ( obj_id )";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    @Override
    public void createTablePriorityDataset(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE priority_dataset ("
                + "id NUMBER(24,0) NOT NULL, "
                + "version NUMBER(10,0) DEFAULT '0' NOT NULL, "
                + "obj_id NUMBER(24,0), "
                + "line NUMBER(10,0) DEFAULT '0', "
                + "priority_key NUMBER(10,0), "
                + "priority_value VARCHAR2(255 CHAR))";
        jdbc.executeUpdate( sql );
        sql = "ALTER TABLE priority_dataset ADD CONSTRAINT PRIMARY_PriorityDataset PRIMARY KEY ( id ) ENABLE";
        jdbc.executeUpdate( sql );
        sql = "CREATE INDEX idxPrioData_ObjId ON priority_dataset ( obj_id )";
        jdbc.executeUpdate( sql );
        jdbc.commit();
    }

    @Override
    public void createDatabase(JDBCConnectionProxy jdbcConnectionProxy, Connection dbConnection, String dbName, String user) throws SQLException {
        // TODO implement if possible

    }

    @Override
    public void importFileToDatabase(JDBCConnectionProxy jdbcConnectionProxy) throws SQLException, IOException {
        // TODO implement if possible

    }

    public int checkIndexExists(JDBCConnectionProxy jdbc, String tableName, String indexName) throws SQLException {
        indexName = indexName.toUpperCase();
        tableName = tableName.toUpperCase();

        String sql = "select count(*) as index_exists "
                + "from dba_ind_columns "
                + "where table_name = '" + tableName + "' and index_name = '" + indexName + "' ";
        Statement st = jdbc.createStatement();

        ResultSet exists = jdbc.executeQuery( sql, st );
        exists.next();

        return exists.getInt( "index_exists" );
    }
}
