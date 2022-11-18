/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2022 wemove digital solutions GmbH
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
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.io.ClassPathResource;

/**
 * This class implements the database logic for PostgreSQL
 * 
 * @author martin@wemove.com
 */
public class PostgreSQLLogic implements DBLogic {

    /**
     * The logging object
     */
    private static Log log = LogFactory.getLog(PostgreSQLLogic.class);

	public void setSchema(Connection connection, String schema) throws Exception {
        if (connection == null) {
            throw new IllegalArgumentException("Connection parameter can't be null");
        }
        if (schema == null) {
            throw new IllegalArgumentException("Schema parameter can't be null");
        }

        if (schema.trim().equals("")) {
/*
            if (log.isDebugEnabled()) {
                log.debug("Using the default schema for the current user "
                        + (connection.getMetaData() != null ? connection.getMetaData().getUserName() : ""));
            }
*/
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("Setting schema to '" + schema + "'");
        }

        Statement statement = connection.createStatement();
        statement.execute("set search_path to '" + schema + "'");
        statement.close();
	}

	public void addColumn(String colName, ColumnType colType, String tableName, boolean notNull, 
			Object defaultValue, JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "ALTER TABLE " + tableName	+ " ADD " + colName + " " + mapColumnTypeToSQL(colType);

		if (notNull) {
			sql += " NOT NULL";
			// NOTICE: adding default value causes ERROR (at least when type string) ! is added by jdbc automatically !
		}
		if (defaultValue != null) {
			sql += " DEFAULT " + defaultValue;
		}

		jdbc.executeUpdate(sql);
	}

	public void modifyColumn(String colName, ColumnType colType, String tableName, boolean notNull, 
			JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "ALTER TABLE " + tableName + " ALTER COLUMN " + colName + " TYPE " + mapColumnTypeToSQL(colType);
        jdbc.executeUpdate(sql);

		if (notNull) {
	        sql = "ALTER TABLE " + tableName + " ALTER COLUMN " + colName + " SET NOT NULL";
		} else {
            sql = "ALTER TABLE " + tableName + " ALTER COLUMN " + colName + " DROP NOT NULL";
		}
        jdbc.executeUpdate(sql);            
	}

	public void renameColumn(String oldColName, String newColName, ColumnType colType, 
			String tableName, boolean notNull, JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "ALTER TABLE " + tableName	+ " RENAME COLUMN " + oldColName + " TO " + newColName;  
        jdbc.executeUpdate(sql);

        modifyColumn( newColName, colType, tableName, notNull, jdbc );
	}

	public void dropColumn(String colName, String tableName, JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "ALTER TABLE " + tableName	+ " DROP COLUMN " + colName;
		jdbc.executeUpdate(sql);
	}

	public void dropTable(String tableName, JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "DROP TABLE " + tableName;
		jdbc.executeUpdate(sql);
	}

	public void addIndex(String colName, String tableName, String indexName,
			JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE INDEX " + indexName + " ON " + tableName + " (" + colName + ")";
        jdbc.executeUpdate(sql);
	}

	public void dropKey(String tableName, String keyName,
						JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "ALTER TABLE " + tableName + " DROP CONSTRAINT " + keyName;
		jdbc.executeUpdate( sql );
	}

	public void addKey(String tableName, String keyName, String columns,
					   JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "ALTER TABLE " + tableName + " ADD UNIQUE ( " + columns + ")";
		jdbc.executeUpdate( sql );
	}

	public void createTableObjectConformity(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE object_conformity(" +
			"id BIGINT NOT NULL, " +
			"version INTEGER NOT NULL DEFAULT 0, " +
			"obj_id BIGINT, " +
			"line INTEGER DEFAULT 0, " +
			"specification TEXT, " +
			"degree_key INTEGER, " +
			"degree_value VARCHAR(255),	" +
			"publication_date VARCHAR(17), " +
			"PRIMARY KEY (id))";
		jdbc.executeUpdate(sql);
        sql = "CREATE INDEX idxObjConf_ObjId ON object_conformity (obj_id ASC)";
        jdbc.executeUpdate(sql);
	}

	public void createTableObjectAccess(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE object_access(" +
			"id BIGINT NOT NULL, " +
			"version INTEGER NOT NULL DEFAULT 0, " +
			"obj_id BIGINT, " +
			"line INTEGER DEFAULT 0, " +
			"restriction_key INTEGER, " +
			"restriction_value VARCHAR(255), " +
			"terms_of_use TEXT,	" +
			"PRIMARY KEY (id))";
		jdbc.executeUpdate(sql);
        sql = "CREATE INDEX idxObjAccess_ObjId ON object_access (obj_id ASC)";
        jdbc.executeUpdate(sql);
	}

	public void createTableT011ObjServType(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE t011_obj_serv_type(" +
			"id BIGINT NOT NULL, " +
			"version INTEGER NOT NULL DEFAULT 0, " +
			"obj_serv_id BIGINT, " +
			"line INTEGER DEFAULT 0, " +
			"serv_type_key INTEGER, " +
			"serv_type_value TEXT, " +
			"PRIMARY KEY (id))";
		jdbc.executeUpdate(sql);
        sql = "CREATE INDEX idxOSerType_OSerId ON t011_obj_serv_type (obj_serv_id ASC)";
        jdbc.executeUpdate(sql);
	}
	public void createTableT011ObjServScale(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE t011_obj_serv_scale(" +
			"id BIGINT NOT NULL, " +
			"version INTEGER NOT NULL DEFAULT 0, " +
			"obj_serv_id BIGINT, " +
			"line INTEGER DEFAULT 0, " +
			"scale INTEGER, " +
			"resolution_ground DOUBLE PRECISION, " +
			"resolution_scan DOUBLE PRECISION, " +
			"PRIMARY KEY (id))";
		jdbc.executeUpdate(sql);
        sql = "CREATE INDEX idxOSrvScal_OSrvId ON t011_obj_serv_scale (obj_serv_id ASC)";
        jdbc.executeUpdate(sql);
	}

	@Override
	public void createTableT011ObjGeoAxisDim(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE t011_obj_geo_axisdim(" +
				"id BIGINT NOT NULL, " +
				"version INTEGER NOT NULL DEFAULT 0, " +
				"obj_geo_id BIGINT, " +
				"line INTEGER DEFAULT 0, " +
				"name TEXT, " +
				"count INTEGER, " +
				"axis_resolution DOUBLE PRECISION, " +
				"PRIMARY KEY (id))";
		jdbc.executeUpdate(sql);
		sql = "CREATE INDEX idxOGeoAxisDim_OGeoId ON t011_obj_geo_axisdim (obj_geo_id ASC)";
		jdbc.executeUpdate(sql);
	}

	@Override
	public void createTableT011ObjGeoDataBases(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE t011_obj_geo_data_bases(" +
				"id BIGINT NOT NULL, " +
				"version INTEGER NOT NULL DEFAULT 0, " +
				"obj_geo_id BIGINT, " +
				"line INTEGER DEFAULT 0, " +
				"data_base TEXT, " +
				"PRIMARY KEY (id))";
		jdbc.executeUpdate(sql);
		sql = "CREATE INDEX idxOGeoDataBases_OGeoId ON t011_obj_geo_data_bases (obj_geo_id ASC)";
		jdbc.executeUpdate(sql);
	}

	public void createTableSysGui(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE sys_gui(" +
			"id BIGINT NOT NULL, " +
			"version INTEGER NOT NULL DEFAULT 0, " +
			"gui_id VARCHAR(100) NOT NULL, " +
			"behaviour INTEGER NOT NULL DEFAULT -1, " +
			"PRIMARY KEY (id), " +
			"UNIQUE (gui_id))";
		jdbc.executeUpdate(sql);
	}
	public void createTablesMetadata(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE object_metadata(" +
			"id BIGINT NOT NULL, " +
			"version INTEGER NOT NULL DEFAULT 0, " +
			"expiry_state INTEGER DEFAULT 0, " +
			"lastexport_time VARCHAR(17), " +
			"mark_deleted CHAR(1) DEFAULT 'N', " +
			"assigner_uuid VARCHAR(40), " +
			"assign_time VARCHAR(17), " +
			"reassigner_uuid VARCHAR(40), " +
			"reassign_time VARCHAR(17), " +
			"PRIMARY KEY (id))";
		jdbc.executeUpdate(sql);
		
		sql = "CREATE TABLE address_metadata(" +
			"id BIGINT NOT NULL, " +
			"version INTEGER NOT NULL DEFAULT 0, " +
			"expiry_state INTEGER DEFAULT 0, " +
			"lastexport_time VARCHAR(17), " +
			"mark_deleted CHAR(1) DEFAULT 'N', " +
			"assigner_uuid VARCHAR(40), " +
			"assign_time VARCHAR(17), " +
			"reassigner_uuid VARCHAR(40), " +
			"reassign_time VARCHAR(17), " +
			"PRIMARY KEY (id))";
		jdbc.executeUpdate(sql);
	}
	public void createTableSysJobInfo(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE sys_job_info(" +
			"id BIGINT NOT NULL," +
			"version INTEGER NOT NULL DEFAULT 0," +
			"job_type VARCHAR(50)," +
			"user_uuid VARCHAR(40)," +
			"start_time VARCHAR(17)," +
			"end_time VARCHAR(17)," +
			"job_details TEXT," +
			"PRIMARY KEY (id))";
		jdbc.executeUpdate(sql);
	}
	public void createTableSysGenericKey(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE sys_generic_key(" +
			"id BIGINT NOT NULL," +
			"version INTEGER NOT NULL DEFAULT 0," +
			"key_name VARCHAR(255) NOT NULL," +
			"value_string VARCHAR(255)," +
			"PRIMARY KEY (id)," +
			"UNIQUE (key_name))";
		jdbc.executeUpdate(sql);
	}

	public void createTableObjectUse(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE object_use(" +
			"id BIGINT NOT NULL, " +
			"version INTEGER NOT NULL DEFAULT 0, " +
			"obj_id BIGINT, " +
			"line INTEGER DEFAULT 0, " +
			"terms_of_use TEXT, " +
			"PRIMARY KEY (id))";
		jdbc.executeUpdate(sql);
        sql = "CREATE INDEX idxObjUse_ObjId ON object_use (obj_id ASC)";
        jdbc.executeUpdate(sql);
	}
	public void createTableT011ObjServUrl(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE t011_obj_serv_url(" +
			"id BIGINT NOT NULL, " +
			"version INTEGER NOT NULL DEFAULT 0, " +
			"obj_serv_id BIGINT, " +
			"line INTEGER DEFAULT 0, " +
			"name VARCHAR(1024), " +
			"url VARCHAR(1024), " +
			"description VARCHAR(4000), " +
			"PRIMARY KEY (id))";
		jdbc.executeUpdate(sql);
        sql = "CREATE INDEX idxOSerUrl_OSerId ON t011_obj_serv_url (obj_serv_id ASC)";
        jdbc.executeUpdate(sql);
	}

	public void createTableObjectDataQuality(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE object_data_quality(" +
			"id BIGINT NOT NULL, " +
			"version INTEGER NOT NULL DEFAULT 0, " +
			"obj_id BIGINT, " +
			"dq_element_id INTEGER, " +
			"line INTEGER DEFAULT 0, " +
			"name_of_measure_key INTEGER, " +
			"name_of_measure_value VARCHAR(255), " +
			"result_value VARCHAR(255), " +
			"measure_description VARCHAR(4000), " +
			"PRIMARY KEY (id))";
		jdbc.executeUpdate(sql);
        sql = "CREATE INDEX idxObjDq_ObjId ON object_data_quality (obj_id ASC)";
        jdbc.executeUpdate(sql);
	}

	public void createTableObjectFormatInspire(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE object_format_inspire(" +
			"id BIGINT NOT NULL, " +
			"version INTEGER NOT NULL DEFAULT 0, " +
			"obj_id BIGINT, " +
			"line INTEGER DEFAULT 0, " +
			"format_key INTEGER, " +
			"format_value VARCHAR(255), " +
			"PRIMARY KEY (id))";
		jdbc.executeUpdate(sql);
        sql = "CREATE INDEX idxObjFormatInsp_ObjId ON object_format_inspire (obj_id ASC)";
        jdbc.executeUpdate(sql);
	}

	public void createTableIdcUserGroup(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE idc_user_group(" +
			"id BIGINT NOT NULL, " +
			"version INTEGER NOT NULL DEFAULT 0, " +
			"idc_user_id BIGINT, " +
			"idc_group_id BIGINT, " +
			"PRIMARY KEY (id))";
		jdbc.executeUpdate(sql);
	}

	public void createTableAdditionalFieldData(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE additional_field_data(" +
			"id BIGINT NOT NULL, " +
			"version INTEGER NOT NULL DEFAULT 0, " +
			"obj_id BIGINT, " +
			"sort INTEGER DEFAULT 0, " +
			"field_key VARCHAR(255), " +
			"list_item_id VARCHAR(255), " +
			"data TEXT, " +
			"parent_field_id BIGINT, " +
			"PRIMARY KEY (id))";
		jdbc.executeUpdate(sql);
        sql = "CREATE INDEX idxAddField_ObjId ON additional_field_data (obj_id ASC)";
        jdbc.executeUpdate(sql);
	}

	public void createTableSpatialSystem(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE spatial_system (" +
			"id BIGINT NOT NULL, " +
			"version INTEGER NOT NULL DEFAULT 0, " +
			"obj_id BIGINT NOT NULL, " +
			"line INTEGER NOT NULL DEFAULT 0, " +
			"referencesystem_key INTEGER, " +
			"referencesystem_value VARCHAR(255), " +
			"PRIMARY KEY (id))";
		jdbc.executeUpdate(sql);
        sql = "CREATE INDEX idxSSys_ObjId ON spatial_system (obj_id ASC)";
        jdbc.executeUpdate(sql);
	}

	public void createTableObjectTypesCatalogue(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE object_types_catalogue (" +
			"id BIGINT NOT NULL, " +
			"version INTEGER NOT NULL DEFAULT 0, " +
			"obj_id BIGINT, " +
			"line INTEGER DEFAULT 0, " +
			"title_key INTEGER, " +
			"title_value VARCHAR(255), " +
			"type_date VARCHAR(17), " +
			"type_version VARCHAR(255), " +
			"PRIMARY KEY (id))";
		jdbc.executeUpdate(sql);
        sql = "CREATE INDEX idxOTypCat_ObjId ON object_types_catalogue (obj_id ASC)";
        jdbc.executeUpdate(sql);
	}

	public void createTableObjectOpenDataCategory(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE object_open_data_category(" +
				"id BIGINT NOT NULL, " +
				"version INTEGER NOT NULL DEFAULT 0, " +
				"obj_id BIGINT, " +
				"line INTEGER DEFAULT 0, " +
				"category_key INTEGER, " +
				"category_value TEXT, " +
				"PRIMARY KEY (id))";
		jdbc.executeUpdate(sql);
        sql = "CREATE INDEX idxObjODCategory_ObjId ON object_open_data_category (obj_id ASC)";
        jdbc.executeUpdate(sql);
	}

    public void createTableObjectUseConstraint(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE object_use_constraint ("
                + "id BIGINT NOT NULL, "
                + "version INTEGER NOT NULL DEFAULT 0, "
                + "obj_id BIGINT, "
                + "line INTEGER DEFAULT 0, "
                + "license_key INTEGER, "
                + "license_value TEXT, "
                + "PRIMARY KEY (id))";
        jdbc.executeUpdate(sql);        
        sql = "CREATE INDEX idxObjUConstr_ObjId ON object_use_constraint (obj_id ASC)";
        jdbc.executeUpdate(sql);
    }

	private String mapColumnTypeToSQL(ColumnType colType) {
		String sql = "";

		if (colType == ColumnType.TEXT || colType == ColumnType.TEXT_NO_CLOB) {
			sql = "TEXT";
		} else if (colType == ColumnType.MEDIUMTEXT) {
			sql = "TEXT";
		} else if (colType == ColumnType.VARCHAR1) {
			sql = "VARCHAR(1)";
        } else if (colType == ColumnType.VARCHAR17) {
            sql = "VARCHAR(17)";
		} else if (colType == ColumnType.VARCHAR50) {
			sql = "VARCHAR(50)";
		} else if (colType == ColumnType.VARCHAR255) {
			sql = "VARCHAR(255)";
		} else if (colType == ColumnType.VARCHAR1024) {
			sql = "VARCHAR(1024)";
		} else if (colType == ColumnType.VARCHAR4096) {
			sql = "VARCHAR(4096)";
		} else if (colType == ColumnType.INTEGER) {
			sql = "INTEGER";
		} else if (colType == ColumnType.BIGINT) {
			sql = "BIGINT";
		} else if (colType == ColumnType.DOUBLE) {
		    sql = "DOUBLE PRECISION";
		} else if (colType == ColumnType.DATE) {
			sql = "DATE";
		}
		
		return sql;
	}

    @Override
    public void createTableAdvProductGroup(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE adv_product_group (" +
            "id BIGINT NOT NULL, " +
            "version INTEGER NOT NULL DEFAULT 0, " +
            "obj_id BIGINT, " +
            "line INTEGER DEFAULT 0, " +
            "product_key INTEGER, " +
            "product_value VARCHAR(255), " +
            "PRIMARY KEY (id))";
        jdbc.executeUpdate(sql);
        sql = "CREATE INDEX idxAdvPG_ObjId ON adv_product_group (obj_id ASC)";
        jdbc.executeUpdate(sql);
    }

    @Override
    public void createTableObjectDataLanguage(JDBCConnectionProxy jdbc) throws SQLException {
        String sql = "CREATE TABLE object_data_language ("
                + "id BIGINT NOT NULL, "
                + "version INTEGER NOT NULL DEFAULT 0, "
                + "obj_id BIGINT, "
                + "line INTEGER DEFAULT 0, "
                + "data_language_key INTEGER, "
                + "data_language_value VARCHAR(255), "
                + "PRIMARY KEY (id))";
        jdbc.executeUpdate(sql);        
        sql = "CREATE INDEX idxObjDLang_ObjId ON object_data_language (obj_id ASC)";
        jdbc.executeUpdate(sql);
    }

	@Override
	public void createTablePriorityDataset(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE priority_dataset ("
				+ "id BIGINT NOT NULL, "
				+ "version INTEGER NOT NULL DEFAULT 0, "
				+ "obj_id BIGINT, "
				+ "line INTEGER DEFAULT 0, "
				+ "priority_key INTEGER, "
				+ "priority_value VARCHAR(255), "
				+ "PRIMARY KEY (id))";
		jdbc.executeUpdate(sql);
		sql = "CREATE INDEX idxPrioData_ObjId ON priority_dataset (obj_id ASC)";
		jdbc.executeUpdate(sql);
	}

	@Override
    public void createDatabase(JDBCConnectionProxy jdbc, Connection dbConnection, String dbName, String user) throws SQLException {
        // TODO: check if this works!
        String sql = "CREATE DATABASE " + dbName + " WITH OWNER = " + user + " ENCODING='UTF8' CONNECTION LIMIT=-1";
        jdbc.executeUpdate( dbConnection, sql );
    }

    @Override
    public void importFileToDatabase(JDBCConnectionProxy jdbc) throws SQLException, IOException {
        // TODO: check if this works!
        InputStream importStream = new ClassPathResource( "/ingrid-igc-schema_102_postgres.sql" ).getInputStream();
        jdbc.importFile( importStream  );
    }

	@Override
	public int checkIndexExists(JDBCConnectionProxy jdbc, String tableName, String indexName) throws SQLException{
		indexName = indexName.toLowerCase();
		tableName = tableName.toLowerCase();
		String sql = "SELECT count(*) AS index_exists " +
				"FROM pg_indexes " +
				"WHERE tablename = '" + tableName + "' AND indexname = '" + indexName + "'";
		Statement st = jdbc.createStatement();
		ResultSet exists = jdbc.executeQuery( sql, st );

		exists.next();

		return exists.getInt("index_exists");
	}
}
