package de.ingrid.importer.udk.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * This class implements the database logic for MySQL
 * 
 * @author joachim@wemove.com
 */
public class MySQLLogic implements DBLogic {

	public void setSchema(Connection connection, String schema) throws Exception {
		// mysql does not support db schema
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
		String sql = "ALTER TABLE " + tableName	+ " MODIFY " + colName + " " + mapColumnTypeToSQL(colType);

		if (notNull) {
			sql += " NOT NULL";
			// NOTICE: adding default value causes ERROR ! is added by jdbc automatically !
		}

		jdbc.executeUpdate(sql);
	}

	public void renameColumn(String oldColName, String newColName, ColumnType colType, 
			String tableName, boolean notNull, JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "ALTER TABLE " + tableName	+ 
			" CHANGE " + oldColName + " " + newColName +  
			" " + mapColumnTypeToSQL(colType);

		if (notNull) {
			sql += " NOT NULL";
			// NOTICE: adding default value causes ERROR ! is added by jdbc automatically !
		}

		jdbc.executeUpdate(sql);
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
		String sql = "ALTER TABLE " + tableName + " ADD INDEX " + indexName + " (" + colName + ")";
		jdbc.executeUpdate(sql);
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
			"PRIMARY KEY (id), " +
			"INDEX idxObjConf_ObjId (obj_id ASC)) " +
			"TYPE=InnoDB;";
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
			"PRIMARY KEY (id), " +
			"INDEX idxObjAccess_ObjId (obj_id ASC)) " +
			"TYPE=InnoDB;";
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
			"PRIMARY KEY (id), " +
			"INDEX idxOSerType_OSerId (obj_serv_id ASC)) " +
			"TYPE=InnoDB;";
		jdbc.executeUpdate(sql);
	}
	public void createTableT011ObjServScale(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE t011_obj_serv_scale(" +
			"id BIGINT NOT NULL, " +
			"version INTEGER NOT NULL DEFAULT 0, " +
			"obj_serv_id BIGINT, " +
			"line INTEGER DEFAULT 0, " +
			"scale INTEGER, " +
			"resolution_ground DOUBLE, " +
			"resolution_scan DOUBLE, " +
			"PRIMARY KEY (id), " +
			"INDEX idxOSrvScal_OSrvId (obj_serv_id ASC)) " +
			"TYPE=InnoDB;";
		jdbc.executeUpdate(sql);
	}
	public void createTableSysGui(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE sys_gui(" +
			"id BIGINT NOT NULL, " +
			"version INTEGER NOT NULL DEFAULT 0, " +
			"gui_id VARCHAR(100) NOT NULL, " +
			"behaviour INTEGER NOT NULL DEFAULT -1, " +
			"PRIMARY KEY (id), " +
			"UNIQUE (gui_id)) " +
			"TYPE=InnoDB;";
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
			"PRIMARY KEY (id)) " +
			"TYPE=InnoDB;";
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
			"PRIMARY KEY (id)) " +
			"TYPE=InnoDB;";
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
			"job_details MEDIUMTEXT," +
			"PRIMARY KEY (id)) " +
			"TYPE=InnoDB;";
		jdbc.executeUpdate(sql);
	}
	public void createTableSysGenericKey(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE sys_generic_key(" +
			"id BIGINT NOT NULL," +
			"version INTEGER NOT NULL DEFAULT 0," +
			"key_name VARCHAR(255) NOT NULL," +
			"value_string VARCHAR(255)," +
			"PRIMARY KEY (id)," +
			"UNIQUE (key_name)) " +
			"TYPE=InnoDB;";
		jdbc.executeUpdate(sql);
	}

	public void createTableObjectUse(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE object_use(" +
			"id BIGINT NOT NULL, " +
			"version INTEGER NOT NULL DEFAULT 0, " +
			"obj_id BIGINT, " +
			"line INTEGER DEFAULT 0, " +
			"terms_of_use TEXT, " +
			"PRIMARY KEY (id), " +
			"INDEX idxObjUse_ObjId (obj_id ASC)) " +
			"TYPE=InnoDB;";
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
			"PRIMARY KEY (id), " +
			"INDEX idxOSerUrl_OSerId (obj_serv_id ASC)) " +
			"TYPE=InnoDB;";
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
			"PRIMARY KEY (id), " +
			"INDEX idxObjDq_ObjId (obj_id ASC)) " +
			"TYPE=InnoDB;";
		jdbc.executeUpdate(sql);
	}

	private String mapColumnTypeToSQL(ColumnType colType) {
		String sql = "";

		if (colType == ColumnType.TEXT || colType == ColumnType.TEXT_NO_CLOB) {
			sql = "TEXT";
		} else if (colType == ColumnType.MEDIUMTEXT) {
			sql = "MEDIUMTEXT";
		} else if (colType == ColumnType.VARCHAR1) {
			sql = "VARCHAR(1)";
		} else if (colType == ColumnType.VARCHAR50) {
			sql = "VARCHAR(50)";
		} else if (colType == ColumnType.VARCHAR255) {
			sql = "VARCHAR(255)";
		} else if (colType == ColumnType.INTEGER) {
			sql = "INTEGER";
		} else if (colType == ColumnType.BIGINT) {
			sql = "BIGINT";
		}
		
		return sql;
	}
}
