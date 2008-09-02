package de.ingrid.importer.udk.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class implements the database logic for MySQL
 * 
 * @author joachim@wemove.com
 */
public class MySQLLogic implements DBLogic {

	/** The logging object */
	private static Log log = LogFactory.getLog(MySQLLogic.class);

	public void setSchema(Connection connection, String schema) throws Exception {
		// mysql does not support db schema
	}

	public void addColumn(String colName, ColumnType colType, String tableName, boolean notNull, 
			Object defaultValue, JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "ALTER TABLE " + tableName	+ " ADD " + colName;

		if (colType == ColumnType.TEXT) {
			sql += " TEXT";
		} else if (colType == ColumnType.VARCHAR50) {
			sql += " VARCHAR(50)";
		} else if (colType == ColumnType.INTEGER) {
			sql += " INTEGER";
		} else if (colType == ColumnType.BIGINT) {
			sql += " BIGINT";
		}

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
		String sql = "ALTER TABLE " + tableName	+ " MODIFY " + colName;

		if (colType == ColumnType.TEXT) {
			sql += " TEXT";
		} else if (colType == ColumnType.VARCHAR50) {
			sql += " VARCHAR(50)";
		} else if (colType == ColumnType.INTEGER) {
			sql += " INTEGER";
		} else if (colType == ColumnType.BIGINT) {
			sql += " BIGINT";
		}

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
			"PRIMARY KEY (id)) " +
			"TYPE=InnoDB;";
		jdbc.executeUpdate(sql);
		
		sql = "CREATE TABLE address_metadata(" +
			"id BIGINT NOT NULL, " +
			"version INTEGER NOT NULL DEFAULT 0, " +
			"expiry_state INTEGER DEFAULT 0, " +
			"lastexport_time VARCHAR(17), " +
			"mark_deleted CHAR(1) DEFAULT 'N', " +
			"PRIMARY KEY (id)) " +
			"TYPE=InnoDB;";
		jdbc.executeUpdate(sql);
	}
}
