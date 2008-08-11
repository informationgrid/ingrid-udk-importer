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
			JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "ALTER TABLE " + tableName	+ " ADD " + colName;

		if (colType == ColumnType.TEXT) {
			sql += " TEXT";
		}

		if (notNull) {
			sql += " NOT NULL";
			// NOTICE: adding default value causes ERROR ! is added by jdbc automatically !
		}

		jdbc.executeUpdate(sql);
	}

	public void modifyColumn(String colName, ColumnType colType, String tableName, boolean notNull, 
			JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "ALTER TABLE " + tableName	+ " MODIFY " + colName;

		if (colType == ColumnType.TEXT) {
			sql += " TEXT";
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
}
