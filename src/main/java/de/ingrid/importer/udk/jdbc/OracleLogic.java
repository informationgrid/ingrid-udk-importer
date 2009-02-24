package de.ingrid.importer.udk.jdbc;

import java.sql.Connection;
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
	private static Log log = LogFactory.getLog(OracleLogic.class);

	public void setSchema(Connection connection, String schema) throws Exception {
		if (connection == null) {
			throw new IllegalArgumentException("Connection parameter can't be null");
		}
		if (schema == null) {
			throw new IllegalArgumentException("Schema parameter can't be null");
		}

		if (schema.trim().equals("")) {
			if (log.isDebugEnabled()) {
				log.debug("Using the default schema/tablespace for the current user "
						+ (connection.getMetaData() != null ? connection.getMetaData().getUserName() : ""));
			}
			return;
		}

		Statement statement = connection.createStatement();

		String changeSchema = "alter session set current_schema = " + schema;
		statement.executeUpdate(changeSchema);
	}

	public void addColumn(String colName, ColumnType colType, String tableName, boolean notNull, 
			Object defaultValue, JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}

	public void modifyColumn(String colName, ColumnType colType, String tableName, boolean notNull, 
			JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}

	public void dropColumn(String colName, String tableName, JDBCConnectionProxy jdbc) throws SQLException {
		// TODO
	}

	public void dropTable(String tableName, JDBCConnectionProxy jdbc) throws SQLException {
		// TODO
	}

	public void addIndex(String colName, String tableName, String indexName,
			JDBCConnectionProxy jdbc) throws SQLException {
		// TODO		
	}

	public void createTableObjectConformity(JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}
	public void createTableObjectAccess(JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}
	public void createTableT011ObjServType(JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}
	public void createTableT011ObjServScale(JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}
	public void createTableSysGui(JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}
	public void createTablesMetadata(JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}
	public void createTableSysJobInfo(JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}
	public void createTableSysGenericKey(JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}
}
