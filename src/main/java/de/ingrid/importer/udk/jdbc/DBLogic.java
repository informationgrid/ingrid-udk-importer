package de.ingrid.importer.udk.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * This is the interface for all DBLogic implementations
 * 
 * @author joachim@wmeove.com
 */
public interface DBLogic {
	
	public static enum ColumnType {
		TEXT,
	}

	void setSchema(Connection connection, String schema) throws Exception;

	/**
	 * DDL Operation ! CAUSES COMMIT ON MySQL ! Add a column to a table.
	 * @param colName
	 * @param colType
	 * @param tableName
	 * @param notNull if true the default value is set per jdbc automatically
	 * @param jdbc
	 * @throws SQLException
	 */
	void addColumn(String colName, ColumnType colType, String tableName, 
		boolean notNull, JDBCConnectionProxy jdbc) throws SQLException;

	/**
	 * DDL Operation ! CAUSES COMMIT ON MySQL !
	 */
	void createTableObjectConformity(JDBCConnectionProxy jdbc) throws SQLException;
}
