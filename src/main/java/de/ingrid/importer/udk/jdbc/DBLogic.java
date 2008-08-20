package de.ingrid.importer.udk.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * This is the interface for all DBLogic implementations
 * 
 * @author joachim@wemove.com
 */
public interface DBLogic {
	
	public static enum ColumnType {
		TEXT,
		VARCHAR50,
		INTEGER
	}

	void setSchema(Connection connection, String schema) throws Exception;

	/**
	 * DDL Operation ! CAUSES COMMIT ON MySQL ! ADD a column.
	 * @param notNull pass true if value cannot be null.
	 * 		NOTICE: if true the default value is set per jdbc automatically.
	 * @param defaultValue pass null if no default value. ONLY PASS IF notNull=false.
	 */
	void addColumn(String colName, ColumnType colType, String tableName, 
		boolean notNull, Object defaultValue, JDBCConnectionProxy jdbc) throws SQLException;

	/**
	 * DDL Operation ! CAUSES COMMIT ON MySQL ! Modify a column (new type or constraint)
	 * @param notNull if true the default value is set per jdbc automatically ?
	 */
	void modifyColumn(String colName, ColumnType colType, String tableName, 
		boolean notNull, JDBCConnectionProxy jdbc) throws SQLException;

	/** DDL Operation ! CAUSES COMMIT ON MySQL ! DROP a column. */
	void dropColumn(String colName, String tableName, JDBCConnectionProxy jdbc) throws SQLException;

	/** DDL Operation ! CAUSES COMMIT ON MySQL ! */
	void createTableObjectConformity(JDBCConnectionProxy jdbc) throws SQLException;
	/** DDL Operation ! CAUSES COMMIT ON MySQL ! */
	void createTableObjectAccess(JDBCConnectionProxy jdbc) throws SQLException;
	/** DDL Operation ! CAUSES COMMIT ON MySQL ! */
	void createTableT011ObjServType(JDBCConnectionProxy jdbc) throws SQLException;
	/** DDL Operation ! CAUSES COMMIT ON MySQL ! */
	void createTableT011ObjServScale(JDBCConnectionProxy jdbc) throws SQLException;
	/** DDL Operation ! CAUSES COMMIT ON MySQL ! */
	void createTableSysGui(JDBCConnectionProxy jdbc) throws SQLException;
}
