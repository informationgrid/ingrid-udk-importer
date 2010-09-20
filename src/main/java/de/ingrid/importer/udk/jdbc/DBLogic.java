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
		TEXT_NO_CLOB,
		MEDIUMTEXT,
		VARCHAR50,
		VARCHAR255,
		INTEGER,
		BIGINT
	}

	void setSchema(Connection connection, String schema) throws Exception;

	/**
	 * DDL Operation ! CAUSES COMMIT ON MySQL ! ADD a column.
	 * @param notNull pass true if value cannot be null.
	 * 		NOTICE: if true the default value is set per jdbc automatically ? (at least when type string)
	 * @param defaultValue pass null if no default value. ONLY PASS IF notNull=false (when type string)
	 */
	void addColumn(String colName, ColumnType colType, String tableName, 
		boolean notNull, Object defaultValue, JDBCConnectionProxy jdbc) throws SQLException;

	/**
	 * Modify column type or constraint (NOT NULL).
	 * DDL Operation ! CAUSES COMMIT ON MySQL ! 
	 * @param notNull pass true if column value cannot be null.
	 * 		NOTICE: if true the default value is set per jdbc automatically ? (at least when type string)
	 */
	void modifyColumn(String colName, ColumnType colType, String tableName, 
		boolean notNull, JDBCConnectionProxy jdbc) throws SQLException;

	/**
	 * Rename a column. You have to pass full definition of column (also if type not changed)
	 * DDL Operation ! CAUSES COMMIT ON MySQL ! 
	 */
	void renameColumn(String oldColName, String newColName, ColumnType colType, 
		String tableName, boolean notNull, JDBCConnectionProxy jdbc) throws SQLException;

	void addIndex(String colName, String tableName, String indexName,
		JDBCConnectionProxy jdbc) throws SQLException;

	/** DDL Operation ! CAUSES COMMIT ON MySQL ! DROP a column. */
	void dropColumn(String colName, String tableName, JDBCConnectionProxy jdbc) throws SQLException;

	/** DDL Operation ! CAUSES COMMIT ON MySQL ! DROP a table. */
	void dropTable(String tableName, JDBCConnectionProxy jdbc) throws SQLException;

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
	/** DDL Operation ! CAUSES COMMIT ON MySQL ! */
	void createTablesMetadata(JDBCConnectionProxy jdbc) throws SQLException;
	/** DDL Operation ! CAUSES COMMIT ON MySQL ! */
	void createTableSysJobInfo(JDBCConnectionProxy jdbc) throws SQLException;
	/** DDL Operation ! CAUSES COMMIT ON MySQL ! */
	void createTableSysGenericKey(JDBCConnectionProxy jdbc) throws SQLException;

	/** DDL Operation ! CAUSES COMMIT ON MySQL ! */
	void createTableObjectUse(JDBCConnectionProxy jdbc) throws SQLException;
}
