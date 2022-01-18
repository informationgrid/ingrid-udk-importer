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
import java.sql.Connection;
import java.sql.SQLException;

/**
 * This is the interface for all DBLogic implementations
 * 
 * @author joachim@wemove.com
 */
public interface DBLogic {
	
	public static enum ColumnType {
		DATE,
		TEXT,
		TEXT_NO_CLOB,
		MEDIUMTEXT,
		VARCHAR1,
        VARCHAR17,
		VARCHAR50,
		VARCHAR255,
		VARCHAR1024,
		VARCHAR4096,
		INTEGER,
		BIGINT,
		DOUBLE
	}

	void setSchema(Connection connection, String schema) throws Exception;

	/**
	 * DDL Operation ! CAUSES COMMIT ON MySQL ! ADD a column.
	 * @param colName
	 * @param colType
	 * @param tableName
	 * @param notNull pass true if value cannot be null.
	 * 		NOTICE: if true the default value is set per jdbc automatically ? (at least when type string)
	 * @param defaultValue pass null if no default value. ONLY PASS IF notNull=false (when type string)
	 * @param jdbc
	 * @throws SQLException
	 */
	void addColumn(String colName, ColumnType colType, String tableName, 
		boolean notNull, Object defaultValue, JDBCConnectionProxy jdbc) throws SQLException;

	/**
	 * Modify column type or constraint (NOT NULL).
	 * DDL Operation ! CAUSES COMMIT ON MySQL ! 
	 * @param colName
	 * @param colType
	 * @param tableName
	 * @param notNull pass true if column value cannot be null.<br>
	 * 		NOTICE: if true the default value is set per jdbc automatically ? (at least when type string)<br>
	 * 		NOTICE: On ORACLE do NOT pass true if NOT NULL constraint already set, CAUSES ERROR ORA-01442: Die auf NOT NULL zu ändernde Spalte ist bereits NOT NULL
	 * @param jdbc
	 * @throws SQLException
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
	void createTableT011ObjGeoAxisDim(JDBCConnectionProxy jdbc) throws SQLException;
	/** DDL Operation ! CAUSES COMMIT ON MySQL ! */
	void createTableT011ObjGeoDataBases(JDBCConnectionProxy jdbc) throws SQLException;
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
	/** DDL Operation ! CAUSES COMMIT ON MySQL ! */
	void createTableT011ObjServUrl(JDBCConnectionProxy jdbc) throws SQLException;

	/** DDL Operation ! CAUSES COMMIT ON MySQL ! */
	void createTableObjectDataQuality(JDBCConnectionProxy jdbc) throws SQLException;
	/** DDL Operation ! CAUSES COMMIT ON MySQL ! */
	void createTableObjectFormatInspire(JDBCConnectionProxy jdbc) throws SQLException;
	/** DDL Operation ! CAUSES COMMIT ON MySQL ! */
	void createTableIdcUserGroup(JDBCConnectionProxy jdbc) throws SQLException;

	/** DDL Operation ! CAUSES COMMIT ON MySQL ! */
	void createTableAdditionalFieldData(JDBCConnectionProxy jdbc) throws SQLException;

	/** DDL Operation ! CAUSES COMMIT ON MySQL ! */
	void createTableSpatialSystem(JDBCConnectionProxy jdbc) throws SQLException;

	/** DDL Operation ! CAUSES COMMIT ON MySQL ! */
	void createTableObjectTypesCatalogue(JDBCConnectionProxy jdbc) throws SQLException;

	/** DDL Operation ! CAUSES COMMIT ON MySQL ! */
	void createTableObjectOpenDataCategory(JDBCConnectionProxy jdbc) throws SQLException;

    /** DDL Operation ! CAUSES COMMIT ON MySQL ! */
    void createTableObjectUseConstraint(JDBCConnectionProxy jdbc) throws SQLException;

    /** DDL Operation ! CAUSES COMMIT ON MySQL ! */
    void createTableAdvProductGroup(JDBCConnectionProxy jdbc) throws SQLException;

    /** DDL Operation ! CAUSES COMMIT ON MySQL ! */
    void createTableObjectDataLanguage(JDBCConnectionProxy jdbc) throws SQLException;

    /** DDL Operation ! CAUSES COMMIT ON MySQL ! */
    void createTablePriorityDataset(JDBCConnectionProxy jdbc) throws SQLException;

    void createDatabase(JDBCConnectionProxy jdbcConnectionProxy, Connection dbConnection, String dbName, String user) throws SQLException;
    
    void importFileToDatabase(JDBCConnectionProxy jdbcConnectionProxy) throws SQLException, IOException;

    int checkIndexExists(JDBCConnectionProxy jdbc, String tableName, String indexName) throws SQLException;
}
