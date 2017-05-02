/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2017 wemove digital solutions GmbH
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

import java.sql.Connection;
import java.sql.SQLException;

/**
 * This class implements the database logic for Microsoft SQL
 * 
 * @author joachim@wemove.com
 */
public class MSSQLLogic implements DBLogic {

//	private static Log log = LogFactory.getLog(MSSQLLogic.class);

	public void setSchema(Connection connection, String schema) throws Exception {
		// no schema support for mssql
	}

	public void addColumn(String colName, ColumnType colType, String tableName, boolean notNull, 
			Object defaultValue, JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}

	public void modifyColumn(String colName, ColumnType colType, String tableName, boolean notNull, 
			JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}

	public void renameColumn(String oldColName, String newColName, ColumnType colType, 
			String tableName, boolean notNull, JDBCConnectionProxy jdbc) throws SQLException {
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
	public void createTableObjectUse(JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}
	public void createTableT011ObjServUrl(JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}
	public void createTableObjectDataQuality(JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}
	public void createTableObjectFormatInspire(JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}
	public void createTableIdcUserGroup(JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}
	public void createTableAdditionalFieldData(JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}
	public void createTableSpatialSystem(JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}
	public void createTableObjectTypesCatalogue(JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}
	public void createTableObjectOpenDataCategory(JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}
    public void createTableObjectUseConstraint(JDBCConnectionProxy jdbc) throws SQLException {
        // TODO !
    }

    @Override
    public void createTableAdvProductGroup(JDBCConnectionProxy jdbc) throws SQLException {
        // TODO !
        
    }
}
