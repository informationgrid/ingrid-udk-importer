package de.ingrid.importer.udk.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class implements the database logic for Microsoft SQL
 * 
 * @author joachim@wemove.com
 */
public class MSSQLLogic implements DBLogic {

	private static Log log = LogFactory.getLog(MSSQLLogic.class);

	public void setSchema(Connection connection, String schema) throws Exception {
		// no schema support for mssql
	}

	public void addColumn(String colName, ColumnType colType, String tableName, boolean notNull, 
			JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}

	public void modifyColumn(String colName, ColumnType colType, String tableName, boolean notNull, 
			JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !
	}

	public void dropColumn(String colName, String tableName, JDBCConnectionProxy jdbc) throws SQLException {
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
}
