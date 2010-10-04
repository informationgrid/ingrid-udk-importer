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

	public void addColumn(String colName, ColumnType colType, String tableName, boolean notNull, Object defaultValue,
			JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "ALTER TABLE " + tableName + " ADD " + colName + " " + mapColumnTypeToSQL(colType);

		if (notNull) {
			sql += " NOT NULL";
			// NOTICE: adding default value causes ERROR (at least when type
			// string) ! is added by jdbc automatically !
		}
		if (defaultValue != null) {
			sql += " DEFAULT " + defaultValue;
		}

		jdbc.executeUpdate(sql);
		jdbc.commit();
	}

	public void renameColumn(String oldColName, String newColName, ColumnType colType, 
			String tableName, boolean notNull, JDBCConnectionProxy jdbc) throws SQLException {
		// TODO !

		if (log.isWarnEnabled()) {
			log.warn("ORACLE renameColumn not implemented yet !!!");
		}
		
		// NOTICE: Only used on MySQL to adapt Column to Oracle name.
		// In Oracle column names are "correct" with initial schema of tables !
		// Implement if needed on Oracle ;)
		
		throw new SQLException("ORACLE renameColumn not implemented yet !!!");
	}

	public void modifyColumn(String colName, ColumnType colType, String tableName, boolean notNull,
			JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "ALTER TABLE " + tableName + " MODIFY " + colName + " " + mapColumnTypeToSQL(colType);

		if (notNull) {
			sql += " NOT NULL";
			// NOTICE: adding default value causes ERROR ! is added by jdbc
			// automatically !
		}

		jdbc.executeUpdate(sql);
		jdbc.commit();
	}

	public void dropColumn(String colName, String tableName, JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "ALTER TABLE " + tableName + " DROP COLUMN " + colName;
		jdbc.executeUpdate(sql);
		jdbc.commit();
	}

	public void dropTable(String tableName, JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "DROP TABLE " + tableName;
		jdbc.executeUpdate(sql);
		jdbc.commit();
	}

	public void addIndex(String colName, String tableName, String indexName, JDBCConnectionProxy jdbc)
			throws SQLException {
		String sql = "CREATE INDEX " + indexName + " ON " + tableName + " (" + colName + ")";
		jdbc.executeUpdate(sql);
		jdbc.commit();
	}

	public void createTableObjectConformity(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE object_conformity ( id NUMBER(24,0) NOT NULL,"
				+ "  version NUMBER(10,0) DEFAULT '0' NOT NULL, obj_id NUMBER(24,0),"
				+ "  line NUMBER(10,0) DEFAULT '0', specification CLOB, degree_key NUMBER(10,0),"
				+ "  degree_value VARCHAR2(255 CHAR), publication_date VARCHAR2(17 CHAR))";
		jdbc.executeUpdate(sql);
		sql = "ALTER TABLE object_conformity ADD CONSTRAINT PRIMARY_10 PRIMARY KEY ( id ) ENABLE";
		jdbc.executeUpdate(sql);
		sql = "CREATE INDEX idxObjConf_ObjId ON object_conformity ( obj_id )";
		jdbc.executeUpdate(sql);
		jdbc.commit();
	}

	public void createTableObjectAccess(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE object_access ( id NUMBER(24,0) NOT NULL,"
				+ "  version NUMBER(10,0) DEFAULT '0' NOT NULL, obj_id NUMBER(24,0),"
				+ "  line NUMBER(10,0) DEFAULT '0', restriction_key NUMBER(10,0),"
				+ "  restriction_value VARCHAR2(255 CHAR), terms_of_use CLOB )";
		jdbc.executeUpdate(sql);
		sql = "ALTER TABLE object_access ADD CONSTRAINT PRIMARY_8 PRIMARY KEY ( id ) ENABLE";
		jdbc.executeUpdate(sql);
		sql = "CREATE INDEX idxObjAccess_ObjId ON object_access ( obj_id )";
		jdbc.executeUpdate(sql);
		jdbc.commit();
	}

	public void createTableT011ObjServType(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE t011_obj_serv_type ( id NUMBER(24,0) NOT NULL,"
				+ "  version NUMBER(10,0) DEFAULT '0' NOT NULL, obj_serv_id NUMBER(24,0),"
				+ "  line NUMBER(10,0) DEFAULT '0', serv_type_key NUMBER(10,0), serv_type_value CLOB)";
		jdbc.executeUpdate(sql);
		sql = "ALTER TABLE t011_obj_serv_type ADD CONSTRAINT PRIMARY_51 PRIMARY KEY ( id ) ENABLE";
		jdbc.executeUpdate(sql);
		sql = "CREATE INDEX idxOSerType_OSerId ON t011_obj_serv_type ( obj_serv_id )";
		jdbc.executeUpdate(sql);
		jdbc.commit();
	}

	public void createTableT011ObjServScale(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE t011_obj_serv_scale ( id NUMBER(24,0) NOT NULL,"
				+ "  version NUMBER(10,0) DEFAULT '0' NOT NULL, obj_serv_id NUMBER(24,0),"
				+ "  line NUMBER(10,0) DEFAULT '0', scale NUMBER(10,0), resolution_ground FLOAT,"
				+ "  resolution_scan FLOAT)";
		jdbc.executeUpdate(sql);
		sql = "ALTER TABLE t011_obj_serv_scale ADD CONSTRAINT PRIMARY_50 PRIMARY KEY ( id ) ENABLE";
		jdbc.executeUpdate(sql);
		sql = "CREATE INDEX idxOSrvScal_OSrvId ON t011_obj_serv_scale ( obj_serv_id )";
		jdbc.executeUpdate(sql);
		jdbc.commit();
	}

	public void createTableSysGui(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE sys_gui ( id NUMBER(24,0) NOT NULL,"
				+ "  version NUMBER(10,0) DEFAULT '0' NOT NULL, gui_id VARCHAR2(100 CHAR) NOT NULL,"
				+ "  behaviour NUMBER(10,0) DEFAULT '-1' NOT NULL)";
		jdbc.executeUpdate(sql);
		sql = "ALTER TABLE sys_gui ADD CONSTRAINT PRIMARY_25 PRIMARY KEY ( id ) ENABLE";
		jdbc.executeUpdate(sql);
		sql = "CREATE UNIQUE INDEX gui_id ON sys_gui ( gui_id )";
		jdbc.executeUpdate(sql);
		jdbc.commit();
	}

	public void createTablesMetadata(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE object_metadata ( id NUMBER(24,0) NOT NULL,"
				+ "  version NUMBER(10,0) DEFAULT '0' NOT NULL, expiry_state NUMBER(10,0) DEFAULT '0',"
				+ "  lastexport_time VARCHAR2(17 CHAR), mark_deleted CHAR(1 CHAR) DEFAULT 'N',"
				+ "  assigner_uuid VARCHAR2(40 CHAR), assign_time VARCHAR2(17 CHAR),"
				+ "  reassigner_uuid VARCHAR2(40 CHAR), reassign_time VARCHAR2(17 CHAR))";
		jdbc.executeUpdate(sql);
		sql = "ALTER TABLE object_metadata ADD CONSTRAINT PRIMARY_11 PRIMARY KEY ( id ) ENABLE";
		jdbc.executeUpdate(sql);

		sql = "CREATE TABLE address_metadata ( id NUMBER(24,0) NOT NULL,"
				+ "  version NUMBER(10,0) DEFAULT '0' NOT NULL, expiry_state NUMBER(10,0) DEFAULT '0',"
				+ "  lastexport_time VARCHAR2(17 CHAR), mark_deleted CHAR(1 CHAR) DEFAULT 'N',"
				+ "  assigner_uuid VARCHAR2(40 CHAR), assign_time VARCHAR2(17 CHAR),"
				+ "  reassigner_uuid VARCHAR2(40 CHAR), reassign_time VARCHAR2(17 CHAR))";
		jdbc.executeUpdate(sql);
		sql = "ALTER TABLE address_metadata ADD CONSTRAINT PRIMARY_1 PRIMARY KEY ( id ) ENABLE";
		jdbc.executeUpdate(sql);
		jdbc.commit();
	}

	public void createTableSysJobInfo(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE sys_job_info ( id NUMBER(24,0) NOT NULL,"
				+ "  version NUMBER(10,0) DEFAULT '0' NOT NULL, job_type VARCHAR2(50 CHAR),"
				+ "  user_uuid VARCHAR2(40 CHAR), start_time VARCHAR2(17 CHAR),"
				+ "  end_time VARCHAR2(17 CHAR), job_details CLOB)";
		jdbc.executeUpdate(sql);
		sql = "ALTER TABLE sys_job_info ADD CONSTRAINT PRIMARY_26 PRIMARY KEY ( id ) ENABLE";
		jdbc.executeUpdate(sql);
		jdbc.commit();
	}

	public void createTableSysGenericKey(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE sys_generic_key ( id NUMBER(24,0) NOT NULL,"
				+ "  version NUMBER(10,0) DEFAULT '0' NOT NULL, key_name VARCHAR2(255 CHAR) NOT NULL,"
				+ "  value_string VARCHAR2(255 CHAR) )";
		jdbc.executeUpdate(sql);
		sql = "ALTER TABLE sys_generic_key ADD CONSTRAINT PRIMARY_24 PRIMARY KEY ( id ) ENABLE";
		jdbc.executeUpdate(sql);
		sql = "CREATE UNIQUE INDEX key_name ON sys_generic_key ( key_name ) ";
		jdbc.executeUpdate(sql);
		jdbc.commit();
	}

	public void createTableObjectUse(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE object_use ( id NUMBER(24,0) NOT NULL, " +
			"version NUMBER(10,0) DEFAULT '0' NOT NULL, obj_id NUMBER(24,0), " +
			"line NUMBER(10,0) DEFAULT '0', terms_of_use CLOB )";
		jdbc.executeUpdate(sql);
		sql = "ALTER TABLE object_use ADD CONSTRAINT PRIMARY_OBJECT_USE PRIMARY KEY ( id ) ENABLE";
		jdbc.executeUpdate(sql);
		sql = "CREATE INDEX idxObjUse_ObjId ON object_use ( obj_id )";
		jdbc.executeUpdate(sql);
		jdbc.commit();
	}
	public void createTableT011ObjServUrl(JDBCConnectionProxy jdbc) throws SQLException {
		String sql = "CREATE TABLE t011_obj_serv_url ( " +
			"id NUMBER(24,0) NOT NULL, " +
			"version NUMBER(10,0) DEFAULT '0' NOT NULL, " +
			"obj_serv_id NUMBER(24,0), " +
			"line NUMBER(10,0) DEFAULT '0', " +
			"url VARCHAR2(1024 CHAR), " +
			"description VARCHAR2(4000 CHAR))";
		jdbc.executeUpdate(sql);
		sql = "ALTER TABLE t011_obj_serv_url ADD CONSTRAINT PRIMARY_T011ObjServUrl PRIMARY KEY ( id ) ENABLE";
		jdbc.executeUpdate(sql);
		sql = "CREATE INDEX idxOSerUrl_OSerId ON t011_obj_serv_url ( obj_serv_id )";
		jdbc.executeUpdate(sql);
		jdbc.commit();
	}


	private String mapColumnTypeToSQL(ColumnType colType) {
		String sql = "";

		if (colType == ColumnType.TEXT) {
			sql = "CLOB";
		} else if (colType == ColumnType.TEXT_NO_CLOB) {
			sql = "VARCHAR2(4000 CHAR)";
		} else if (colType == ColumnType.MEDIUMTEXT) {
			sql = "CLOB";
		} else if (colType == ColumnType.VARCHAR1) {
			sql = "VARCHAR2(1 CHAR)";
		} else if (colType == ColumnType.VARCHAR50) {
			sql = "VARCHAR2(50 CHAR)";
		} else if (colType == ColumnType.VARCHAR255) {
			sql = "VARCHAR2(255 CHAR)";
		} else if (colType == ColumnType.INTEGER) {
			sql = "NUMBER(10,0)";
		} else if (colType == ColumnType.BIGINT) {
			sql = "NUMBER(24,0)";
		}

		return sql;
	}
}
