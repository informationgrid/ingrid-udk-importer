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
		return;
	}

	public void createTableObjectConformity(JDBCConnectionProxy jdbc) throws SQLException {
		// first create table
		String sql = "CREATE TABLE object_conformity(id BIGINT NOT NULL, " +
			"version INTEGER NOT NULL DEFAULT 0, obj_id BIGINT, specification VARCHAR(255),	" +
			"degree_key INTEGER, degree_value VARCHAR(255),	PRIMARY KEY (id), " +
			"INDEX idxObjConf_ObjId (obj_id ASC)) TYPE=InnoDB;";
		jdbc.executeUpdate(sql);
	}
}
