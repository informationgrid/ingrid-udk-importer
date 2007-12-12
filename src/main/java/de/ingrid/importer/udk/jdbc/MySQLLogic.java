package de.ingrid.importer.udk.jdbc;

import java.sql.Connection;

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

}
