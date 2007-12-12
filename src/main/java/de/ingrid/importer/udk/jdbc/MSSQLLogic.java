package de.ingrid.importer.udk.jdbc;

import java.sql.Connection;

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
		return;

	}

}
