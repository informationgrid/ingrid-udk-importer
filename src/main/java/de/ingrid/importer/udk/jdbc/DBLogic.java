package de.ingrid.importer.udk.jdbc;

import java.sql.Connection;

/**
 * This is the interface for all DBLogic implementations
 * 
 * @author joachim@wmeove.com
 */
public interface DBLogic {

	void setSchema(Connection connection, String schema) throws Exception;

}
