/**
 * 
 */
package de.ingrid.importer.udk;

import java.util.ArrayList;

/**
 * @author joachim
 * 
 */
public class ImportDescriptor {

	/** * Database parameter: DB Url */
	private String dbURL = "";

	/** * Database parameter: JDBC driver class */
	private String dbDriver = "";

	/** * Database parameter: DB user name */
	private String dbUser = "";

	/** * Database parameter: DB user password */
	private String dbPass = "";

	/** * Database Name: Oracle / MySQL / MSSQL */
	private String dbName = "";

	/** * Datenbankschema/-Katalog */
	private String dbSchema = "";

	private String configurationFile = "conf/descriptor.properties";

	private String idcVersion = null;

	/**
	 * ArrayList to store import files
	 * 
	 */
	private ArrayList<String> files = new ArrayList<String>();

	/**
	 * @return the dbDriver
	 */
	public String getDbDriver() {
		return dbDriver;
	}

	/**
	 * @param dbDriver
	 *            the dbDriver to set
	 */
	public void setDbDriver(String dbDriver) {
		this.dbDriver = dbDriver;
	}

	/**
	 * @return the dbName
	 */
	public String getDbName() {
		return dbName;
	}

	/**
	 * @param dbName
	 *            the dbName to set
	 */
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	/**
	 * @return the dbPass
	 */
	public String getDbPass() {
		return dbPass;
	}

	/**
	 * @param dbPass
	 *            the dbPass to set
	 */
	public void setDbPass(String dbPass) {
		this.dbPass = dbPass;
	}

	/**
	 * @return the dbSchema
	 */
	public String getDbSchema() {
		return dbSchema;
	}

	/**
	 * @param dbSchema
	 *            the dbSchema to set
	 */
	public void setDbSchema(String dbSchema) {
		this.dbSchema = dbSchema;
	}

	/**
	 * @return the dbURL
	 */
	public String getDbURL() {
		return dbURL;
	}

	/**
	 * @param dbURL
	 *            the dbURL to set
	 */
	public void setDbURL(String dbURL) {
		this.dbURL = dbURL;
	}

	/**
	 * @return the dbUser
	 */
	public String getDbUser() {
		return dbUser;
	}

	/**
	 * @param dbUser
	 *            the dbUser to set
	 */
	public void setDbUser(String dbUser) {
		this.dbUser = dbUser;
	}

	/**
	 * @return the files
	 */
	public ArrayList<String> getFiles() {
		return files;
	}

	/**
	 * @param files
	 *            the files to set
	 */
	public void setFiles(ArrayList<String> files) {
		this.files = files;
	}

	/**
	 * @return the configurationFile
	 */
	public String getConfigurationFile() {
		return configurationFile;
	}

	/**
	 * @param configurationFile
	 *            the configurationFile to set
	 */
	public void setConfigurationFile(String configurationFile) {
		this.configurationFile = configurationFile;
	}

	/**
	 * @return the idcVersion
	 */
	public String getIdcVersion() {
		return idcVersion;
	}

	/**
	 * @param idcVersion
	 *            the idcVersion to set
	 */
	public void setIdcVersion(String udkDbVersion) {
		this.idcVersion = udkDbVersion;
	}

}
