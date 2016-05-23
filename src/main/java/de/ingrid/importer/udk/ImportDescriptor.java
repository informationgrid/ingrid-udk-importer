/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2016 wemove digital solutions GmbH
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
/**
 * 
 */
package de.ingrid.importer.udk;

import java.util.ArrayList;
import java.util.List;

/**
 * @author joachim
 * 
 */
public class ImportDescriptor {

	/** Database parameter: DB Url */
	private String dbURL = "";

	/** Database parameter: JDBC driver class */
	private String dbDriver = "";

	/** Database parameter: DB user name */
	private String dbUser = "";

	/** Database parameter: DB user password */
	private String dbPass = "";

	/** Database Name: Oracle / MySQL / MSSQL */
	private String dbName = "";

	/** Datenbankschema/-Katalog */
	private String dbSchema = "";

	private String configurationFile = "conf/descriptor.properties";

	private String idcVersion = null;
	private String idcCatalogueLanguage = null;
	private String idcEmailDefault = null;
	private String idcProfileFileName = null;

	private String idcCatalogueName = null;
	private String idcPartnerName = null;
	private String idcProviderName = null;
	private String idcCatalogueCountry = null;

	/** ArrayList to store import files */
	private List<String> files = new ArrayList<String>();

	public String getDbDriver() {
		return dbDriver;
	}
	public void setDbDriver(String dbDriver) {
		this.dbDriver = dbDriver;
	}

	public String getDbName() {
		return dbName;
	}
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getDbPass() {
		return dbPass;
	}
	public void setDbPass(String dbPass) {
		this.dbPass = dbPass;
	}

	public String getDbSchema() {
		return dbSchema;
	}
	public void setDbSchema(String dbSchema) {
		this.dbSchema = dbSchema;
	}

	public String getDbURL() {
		return dbURL;
	}
	public void setDbURL(String dbURL) {
		this.dbURL = dbURL;
	}

	public String getDbUser() {
		return dbUser;
	}
	public void setDbUser(String dbUser) {
		this.dbUser = dbUser;
	}

	public List<String> getFiles() {
		return files;
	}
	public void setFiles(List<String> files) {
		this.files = files;
	}

	public String getConfigurationFile() {
		return configurationFile;
	}
	public void setConfigurationFile(String configurationFile) {
		this.configurationFile = configurationFile;
	}

	public String getIdcVersion() {
		return idcVersion;
	}
	public void setIdcVersion(String udkDbVersion) {
		this.idcVersion = udkDbVersion;
	}

	public String getIdcCatalogueLanguage() {
		return idcCatalogueLanguage;
	}
	public void setIdcCatalogueLanguage(String idcCatalogueLanguage) {
		this.idcCatalogueLanguage = idcCatalogueLanguage;
	}

	public String getIdcEmailDefault() {
		return idcEmailDefault;
	}
	public void setIdcEmailDefault(String idcEmailDefault) {
		this.idcEmailDefault = idcEmailDefault;
	}

	public String getIdcProfileFileName() {
		return idcProfileFileName;
	}
	public void setIdcProfileFileName(String idcProfileFileName) {
		this.idcProfileFileName = idcProfileFileName;
	}

	public String getIdcCatalogueName() {
		return idcCatalogueName;
	}
	public void setIdcCatalogueName(String idcCatalogueName) {
		this.idcCatalogueName = idcCatalogueName;
	}

	public String getIdcPartnerName() {
		return idcPartnerName;
	}
	public void setIdcPartnerName(String idcPartnerName) {
		this.idcPartnerName = idcPartnerName;
	}

	public String getIdcProviderName() {
		return idcProviderName;
	}
	public void setIdcProviderName(String idcProviderName) {
		this.idcProviderName = idcProviderName;
	}

	public String getIdcCatalogueCountry() {
		return idcCatalogueCountry;
	}
	public void setIdcCatalogueCountry(String idcCatalogueCountry) {
		this.idcCatalogueCountry = idcCatalogueCountry;
	}
}
