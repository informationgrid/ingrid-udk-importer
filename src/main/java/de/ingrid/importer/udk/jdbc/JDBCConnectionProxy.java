/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2015 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.ImportDescriptor;

/**
 * @author Administrator
 * 
 */
public class JDBCConnectionProxy {

	private ImportDescriptor descriptor = null;
	private Connection fConnection = null;
	private DBLogic dbLogic = null;

	private static Log log = LogFactory.getLog(JDBCConnectionProxy.class);

	public JDBCConnectionProxy(ImportDescriptor descriptor) throws Exception {
		this.descriptor = descriptor;
		connectToDB();
	}

	private void connectToDB() throws Exception {
		try {
			if (log.isDebugEnabled()) {
				log.debug("Connecting to database...");
			}

			if (descriptor.getDbDriver().indexOf("oracle") != -1) {
				dbLogic = new OracleLogic();
			} else if (descriptor.getDbDriver().indexOf("microsoft") != -1) {
				dbLogic = new MSSQLLogic();
			} else if (descriptor.getDbDriver().indexOf("mysql") != -1) {
				dbLogic = new MySQLLogic();
			} else {
				log.error("Unsupported DB driver: " + descriptor.getDbDriver());
				throw new RuntimeException("Unsupported DB driver: " + descriptor.getDbDriver());
			}

			Class.forName(descriptor.getDbDriver());
			String url=(descriptor.getDbURL());
			Properties p = new Properties();
			p.setProperty("user",descriptor.getDbUser());
			p.setProperty("password",descriptor.getDbPass());
//			p.setProperty("jdbcCompliantTruncation","false"); //new line
			if (log.isDebugEnabled()) {
				log.debug("Connecting to database, url='" + url + "'");
			}
			fConnection = DriverManager.getConnection(url,p);

			String dbSchema = descriptor.getDbSchema();

			if (dbLogic != null) {
				dbLogic.setSchema(fConnection, dbSchema);
			}
			if (log.isDebugEnabled()) {
				log.debug("Connecting to database... success.");
			}
			

		} catch (SQLException e) {
			log.error("Can't connect to database! Please check your connection parameters.", e);
			throw new RuntimeException("Can't connect to database! Please check your connection parameters.");

		} catch (ClassNotFoundException e) {
			log.error("Can't connect to database! Error while getting/instantiating JDBC driver '" + descriptor.getDbDriver() + "'.", e);
			throw new RuntimeException("Can't connect to database! Error while getting/instantiating JDBC driver '" + descriptor.getDbDriver() + "'.");
		} catch (Exception e) {
			log.error("Can't connect to database!", e);
			throw new RuntimeException("Can't connect to database!");
		}
	}

	public void setAutoCommit(boolean autoCommit) throws SQLException {
		fConnection.setAutoCommit(autoCommit);
	}

	public void commit() throws SQLException {
		fConnection.commit();
	}

	public void rollback() throws SQLException {
		fConnection.rollback();
	}

	public void close() throws SQLException {
		fConnection.close();
	}

	public int executeUpdate(String sql) throws SQLException {
		Statement statement = fConnection.createStatement();
		int result = statement.executeUpdate(sql);
		statement.close();
		return result;
	}

	public ResultSet executeQuery(String sql, Statement statement) throws SQLException {
		return statement.executeQuery(sql);
	}
	
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return fConnection.prepareStatement(sql);
	}
	
	public Statement createStatement() throws SQLException {
		return fConnection.createStatement();
	}

	public DBLogic getDBLogic() {
		return dbLogic;
	}

	public String getCatalog() throws SQLException {
		String myCatalog = fConnection.getCatalog();
		if (myCatalog == null && isOracle()) {
			myCatalog = descriptor.getDbUser();
		}

		return myCatalog;
	}

	public boolean isOracle() {
		return OracleLogic.class.isAssignableFrom(getDBLogic().getClass());
	}
}
