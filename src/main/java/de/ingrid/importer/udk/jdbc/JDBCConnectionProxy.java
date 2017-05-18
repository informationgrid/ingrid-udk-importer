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
/**
 * 
 */
package de.ingrid.importer.udk.jdbc;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;

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


	private void setSchema() throws Exception {
        this.dbLogic.setSchema(this.fConnection, this.descriptor.getDbSchema());	    
	}

	private void connectToDB() throws Exception {
	    String url = null;
	    Properties p = null;
	    
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
            } else if (descriptor.getDbDriver().indexOf("postgresql") != -1) {
                dbLogic = new PostgreSQLLogic();
			} else {
				log.error("Unsupported DB driver: " + descriptor.getDbDriver());
				throw new RuntimeException("Unsupported DB driver: " + descriptor.getDbDriver());
			}

			Class.forName(descriptor.getDbDriver());
			url = descriptor.getDbURL();
			p = new Properties();
			p.setProperty("user",descriptor.getDbUser());
			p.setProperty("password",descriptor.getDbPass());
//			p.setProperty("jdbcCompliantTruncation","false"); //new line
			if (log.isDebugEnabled()) {
			    log.debug("Connecting to database, url='" + url + "'");
			}
			fConnection = DriverManager.getConnection(url,p);
			
			setSchema();

            if (log.isDebugEnabled()) {
                log.debug("Connecting to database... success.");
            }

		} catch (SQLException e) {
		    if (e.getMessage().contains( "Unknown database" )) {
		        try {
    		        createDatabase(p);
    		        fConnection = DriverManager.getConnection(url, p);
    		        this.dbLogic.importFileToDatabase( this );
    		        String msg = "\n\nCreated new database and imported initial version: " + url;
    	            System.out.println(msg);
    	            log.info(msg);

    	            setSchema();

		        } catch (SQLException e2) {
		            log.error("Can't create or connect to database! Please check your connection parameters.", e2);
	                throw new RuntimeException("Can't create or connect to database! Please check your connection parameters.");
		        }
		    } else {
    			log.error("Can't connect to database! Please check your connection parameters.", e);
    			throw new RuntimeException("Can't connect to database! Please check your connection parameters.");
		    }

		} catch (ClassNotFoundException e) {
			log.error("Can't connect to database! Error while getting/instantiating JDBC driver '" + descriptor.getDbDriver() + "'.", e);
			throw new RuntimeException("Can't connect to database! Error while getting/instantiating JDBC driver '" + descriptor.getDbDriver() + "'.");
		} catch (Exception e) {
			log.error("Can't connect to database!", e);
			throw new RuntimeException("Can't connect to database!");
		}
	}

	private void createDatabase(Properties p) throws SQLException {
	    String url = descriptor.getDbURL();
        int pos = url.lastIndexOf( "/" );
        String dbUrl = url.substring( 0, pos );
        String dbName = url.substring( pos + 1 );
        
        Connection dbConnection = DriverManager.getConnection( dbUrl, p);
        
        this.dbLogic.createDatabase( this, dbConnection, dbName, descriptor.getDbUser() );
        
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
		return executeUpdate( fConnection, sql );
	}
	
	public int executeUpdate(Connection connection, String sql) throws SQLException {
	    Statement statement = connection.createStatement();
	    int result = statement.executeUpdate(sql);
	    statement.close();
	    return result;
	}

	public ResultSet executeQuery(String sql, Statement statement) throws SQLException {
		return statement.executeQuery(sql);
	}
	
	public void importFile(InputStream importFileStream) throws FileNotFoundException, SQLException {
        Scanner s = new Scanner(importFileStream);
//        s.useDelimiter("(;(\r)?\n)|(--\n)");
        s.useDelimiter("(;(\r)?\n)|((\r)?\n)?(--)?.*(--(\r)?\n)");
        Statement st = null;
        try
        {
            st = fConnection.createStatement();
            while (s.hasNext())
            {
                String line = s.next();
                if (line.startsWith("/*!") && line.endsWith("*/"))
                {
                    int i = line.indexOf(' ');
                    line = line.substring(i + 1, line.length() - " */".length());
                }

                if (line.trim().length() > 0)
                {
                    st.execute(line);
                }
            }
        }
        finally
        {
            if (st != null) st.close();
            if (s != null) s.close();
        }
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
