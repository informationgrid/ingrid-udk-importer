/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.ImportDescriptor;
import de.ingrid.importer.udk.jdbc.JDBCConnectionProxy;
import de.ingrid.importer.udk.jdbc.JDBCHelper;
import de.ingrid.importer.udk.provider.DataProvider;
import de.ingrid.importer.udk.provider.Row;

/**
 * @author Administrator
 * 
 */
public class IDCHelpImporterStrategy implements IDCStrategy {

	protected DataProvider dataProvider = null;

	protected ImportDescriptor importDescriptor = null;

	protected JDBCConnectionProxy jdbc = null;

	String sqlStr = null;

	String pSqlStr = null;

	private static Log log = LogFactory.getLog(IDCHelpImporterStrategy.class);

	public void setDataProvider(DataProvider data) {
		dataProvider = data;
	}

	public void setJDBCConnectionProxy(JDBCConnectionProxy jdbc) {
		this.jdbc = jdbc;
	}

	public void setImportDescriptor(ImportDescriptor descriptor) {
		importDescriptor = descriptor;
	}

	public IDCHelpImporterStrategy() {
		super();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.ingrid.importer.udk.strategy.IDCStrategy#execute()
	 */
	public void execute() {
		
		try {
		
			jdbc.setAutoCommit(false);

			String entityName = "sys_gui";
			
			if (log.isInfoEnabled()) {
				log.info("Importing " + entityName + "...");
			}
			
			pSqlStr = "INSERT INTO help_messages (id, help_id, entity_class, language,"
				+ "name, help_text, sample) VALUES "
				+ "( ?, ?, ?, ?, ?, ?, ?);";
			
			PreparedStatement p = jdbc.prepareStatement(pSqlStr);
	
			sqlStr = "DELETE FROM help_messages";
			jdbc.executeUpdate(sqlStr);
			
			for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
				Row row = i.next();
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				JDBCHelper.addInteger(p, cnt++, row.getInteger("gui_id")); // help_id
				JDBCHelper.addInteger(p, cnt++, row.getInteger("class_id")); // entity_class
				p.setString(cnt++, "de"); // language
				p.setString(cnt++, row.get("name")); // name
				p.setString(cnt++, row.get("help")); // help_text
				p.setString(cnt++, row.get("bsp")); // sample
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
				
			}
			
			jdbc.commit();
		} catch (Exception e) {
			System.out.println("Error executing sql! See log file for further information.");
			log.error("Error executing SQL!", e);
			if (jdbc != null) {
				try {
					jdbc.rollback();
				} catch (SQLException e1) {
					log.error("Error rolling back transaction!", e);
				}
			}
		} finally {
			if (jdbc != null) {
				try {
					jdbc.close();
				} catch (SQLException e) {
					log.error("Error closing DB connection!", e);
				}
			}
		}		
	}


	protected void setHiLoGenerator() throws SQLException {
		sqlStr = "DELETE FROM hibernate_unique_key";
		jdbc.executeUpdate(sqlStr);

		sqlStr = "INSERT INTO hibernate_unique_key (next_hi) VALUES (" + (int)(dataProvider.getId() / Short.MAX_VALUE + 1) + ")";
		jdbc.executeUpdate(sqlStr);
	}
	
}