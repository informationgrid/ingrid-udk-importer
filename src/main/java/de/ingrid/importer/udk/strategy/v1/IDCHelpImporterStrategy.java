/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2025 wemove digital solutions GmbH
 * ==================================================
 * Licensed under the EUPL, Version 1.2 or – as soon they will be
 * approved by the European Commission - subsequent versions of the
 * EUPL (the "Licence");
 * 
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 * 
 * https://joinup.ec.europa.eu/software/page/eupl
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
package de.ingrid.importer.udk.strategy.v1;

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
import de.ingrid.importer.udk.strategy.IDCStrategy;

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

	public ImportDescriptor getImportDescriptor() {
		return importDescriptor;
	}
	public void setImportDescriptor(ImportDescriptor descriptor) {
		importDescriptor = descriptor;
	}

	public String getIDCVersion() {
		// no version ! keep current version when importing help messages !
		return null;
	}

	public IDCHelpImporterStrategy() {
		super();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.ingrid.importer.udk.strategy.IDCStrategy#execute()
	 */
	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		String entityName = "sys_gui";
		
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}
		
		pSqlStr = "INSERT INTO help_messages (id, help_id, entity_class, language,"
			+ "name, help_text, sample) VALUES "
			+ "( ?, ?, ?, ?, ?, ?, ?)";
		
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
	}


	protected void setHiLoGenerator() throws SQLException {
		sqlStr = "DELETE FROM hibernate_unique_key";
		jdbc.executeUpdate(sqlStr);

		sqlStr = "INSERT INTO hibernate_unique_key (next_hi) VALUES (" + (int)(dataProvider.getId() / Short.MAX_VALUE + 1) + ")";
		jdbc.executeUpdate(sqlStr);
	}
	
}
