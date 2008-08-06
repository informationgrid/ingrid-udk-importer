/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Administrator
 * 
 */
public class IDCStrategy1_0_3 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy1_0_3.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_103;
	
	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {

		try {
			jdbc.setAutoCommit(false);

			// write version !
			setGenericKey(KEY_IDC_VERSION, MY_VERSION);

			System.out.print("  Updating sys_list...");
			// must be processed first because other methods depend on that data
			updateSysList();
			System.out.println("done.");
			// Updating of HI/LO table not necessary anymore ! is checked and updated when fetching next id
			// via getNextId() ...
			jdbc.commit();
			System.out.println("Update finished successfully.");

		} catch (Exception e) {
			System.out.println("Error executing strategy ! See log file for further information.");
			log.error("Error executing strategy!", e);
			throw e;
		}
	}

	protected void updateSysList() throws Exception {

		if (log.isInfoEnabled()) {
			log.info("Updating sys_list...");
		}

		int lstId = 6000;

		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" (Grad der Konformitaet)...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = " + lstId;
		jdbc.executeUpdate(sqlStr);

		// insert new syslist
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 1, 'de', 'konform: Die Datenquelle ist vollständig konform zur zitierten Spezifikation', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 2, 'de', 'nicht konform: Die Datenquelle ist nicht konform zur zitierten Spezifikation', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 3, 'de', 'nicht evaluiert: Die Konformität der Datenquelle wurde noch nicht evaluiert', 0, 'Y');");

		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" (Grad der Konformitaet)... done");
		}
	}
}
