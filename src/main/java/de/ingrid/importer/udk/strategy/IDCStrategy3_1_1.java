/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Changes InGrid 3.1.1:<p>
 * - remove table t0114_env_category and according syslist 1400
 */
public class IDCStrategy3_1_1 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_1_1.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_1_1;
	
	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		// THEN PERFORM DATA MANIPULATIONS !
		// ---------------------------------

		System.out.print("  Clean up sys_list...");
		cleanUpSysList();
		System.out.println("done.");

		// FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause commit (e.g. on MySQL)
		// ---------------------------------

		System.out.print("  Clean up datastructure...");
		cleanUpDataStructure();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void cleanUpSysList() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Clean up sys_list...");
		}
		
		int numDeleted;
		if (log.isInfoEnabled()) {
			log.info("Delete syslist 1400 (environmental categories)...");
		}

		sqlStr = "DELETE FROM sys_list where lst_id = 1400";
		numDeleted = jdbc.executeUpdate(sqlStr);
		if (log.isDebugEnabled()) {
			log.debug("Deleted " + numDeleted +	" entries (all languages).");
		}
		
		if (log.isInfoEnabled()) {
			log.info("Clean up sys_list... done");
		}
	}

	private void cleanUpDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Drop table 't0114_env_category' ...");
		}
		jdbc.getDBLogic().dropTable("t0114_env_category", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure... done");
		}
	}
}
