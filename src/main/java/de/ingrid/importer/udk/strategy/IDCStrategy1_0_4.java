/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;

/**
 * IGC Update: IMPORT/EXPORT (sys_job_info) etc. 
 * 
 * @author martin
 */
public class IDCStrategy1_0_4 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy1_0_4.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_104;

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// SPECIAL: first update structure of generic key table !!! has changed !
		// NOTICE: causes commit (e.g. on MySQL)
		System.out.print("  Recreate sys_generic_key table (new structure)...");
		recreateGenericKeyDataStructure();
		System.out.println("done.");

		// then write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		// THEN EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit (e.g. on MySQL)
		System.out.print("  Extend datastructure...");
		extendDataStructure();
		System.out.println("done.");

		// THEN PERFORM DATA MANIPULATIONS !

		// Updating of HI/LO table not necessary anymore ! is checked and updated when fetching next id
		// via getNextId() ...

		// FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause commit (e.g. on MySQL)
		System.out.print("  Clean up datastructure...");
		cleanUpDataStructure();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	protected void recreateGenericKeyDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Recreating table sys_generic_key -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Drop table 'sys_generic_key' ...");
		}
		jdbc.getDBLogic().dropTable("sys_generic_key", jdbc);


		if (log.isInfoEnabled()) {
			log.info("Recreate table 'sys_generic_key' with new structure ...");
		}
		jdbc.getDBLogic().createTableSysGenericKey(jdbc);
		
		if (log.isInfoEnabled()) {
			log.info("Recreating table sys_generic_key... done");
		}
	}
	
	protected void extendDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Extending datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Create table 'sys_job_info' ...");
		}
		jdbc.getDBLogic().createTableSysJobInfo(jdbc);
		
		if (log.isInfoEnabled()) {
			log.info("Change field type of 't011_obj_geo.datasource_uuid' from TEXT to VARCHAR(255) ...");
		}
		jdbc.getDBLogic().modifyColumn("datasource_uuid", ColumnType.VARCHAR255, "t011_obj_geo", false, jdbc);
		

		if (log.isInfoEnabled()) {
			log.info("Extending datastructure... done");
		}
	}
	
	protected void cleanUpDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Drop table 'sys_export' ...");
		}
		jdbc.getDBLogic().dropTable("sys_export", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure... done");
		}
	}
}
