/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;

/**
 * IGC Update:
 * - ArcGIS Import
 */
public class IDCStrategy1_0_6 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy1_0_6.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_106;

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// then write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		// THEN EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit (e.g. on MySQL)
		// ---------------------------------
		System.out.print("  Extend datastructure...");
		extendDataStructure();
		System.out.println("done.");

		// THEN PERFORM DATA MANIPULATIONS !
		// ---------------------------------


		// FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause commit (e.g. on MySQL)
		// ---------------------------------


		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void extendDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Extending datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Change field type of 't01_object.dataset_alternate_name' to VARCHAR(255) ...");
		}
		jdbc.getDBLogic().modifyColumn("dataset_alternate_name", ColumnType.VARCHAR255, "t01_object", false, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Change field type of 't015_legist.legist_value' to VARCHAR(255) ...");
		}
		jdbc.getDBLogic().modifyColumn("legist_value", ColumnType.VARCHAR255, "t015_legist", false, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Extending datastructure... done");
		}
	}
}
