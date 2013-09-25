/**
 * 
 */
package de.ingrid.importer.udk.strategy.v331;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 3.3.1<p>
 * <ul>
 *   <li>Add column t01_object.is_open_data, see REDMINE-128
 *   <li>Add table object_open_data_category, see REDMINE-128
 * </ul>
 * Writes NEW Catalog Schema Version to catalog !
 */
public class IDCStrategy3_3_1_a extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_3_1_a.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_3_1_a;

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		// THEN EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit (e.g. on MySQL)
		// ---------------------------------

		System.out.print("  Extend datastructure...");
		extendDataStructure();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void extendDataStructure() throws Exception {
		log.info("\nExtending datastructure -> CAUSES COMMIT ! ...");

		log.info("Add column 'is_open_data' to table 't01_object' ...");
		jdbc.getDBLogic().addColumn("is_open_data", ColumnType.VARCHAR1, "t01_object", false, "'N'", jdbc);

		log.info("Create table 'object_open_data_category'...");
		jdbc.getDBLogic().createTableObjectOpenDataCategory(jdbc);

		log.info("Extending datastructure... done\n");
	}
}
