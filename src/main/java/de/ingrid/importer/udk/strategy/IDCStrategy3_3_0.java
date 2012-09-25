/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;

/**
 * <p>
 * Changes InGrid 3.3<p>
 * <ul>
 *   <li>Add sys_list.data column, see INGRID33-5
 * </ul>
 */
public class IDCStrategy3_3_0 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_3_0.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_3_0;

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

		// THEN PERFORM DATA MANIPULATIONS !
		// ---------------------------------

		// FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause commit (e.g. on MySQL)
		// ---------------------------------

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void extendDataStructure() throws Exception {
		log.info("\nExtending datastructure -> CAUSES COMMIT ! ...");

		log.info("Add column 'data' to table 'sys_list' ...");
		jdbc.getDBLogic().addColumn("data", ColumnType.TEXT_NO_CLOB, "sys_list", false, null, jdbc);

		log.info("Extending datastructure... done\n");
	}

}
