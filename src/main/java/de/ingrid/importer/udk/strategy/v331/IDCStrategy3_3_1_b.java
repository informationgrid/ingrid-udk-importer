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
 *   <li>Add columns t017_url_ref.datatype_key / datatype_value, see REDMINE-118
 * </ul>
 * Writes NEW Catalog Schema Version to catalog !
 */
public class IDCStrategy3_3_1_b extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_3_1_b.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_3_1_b;

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

		log.info("Add column 'datatype_key' to table 't017_url_ref' ...");
		jdbc.getDBLogic().addColumn("datatype_key", ColumnType.INTEGER, "t017_url_ref", false, null, jdbc);
		log.info("Add column 'datatype_value' to table 't017_url_ref' ...");
		jdbc.getDBLogic().addColumn("datatype_value", ColumnType.VARCHAR255, "t017_url_ref", false, null, jdbc);


		log.info("Extending datastructure... done\n");
	}
}
