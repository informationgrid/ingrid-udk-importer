/**
 * 
 */
package de.ingrid.importer.udk.strategy.v33;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 3.3<p>
 * <ul>
 *   <li>increase t017_url_ref.url_link, see INGRID33-27
 * </ul>
 */
public class IDCStrategy3_3_0_b extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_3_0_b.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_3_0_b;

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

		log.info("Change column type t017_url_ref.url_link to TEXT_NO_CLOB ...");
		jdbc.getDBLogic().modifyColumn("url_link", ColumnType.TEXT_NO_CLOB, "t017_url_ref", false, jdbc);

		log.info("Extending datastructure... done\n");
	}
}
