/**
 * 
 */
package de.ingrid.importer.udk.strategy.v341;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 3.4.1<p>
 * <ul>
 *   <li>increase t011_obj_serv_op_connpoint.connect_point, see https://redmine.wemove.com/issues/508
 * </ul>
 */
public class IDCStrategy3_4_1_a extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_4_1_a.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_4_1_a;

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

		log.info("Change column type t011_obj_serv_op_connpoint.connect_point to TEXT_NO_CLOB ...");
		jdbc.getDBLogic().modifyColumn("connect_point", ColumnType.TEXT_NO_CLOB, "t011_obj_serv_op_connpoint", false, jdbc);

		log.info("Extending datastructure... done\n");
	}
}
