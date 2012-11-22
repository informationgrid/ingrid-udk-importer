/**
 * 
 */
package de.ingrid.importer.udk.strategy.v1;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * Change column names for running on Oracle !
 */
public class IDCStrategy1_0_7 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy1_0_7.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_107;

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
			log.info("Manipulate datastructure -> CAUSES COMMIT ! ...");
		}

		// no operations on oracle, has already correct structure
		if (!jdbc.isOracle()) {
			if (log.isInfoEnabled()) {
				log.info("Rename 'object_comment.comment' column to 'comment_' ...");
			}
			jdbc.getDBLogic().renameColumn("comment", "comment_", ColumnType.TEXT, "object_comment", false, jdbc);

			if (log.isInfoEnabled()) {
				log.info("Rename 'address_comment.comment' column to 'comment_' ...");
			}
			jdbc.getDBLogic().renameColumn("comment", "comment_", ColumnType.TEXT, "address_comment", false, jdbc);

		} else {
			if (log.isInfoEnabled()) {
				log.info("Oracle database ! no renaming of columns, already in base schema");
			}
		}

		if (log.isInfoEnabled()) {
			log.info("Manipulate datastructure... done");
		}
	}
}
