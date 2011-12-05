/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;

/**
 * Independent Strategy !
 * Change several fields from VARCHAR 255 to TEXT !
 */
public class IDCStrategy3_1_1_fixVarchar extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_1_1_fixVarchar.class);

    /**
     * Deliver NO Version, this strategy should NOT trigger a strategy workflow (of missing former
     * versions) and can be executed on its own !
     * NOTICE: BUT may be executed in workflow (part of workflow array) !
     * @see de.ingrid.importer.udk.strategy.IDCStrategy#getIDCVersion()
     */
	public String getIDCVersion() {
		return null;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit (e.g. on MySQL)
		// ---------------------------------
		System.out.print("  Extend datastructure...");
		extendDataStructure();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void extendDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Extending datastructure -> CAUSES COMMIT ! ...");
		}

		// change VARCHAR 255 to TEXT !
		List<String[]> tableColumns = new ArrayList<String[]>();
		tableColumns.add(new String[] { "t011_obj_geo_supplinfo", "feature_type" });
		tableColumns.add(new String[] { "t011_obj_literature", "author" });
		tableColumns.add(new String[] { "t011_obj_literature", "publisher" });
		tableColumns.add(new String[] { "t011_obj_literature", "publish_loc" });
		tableColumns.add(new String[] { "t011_obj_literature", "loc" });
		tableColumns.add(new String[] { "t011_obj_literature", "doc_info" });
		tableColumns.add(new String[] { "t011_obj_literature", "publishing" });
		tableColumns.add(new String[] { "t011_obj_data", "base" });
		
		for (String[] tC : tableColumns) {
			if (log.isInfoEnabled()) {
				log.info("Change field type of '" + tC[0] + "." + tC[1] + "' to TEXT ...");
			}
			jdbc.getDBLogic().modifyColumn(tC[1], ColumnType.TEXT_NO_CLOB, tC[0], false, jdbc);
		}

		if (log.isInfoEnabled()) {
			log.info("Extending datastructure... done");
		}
	}
}
