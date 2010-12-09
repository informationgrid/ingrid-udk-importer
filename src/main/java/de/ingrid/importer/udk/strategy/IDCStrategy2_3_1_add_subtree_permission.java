/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * <p>
 * Changes InGrid 2.3 for NI:<p>
 * - Add new subtree permission type
 */
public class IDCStrategy2_3_1_add_subtree_permission extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy2_3_1_add_subtree_permission.class);

    /**
     * Deliver NO Version, this strategy should NOT trigger a strategy workflow and
     * can be executed on its own ! NOTICE: BUT may be executed in workflow (part of workflow array) !
     * @see de.ingrid.importer.udk.strategy.IDCStrategy#getIDCVersion()
     */
    public String getIDCVersion() {
        return null;
    }

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		System.out.print("  Add subtree permission...");
		addTreewritePermission();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void addTreewritePermission() throws Exception {
        jdbc.executeUpdate("INSERT INTO permission (id, class_name, name, action) VALUES ("
                + getNextId() + ", 'IdcEntityPermission', 'entity', 'write-subtree')");
	}

}
