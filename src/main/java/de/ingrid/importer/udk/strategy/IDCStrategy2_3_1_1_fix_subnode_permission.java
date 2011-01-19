/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * <p>
 * FIX InGrid 2.3 for NI:<p>
 * - Rename "write_subtree" permission to "write-subnode"
 */
public class IDCStrategy2_3_1_1_fix_subnode_permission extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy2_3_1_1_fix_subnode_permission.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_2_3_1_1_FIX_SUBNODE_PERMISSION;

    public String getIDCVersion() {
		return MY_VERSION;
    }

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		System.out.print("  Fix subnode permission...");
		fixPermission();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void fixPermission() throws Exception {
		int numUpdated = jdbc.executeUpdate(
			"UPDATE permission SET " +
				"action = 'write-subnode' " +
			"where " +
				"action = 'write-subtree'");
		if (log.isDebugEnabled()) {
			log.debug("Rename permission 'write-subtree' to 'write-subnode': updated " + numUpdated + " records");
		}
	}
}
