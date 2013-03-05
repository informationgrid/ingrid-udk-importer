package de.ingrid.importer.udk.strategy.v33;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 3.3<p>
 * <ul>
 *   <li>Set database catalog version to final release version !
 * </ul>
 */
public class IDCStrategy3_3_0_RELEASE extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_3_0_RELEASE.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_3_0_RELEASE;

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}
}
