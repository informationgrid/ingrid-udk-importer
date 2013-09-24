package de.ingrid.importer.udk.strategy.v34;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 3.4<p>
 * <ul>
 *   <li>Set database catalog version to final release version !
 *   <li>RELOAD ALL SYSLISTS due to removing lastModifiedSyslist entry from sys_generic_key 
 * </ul>
 */
public class IDCStrategy3_4_0_RELEASE extends IDCStrategyDefault {

//	private static Log log = LogFactory.getLog(IDCStrategy3_4_0_RELEASE.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_4_0_RELEASE;

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		// delete time stamp of last update of syslists to reload all syslists
		// (reload from initial codelist file from codelist service if no repo connected).
		// Thus we guarantee syslists are up to date !
		deleteGenericKey("lastModifiedSyslist");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}
}
