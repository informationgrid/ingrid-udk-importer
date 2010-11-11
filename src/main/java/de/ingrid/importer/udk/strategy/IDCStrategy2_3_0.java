/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 2.3.0 = SWITCH OF STRATEGY VERSION ! Strategy Version (first two digits) correlates now with InGrid project version !!!
 * <br>
 * Changes InGrid 2.3:<br>
 * - Steuerung der Klasse Geoinformation/Karte anhand des INSPIRE-Themas
 */
public class IDCStrategy2_3_0 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy2_3_0.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_2_3_0;

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		// THEN EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit (e.g. on MySQL)
		// ---------------------------------
/*
		System.out.print("  Extend datastructure...");
		extendDataStructure();
		System.out.println("done.");
*/
		// THEN PERFORM DATA MANIPULATIONS !
		// ---------------------------------
/*
		System.out.print("  Extending sys_list...");
		extendSysList();
		System.out.println("done.");
*/
		// TODO

/*
		System.out.print("  Clean up sys_list...");
		cleanUpSysList();
		System.out.println("done.");

		System.out.print("  Updating sys_gui...");
		updateSysGui();
		System.out.println("done.");
*/
		// FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause commit (e.g. on MySQL)
		// ---------------------------------
/*
		System.out.print("  Clean up datastructure...");
		cleanUpDataStructure();
		System.out.println("done.");
*/
		jdbc.commit();
		System.out.println("Update finished successfully.");
	}
}
