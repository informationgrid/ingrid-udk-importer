/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import de.ingrid.importer.udk.ImportDescriptor;
import de.ingrid.importer.udk.jdbc.JDBCConnectionProxy;
import de.ingrid.importer.udk.provider.DataProvider;

/**
 * @author Administrator
 * 
 */
public interface IDCStrategy {
	
	/** Key for sys_generic_key table to set/extract version of idc schema. */
	static String KEY_IDC_VERSION = "IDC_VERSION";

	// ALL VERSIONS/STRATEGIES EXECUTED IN STRATEGY WORKFLOW ! 

	/** initial version/strategy */
	static String VALUE_IDC_VERSION_102 = "1.0.2";
	/** FIX strategy (writes NO version) for fixing wrong syslists. ALSO INTEGRATED IN STRATEGY WORKFLOW */
	static String VALUE_IDC_FIX_SYSLIST = "1.0.2_fix_syslist_100_101";
	/** SNS Spatial Type Update */
	static String VALUE_IDC_VERSION_102_SNS_SPATIAL_TYPE = "1.0.2_sns_spatial_type";
	/** INSPIRE update version/strategy */
	static String VALUE_IDC_VERSION_103 = "1.0.3";
	/** IMPORT/EXPORT etc. version/strategy -> InGrid 2.0 internal release ! */
	static String VALUE_IDC_VERSION_104 = "1.0.4";
	/** Update INSPIRE themes version/strategy -> can be executed on its own ! */
	static String VALUE_IDC_VERSION_104_FIX_INSPIRE_THEMES = "1.0.4_fix_inspire_themes";
	/** Official InGrid 2.0 release with fixes, addons */
	static String VALUE_IDC_VERSION_105 = "1.0.5";
	/** Update country codelist to all countries -> can be executed on its own ! */
	static String VALUE_IDC_VERSION_105_FIX_COUNTRY_CODELIST = "1.0.5_fixCountryCodelist";
	/** Extend all VARCHAR to 255 */
	static String VALUE_IDC_VERSION_106 = "1.0.6";

	/** Order of strategies to execute to obtain most recent IGC.
	 * Contains all according versions/strategies in ascending order.
	 * NOTICE: THESE VERSIONS ARE MAPPED TO STRATEGIES IN STRATEGY FACTORY !!! */
	static String[] STRATEGY_WORKFLOW = new String[] {
		VALUE_IDC_VERSION_102,
		VALUE_IDC_FIX_SYSLIST,
		VALUE_IDC_VERSION_102_SNS_SPATIAL_TYPE,
		VALUE_IDC_VERSION_103,

		// Syslist csv have to be imported till version 104 ! in 104 and afterwards syslists are updated via java code !

		VALUE_IDC_VERSION_104,
		VALUE_IDC_VERSION_104_FIX_INSPIRE_THEMES,
		// InGrid 2.0 Release (including post fixes, addons)
		VALUE_IDC_VERSION_105,
		// AT THE MOMENT do not execute this one in workflow to avoid execution when 106 is executed.
		// This way we avoid conflicts with possible user changes of country list (may have been executed in between via IGE).
		// MAY BE ADDED AGAIN when 106 was executed on all IGCs to be part of FULL workflow again.
//		VALUE_IDC_VERSION_105_FIX_COUNTRY_CODELIST,
		VALUE_IDC_VERSION_106,
	};
		
	public void execute() throws Exception;

	public void setDataProvider(DataProvider data);

	public void setImportDescriptor(ImportDescriptor descriptor);

	public void setJDBCConnectionProxy(JDBCConnectionProxy jdbc);
	
	/**
	 * Get the IDC Version generated by this strategy (written into IDC catalog).
	 * After executing the strategy, the IDC structure is according to this version (but may have
	 * different data in tables, depending from strategy executed, e.g. full initial import or "empty" IDC).
	 * NOTICE: may be null, if strategy is independent from idc version, meaning can be executed any time.
	 * If null, then NO former strategies are executed if this is the target strategy. But this strategy
	 * can also be part of strategy workflow and will then be executed when "newer" target strategy is called !
	 * @return IDC version of catalog after execution. MAY RETURN NULL IF STRATEGY IS INDEPENDENT FROM
	 * VERSION AND CAN BE EXECUTED AT ANY TIME (no tracking and execution of strategies for obtaining
	 * former versions !)
	 */
	public String getIDCVersion();
}
