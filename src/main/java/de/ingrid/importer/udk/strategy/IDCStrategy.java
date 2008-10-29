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

	/** initial version/strategy */
	static String VALUE_IDC_VERSION_102 = "1.0.2";
	/** FIX strategy (writes NO version) for fixing wrong syslists. ALSO INTEGRATED IN STRATEGY WORKFLOW */
	static String VALUE_IDC_FIX_SYSLIST = "1.0.2_fix_syslist_100_101";
	/** SNS Spatial Type Update */
	static String VALUE_IDC_VERSION_102_SNS_SPATIAL_TYPE = "1.0.2_sns_spatial_type";
	/** INSPIRE update version/strategy */
	static String VALUE_IDC_VERSION_103 = "1.0.3";

	/** Order of strategies to execute to obtain most recent IGC.
	 * Contains all according versions/strategies in ascending order.
	 * NOTICE: THESE VERSIONS ARE MAPPED TO STRATEGIES IN STRATEGY FACTORY !!! */
	static String[] STRATEGY_WORKFLOW = new String[] {
		VALUE_IDC_VERSION_102,
		VALUE_IDC_FIX_SYSLIST,
		VALUE_IDC_VERSION_102_SNS_SPATIAL_TYPE,
		VALUE_IDC_VERSION_103,
	};
		
	public void execute() throws Exception;

	public void setDataProvider(DataProvider data);

	public void setImportDescriptor(ImportDescriptor descriptor);

	public void setJDBCConnectionProxy(JDBCConnectionProxy jdbc);
	
	/**
	 * Get the IDC Version generated by this strategy (written into IDC catalog).
	 * After executing the strategy, the IDC structure is according to this version (but may have
	 * different data in tables, depending from strategy executed, e.g. full initial import or "empty" IDC).
	 * @return IDC version of catalog after execution. MAY RETURN NULL IF STRATEGY IS INDEPENDENT FROM
	 * VERSION AND CAN BE EXECUTED AT ANY TIME (no tracking and execution of strategies for obtaining
	 * former versions !)
	 */
	public String getIDCVersion();
}
