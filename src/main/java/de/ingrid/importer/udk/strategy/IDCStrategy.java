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

	/** initial version */
	static String VALUE_IDC_VERSION_102 = "1.0.2";
	/** INSPIRE update version */
	static String VALUE_IDC_VERSION_103 = "1.0.3";

	/** contains all idc versions (specifying according default strategy) in ascending order.
	 * The default strategies for obtaining these versions are executed if needed. */
	static String[] ALL_IDC_VERSIONS = new String[] {
		VALUE_IDC_VERSION_102,
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
