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

	/** initial import */
	static String VALUE_IDC_VERSION_102 = "1.0.2";
	/** INSPIRE etc. */
	static String VALUE_IDC_VERSION_103 = "1.0.3";

	/** contains all idc versions (specifying according strategy) in ascending order */
	static String[] ALL_IDC_VERSIONS = new String[] {
		VALUE_IDC_VERSION_102,
		VALUE_IDC_VERSION_103,
	};
		
	public void execute() throws Exception;

	public void setDataProvider(DataProvider data);

	public void setImportDescriptor(ImportDescriptor descriptor);

	public void setJDBCConnectionProxy(JDBCConnectionProxy jdbc);
	
	public String getIDCVersion();
}
