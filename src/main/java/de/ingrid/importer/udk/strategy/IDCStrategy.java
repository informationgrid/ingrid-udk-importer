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

	public void execute() throws Exception;

	public void setDataProvider(DataProvider data);

	public void setImportDescriptor(ImportDescriptor descriptor);

	public void setJDBCConnectionProxy(JDBCConnectionProxy jdbc);

}
