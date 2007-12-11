/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import de.ingrid.importer.udk.ImportDescriptor;
import de.ingrid.importer.udk.provider.DataProvider;

/**
 * @author Administrator
 *
 */
public interface IDCStrategy {

	public void execute();
	
	public void setDataProvider (DataProvider data);
	
	public void setImportDescriptor (ImportDescriptor descriptor);
	
}
