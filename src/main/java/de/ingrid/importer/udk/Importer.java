package de.ingrid.importer.udk;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.provider.InMemoryDataProvider;
import de.ingrid.importer.udk.strategy.IDCDefaultStrategy;
import de.ingrid.importer.udk.strategy.IDCStrategy;

/**
 * Hello world!
 * 
 */
public class Importer {

	/**
	 * The logging object
	 */
	private static Log log = LogFactory.getLog(Importer.class);

	public static void main(String[] args) {
		
		ImportDescriptor descr = null;
		try {
			descr = ImportDescriptorHelper.getDescriptor(args);
		} catch (IllegalArgumentException e) {
			return;
		}
		
		InMemoryDataProvider data = new InMemoryDataProvider(descr);
		
		IDCStrategy strategy = new IDCDefaultStrategy();
		strategy.setImportDescriptor(descr);
		strategy.setDataProvider(data);
		strategy.execute();
	}
}
