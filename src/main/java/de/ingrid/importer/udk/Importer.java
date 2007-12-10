package de.ingrid.importer.udk;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.provider.InMemoryDataProvider;

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
        
        /* build input structure
		 * 
		 * - read all data into hashmap/list constructs
		 * 
		 * choose strategy
		 * 
		 * - dependend on cmd arguments
		 * - call strategy with input data
		 * 
		 */
		  
		
	}
}
