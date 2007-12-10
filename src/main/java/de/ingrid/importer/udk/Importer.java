package de.ingrid.importer.udk;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
		
		try {
			ImportDescriptor descr = ImporterHelper.getDescriptor(args);
		} catch (IllegalArgumentException e) {
			return;
		}
        
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
