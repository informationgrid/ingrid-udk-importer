/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.util.HashMap;
import java.util.Iterator;

import de.ingrid.importer.udk.ImportDescriptor;
import de.ingrid.importer.udk.provider.DataProvider;

/**
 * @author Administrator
 *
 */
public class IDCDefaultStrategy implements IDCStrategy {

	DataProvider dataProvider = null;
	ImportDescriptor importDescriptor = null;
	
	public void setDataProvider (DataProvider data) {
		dataProvider = data;
	}

	public void setImportDescriptor(ImportDescriptor descriptor) {
		importDescriptor = descriptor;
	}

	/* (non-Javadoc)
	 * @see de.ingrid.importer.udk.strategy.IDCStrategy#execute()
	 */
	public void execute() {
		// remove all entries in T03_catalogue
		for (Iterator<HashMap<String, String>> i = dataProvider.getRowIterator("T03_catalogue"); i.hasNext(); ) {
	        HashMap<String,String> row = i.next();
	        
		}
		
		// TODO Auto-generated method stub

	}


}
