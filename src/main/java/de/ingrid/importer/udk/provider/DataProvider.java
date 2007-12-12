/**
 * 
 */
package de.ingrid.importer.udk.provider;

import java.util.HashMap;
import java.util.Iterator;

/**
 * @author Administrator
 * 
 */
public interface DataProvider {

	public Iterator<String> getEntityIterator();

	public HashMap<String, String> findRow(String entityName, String rowName, String rowValue);

	public Iterator<HashMap<String, String>> getRowIterator(String entityName);

	public long getId();
}
