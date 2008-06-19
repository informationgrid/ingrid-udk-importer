/**
 * 
 */
package de.ingrid.importer.udk.provider;

import java.util.Iterator;

/**
 * @author Administrator
 * 
 */
public interface DataProvider {
	
	public static String[] invalidModTypes = new String[] { "D" };

	public Row findRow(String entityName, String rowName, String rowValue);

	public Row findRow(String entityName, String[] rowName, String[] rowValue);
	
	public Row findRowStartsWith(String entityName, String rowName, String rowValue);
	
	public Iterator<Row> getRowIterator(String entityName);

	public long getId();

	public void setId(long id);
}
