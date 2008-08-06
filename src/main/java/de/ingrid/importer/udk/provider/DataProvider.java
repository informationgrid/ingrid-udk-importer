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

	public Row findRow(String entityName, String rowName, String rowValue) throws Exception;

	public Row findRow(String entityName, String[] rowName, String[] rowValue) throws Exception;
	
	public Row findRowStartsWith(String entityName, String rowName, String rowValue) throws Exception;
	
	public Iterator<Row> getRowIterator(String entityName) throws Exception;

	public long getId();

	public void setId(long id);
}
