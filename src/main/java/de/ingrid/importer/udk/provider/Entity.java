/**
 * 
 */
package de.ingrid.importer.udk.provider;

import java.util.ArrayList;
import java.util.List;

/**
 * @author joachim
 * 
 */
public class Entity {

	private String name;

	private List<Row> rows = new ArrayList<Row>();

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name
	 *            the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the rows
	 */
	public List<Row> getRows() {
		return rows;
	}

	/**
	 * @param rows
	 *            the rows to set
	 */
	public void setRows(List<Row> rows) {
		this.rows = rows;
	}
}
