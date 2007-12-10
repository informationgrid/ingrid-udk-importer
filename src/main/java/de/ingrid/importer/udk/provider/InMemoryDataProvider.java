/**
 * 
 */
package de.ingrid.importer.udk.provider;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;

import de.ingrid.importer.udk.ImportDescriptor;

/**
 * @author Administrator
 * 
 */
public class InMemoryDataProvider {

	private static Log log = LogFactory.getLog(InMemoryDataProvider.class);
	
	private HashMap<String, Entity> entities = new HashMap<String, Entity>();

	public InMemoryDataProvider(ImportDescriptor desciptor) {
		getEntities(desciptor);
	}

	public Iterator<String> getEntityIterator() {
		return entities.keySet().iterator();
	}
	
	public HashMap<String, String> findRow(String entityName, String rowName, String rowValue) {
		Entity e = entities.get(entityName);
		for (HashMap<String, String> row : e.getRows()) {
			if (row.get(rowName).equals(rowValue)) {
				return row;
			}
		}
		return null;
	}
	
	public Iterator<HashMap<String, String>> getRowIterator(String entityName) {
		Entity e = entities.get(entityName);
		return e.getRows().iterator();
	}
	

	private class Entity {
		private String name;
		
		private List<HashMap<String, String>> rows = new ArrayList<HashMap<String, String>>();

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
		public List<HashMap<String, String>> getRows() {
			return rows;
		}

		/**
		 * @param rows
		 *            the rows to set
		 */
		public void setRows(List<HashMap<String, String>> rows) {
			this.rows = rows;
		}
	}

	private void getEntities(ImportDescriptor descriptor) {

		Document dom = null;
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

		for (String fileName : descriptor.getFiles()) {
			DocumentBuilder db;
			try {
				db = dbf.newDocumentBuilder();
				dom = db.parse(new File(fileName));
				Entity entity = this.new Entity();
				String entityName = readTargetTable(dom);
				entity.setName(entityName);
				entities.put(entityName, entity);
				entity.setRows(readRows(dom));
			} catch (Exception e) {
				log.error("Error parsing file '" + fileName + "'", e);
			}
		}
	}

	private List<HashMap<String, String>> readRows(Document dom) {
		List<HashMap<String, String>> rows = new ArrayList<HashMap<String, String>>();

		try {
			String xpath = "//data/row";
			NodeList nl = org.apache.xpath.XPathAPI.selectNodeList(dom, xpath);

			for (int i = 0; i < nl.getLength(); i++) {
				HashMap<String, String> row = new HashMap<String, String>();

				NamedNodeMap nm = ((Element) nl.item(i)).getAttributes();
				for (int j = 0; j < nm.getLength(); j++) {
					row.put(nm.item(j).getNodeName(), nm.item(j).getNodeValue());
				}
				rows.add(row);
			}
		} catch (TransformerException e) {
			log.error("Error reading xml document!", e);
		}
		return rows;
	}

	private String readTargetTable(Document dom) {
		String tTable = "";
		try {
			String xpath = "//Schema/ElementType/AttributeType";
			NodeList nl = org.apache.xpath.XPathAPI.selectNodeList(dom, xpath);
			tTable = ((Element) nl.item(0)).getAttribute("rs:basetable");
			log.debug("Reading target table name: '" + tTable + "'");

		} catch (TransformerException e) {
			log.error("Can't read target table from XML file!");
		}
		return tTable;
	}
}
