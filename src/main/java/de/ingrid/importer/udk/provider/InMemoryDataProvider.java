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
public class InMemoryDataProvider implements DataProvider {

	private static Log log = LogFactory.getLog(InMemoryDataProvider.class);

	private HashMap<String, Entity> entities = new HashMap<String, Entity>();

	private long id = 0;

	public InMemoryDataProvider(ImportDescriptor desciptor) {
		getEntities(desciptor);
	}

	public Row findRow(String entityName, String rowName, String rowValue) {
		Entity e = entities.get(entityName);
		for (Row row : e.getRows()) {
			if (row.get(rowName).equals(rowValue)) {
				return row;
			}
		}
		return null;
	}

	public Iterator<Row> getRowIterator(String entityName) {
		Entity e = entities.get(entityName);
		return e.getRows().iterator();
	}

	private void getEntities(ImportDescriptor descriptor) {

		Document dom = null;
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

		for (String fileName : descriptor.getFiles()) {
			DocumentBuilder db;
			try {
				db = dbf.newDocumentBuilder();
				dom = db.parse(new File(fileName));
				Entity entity = new Entity();
				String entityName = readTargetTable(dom);
				entity.setName(entityName);
				entities.put(entityName, entity);
				entity.setRows(readRows(dom));
			} catch (Exception e) {
				log.error("Error parsing file '" + fileName + "'", e);
			}
		}
	}

	private List<Row> readRows(Document dom) {
		List<Row> rows = new ArrayList<Row>();

		try {
			String xpath = "//data/row";
			NodeList nl = org.apache.xpath.XPathAPI.selectNodeList(dom, xpath);

			for (int i = 0; i < nl.getLength(); i++) {
				Row row = new Row();

				NamedNodeMap nm = ((Element) nl.item(i)).getAttributes();
				for (int j = 0; j < nm.getLength(); j++) {
					row.put(nm.item(j).getNodeName(), nm.item(j).getNodeValue());
				}
				row.put("primary_key", String.valueOf(++id));
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

	public long getId() {
		return id;
	}
}
