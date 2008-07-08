/**
 * 
 */
package de.ingrid.importer.udk.provider;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.xml.sax.InputSource;

import de.ingrid.importer.udk.ImportDescriptor;
import de.ingrid.importer.udk.util.UdkXMLReader;

/**
 * @author Administrator
 * 
 */
public class InMemoryDataProvider implements DataProvider {

	private static Log log = LogFactory.getLog(InMemoryDataProvider.class);

	private HashMap<String, Entity> entities = new HashMap<String, Entity>();

	private long id = 0;

	protected static List<String> invalidModTypes;

	public InMemoryDataProvider(ImportDescriptor desciptor) {
		getEntities(desciptor);

		invalidModTypes = Arrays.asList(DataProvider.invalidModTypes);
	}

	public Row findRow(String entityName, String rowName, String rowValue) {
		Entity e = entities.get(entityName);
		if (e == null) {
			return null;
		}
		for (Row row : e.getRows()) {
			if (row.get(rowName) != null && row.get(rowName).equals(rowValue)) {
				return row;
			}
		}
		return null;
	}

	public Row findRowStartsWith(String entityName, String rowName, String rowValue) {
		Entity e = entities.get(entityName);
		if (e == null) {
			return null;
		}
		for (Row row : e.getRows()) {
			if (row.get(rowName) != null && row.get(rowName).startsWith(rowValue)) {
				return row;
			}
		}
		return null;
	}

	public Row findRow(String entityName, String[] rowName, String[] rowValue) {
		if (rowName.length != rowValue.length) {
			log.error("Error finding row! Parameter arrays for rowName and rowValue are not equal.");
			return null;
		}

		Entity e = entities.get(entityName);
		for (Row row : e.getRows()) {
			int cnt = 0;
			for (int i = 0; i < rowName.length; i++) {
				if (row.get(rowName[i]) != null && row.get(rowName[i]).equals(rowValue[i])) {
					cnt++;
				}
			}
			if (cnt == rowName.length) {
				return row;
			}

		}
		return null;
	}

	public Iterator<Row> getRowIterator(String entityName) {
		Entity e = entities.get(entityName);
		if (e == null) {
			// return empty iterator
			return new ArrayList<Row>().iterator();
		} else {
			return e.getRows().iterator();
		}
	}

	private void getEntities(ImportDescriptor descriptor) {

		Document dom = null;
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = null;

		for (String fileName : descriptor.getFiles()) {
			try {
				db = dbf.newDocumentBuilder();
				
		        Reader fr = new InputStreamReader(new FileInputStream(fileName), "UTF-8");
				// preprocess file -> replace line feeds in attributes with "&#xA;" so parser doesn't replace them with blank  !
		        UdkXMLReader xr = new UdkXMLReader( fr );
				InputSource inputSource = new InputSource(xr);
				dom = db.parse(inputSource);
				
				Entity entity = new Entity();
				String entityName = readTargetTable(dom);
				entity.setName(entityName);
				entities.put(entityName, entity);
				entity.setRows(readRows(dom));
				dom = null;
				db = null;
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
					row.put(nm.item(j).getNodeName().toLowerCase(), nm.item(j).getNodeValue());
				}

				// SKIP INVALID ROWS (e.g. marked deleted) !
				if (row.get("mod_type") != null && invalidModTypes.contains(row.get("mod_type"))) {
					if (log.isDebugEnabled()) {
						log.debug("Skip row with mod_type('" + row.get("mod_type") +
							"'). Skipped Row: " + row);
					}
					continue;
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
			tTable = ((Element) nl.item(0)).getAttribute("rs:basetable").toLowerCase();
			log.debug("Reading target table name: '" + tTable + "'");

		} catch (TransformerException e) {
			log.error("Can't read target table from XML file!");
		}
		return tTable;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

}
