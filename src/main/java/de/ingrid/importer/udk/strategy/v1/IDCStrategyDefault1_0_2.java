/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 wemove digital solutions GmbH
 * ==================================================
 * Licensed under the EUPL, Version 1.1 or – as soon they will be
 * approved by the European Commission - subsequent versions of the
 * EUPL (the "Licence");
 * 
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 * 
 * http://ec.europa.eu/idabc/eupl5
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 * **************************************************#
 */
/**
 * 
 */
package de.ingrid.importer.udk.strategy.v1;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.JDBCHelper;
import de.ingrid.importer.udk.provider.Row;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.importer.udk.strategy.IDCStrategyHelper;

/**
 * @author Administrator
 * 
 */
public abstract class IDCStrategyDefault1_0_2 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategyDefault1_0_2.class);

	protected int defaultThemenkategorieEntryId = -1;
	private int ROLE_CATALOG_ADMINISTRATOR = 1;

	// maps mapping old syslist entryIds to new ones <oldEntryId, newEntryId>
	protected HashMap<Integer, Integer> mapOldKeyToNewKeyList100 = new HashMap<Integer, Integer>();
	protected HashMap<Integer, Integer> mapOldKeyToNewKeyList101 = new HashMap<Integer, Integer>();

	/** REDEFINE ! OLDER VERSION, no ID column yet ! */
	protected void setGenericKey(String key, String value) throws SQLException {
		jdbc.executeUpdate("DELETE FROM sys_generic_key WHERE key_name='" + key + "'");

		sqlStr = "INSERT INTO sys_generic_key (key_name, value_string) " +
			"VALUES ('" + key + "', '" + value + "')";
		jdbc.executeUpdate(sqlStr);
	}

	protected void processSysList() throws Exception {

		String entityName = "sys_list";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		// set up mapping of old syslist 100 to new syslist 100 via map<oldValue, newKey>
		HashMap<String, Integer> mapOldValueToNewKeyList100 = new HashMap<String, Integer>();
		mapOldValueToNewKeyList100.put("EPSG:4178 / Pulkovo 1942(83) / geographisch", 4178);
		mapOldValueToNewKeyList100.put("EPSG:4230 / ED50 / geographisch ", 4230);
		mapOldValueToNewKeyList100.put("EPSG:4258 / ETRS89 / geographisch", 4258);
		mapOldValueToNewKeyList100.put("EPSG:4284 / Pulkovo 1942 / geographisch", 4284);
		mapOldValueToNewKeyList100.put("EPSG:4314 / DHDN / geographisch", 4314);
		mapOldValueToNewKeyList100.put("EPSG:4326 / WGS 84 / geographisch", 4326);
		mapOldValueToNewKeyList100.put("EPSG:23031 / ED50 / UTM Zone 31N", 23031);
		mapOldValueToNewKeyList100.put("EPSG:23032 / ED50 / UTM Zone 32N", 23032);
		mapOldValueToNewKeyList100.put("EPSG:23033 / ED50 / UTM Zone 33N", 23033);
		mapOldValueToNewKeyList100.put("EPSG:32631 / WGS 84 / UTM Zone 31N", 32631);
		mapOldValueToNewKeyList100.put("EPSG:32632 / WGS 84 / UTM Zone 32N/33N", 32632);
		mapOldValueToNewKeyList100.put("EPSG:25831 / ETRS89 / UTM Zone 31N ", 25831);
		mapOldValueToNewKeyList100.put("EPSG:25832 / ETRS89 / UTM Zone 32N", 25832);
		mapOldValueToNewKeyList100.put("EPSG:25833 / ETRS89 / UTM Zone 33N", 25833);
		mapOldValueToNewKeyList100.put("EPSG:28463 / Pulkovo 1942 / Gauss-Krüger 2N/3N ", 28463);
		mapOldValueToNewKeyList100.put("EPSG:31466 / DHDN / Gauss-Krüger Zone 2", 31466);
		mapOldValueToNewKeyList100.put("EPSG:31467 /DHDN / Gauss-Krüger Zone 3", 31467);
		mapOldValueToNewKeyList100.put("EPSG:31468 / DHDN / Gauss-Krüger Zone 4", 31468);
		mapOldValueToNewKeyList100.put("EPSG:31469 / DHDN / Gauss-Krüger Zone 5", 31469);
		mapOldValueToNewKeyList100.put("EPSG:31492 /DHDN / Germany zone 2", 31466);
		mapOldValueToNewKeyList100.put("EPSG:31493 / DHDN / Germany zone 3", 31467);
		mapOldValueToNewKeyList100.put("EPSG:31494 / DHDN / Germany zone 4", 31468);
		mapOldValueToNewKeyList100.put("EPSG:31495 / DHDN / Germany zone 5", 31469);
		mapOldValueToNewKeyList100.put("DE_42/83 / GK_3", 9000001);
		mapOldValueToNewKeyList100.put("DE_DHDN / GK_3", 9000002);
		mapOldValueToNewKeyList100.put("DE_ETRS89 / UTM", 9000003);
		mapOldValueToNewKeyList100.put("DE_PD/83 / GK_3", 9000005);
		mapOldValueToNewKeyList100.put("DE_RD/83 / GK_3", 9000006);

		// set up mapping of old syslist 101 to new syslist 101 via map<oldValue, newKey>
		HashMap<String, Integer> mapOldValueToNewKeyList101 = new HashMap<String, Integer>();
		mapOldValueToNewKeyList101.put("Baltic Sea", 5105);
		mapOldValueToNewKeyList101.put("Normaal Amsterdams Peil", 900002);
		mapOldValueToNewKeyList101.put("European Vertical Reference Frame 2000", 5129);
		mapOldValueToNewKeyList101.put("Kronstädter Pegel (HN)", 900004);
		mapOldValueToNewKeyList101.put("DE_AMST / NH", 900002);
		mapOldValueToNewKeyList101.put("DE_AMST / NOH", 900003);
		mapOldValueToNewKeyList101.put("DE_KRON / NH", 900004);

		pSqlStr = "INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, description, maintainable) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM sys_list";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();

			if (row.getInteger("lst_id") == 1000) {
				// ignore list with id==1000, codelist 505 will be used instead
				
			} else if (row.getInteger("lst_id") == 3571 && row.getInteger("entry_id") == 4) {
				// ignore list with id==3571 and entry_id==4, codelist 505 will
				// be used instead
				
			} else {
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setInt(cnt++, row.getInteger("lst_id")); // lst_id
				p.setInt(cnt++, row.getInteger("entry_id")); // entry_id
				p.setString(cnt++, IDCStrategyHelper.transLanguageCode(row.get("lang_id"))); // lang_id
				p.setString(cnt++, row.get("name")); // name
				p.setString(cnt++, null); // description
				JDBCHelper.addInteger(p, cnt++, row.getInteger("maintainable")); // maintainable
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
			}
		}
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}

		entityName = "sys_codelist_domain";
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}
		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			
			if (row.getInteger("codelist_id") == 100) {
				// list with id==100 has to be mapped from old to new values !
				// set up mapping !
				Integer oldKey = row.getInteger("domain_id");
				if (mapOldKeyToNewKeyList100.get(oldKey) == null) {
					String oldValue = row.get("name");
					Integer newKey = mapOldValueToNewKeyList100.get(oldValue);
					if (newKey != null) {
						mapOldKeyToNewKeyList100.put(oldKey, newKey);
					}
				}

			} else if (row.getInteger("codelist_id") == 101) {
				// list with id==101 has to be mapped from old to new values !
				// set up mapping !
				Integer oldKey = row.getInteger("domain_id");
				if (mapOldKeyToNewKeyList101.get(oldKey) == null) {
					String oldValue = row.get("name");
					Integer newKey = mapOldValueToNewKeyList101.get(oldValue);
					if (newKey != null) {
						mapOldKeyToNewKeyList101.put(oldKey, newKey);
					}
				}

			} else {
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setInt(cnt++, row.getInteger("codelist_id")); // lst_id
				p.setInt(cnt++, row.getInteger("domain_id")); // entry_id
				p.setString(cnt++, IDCStrategyHelper.transLanguageCode(row.get("lang_id"))); // lang_id
				p.setString(cnt++, row.get("name")); // name
				p.setString(cnt++, row.get("description")); // description
				p.setInt(cnt++, 0); // maintainable
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
			}
		}
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
		if (log.isInfoEnabled()) {
			log.info("Importing special values...");
		}
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1400, 1, 'de', 'Daten und Karten', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1400, 2, 'de', 'Konzeptionelles', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1400, 3, 'de', 'Rechtliches', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1400, 4, 'de', 'Risikobewertungen', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1400, 5, 'de', 'Statusberichte', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1400, 6, 'de', 'Umweltzustand', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 1, 'de', 'Abfall', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 2, 'de', 'Altlasten', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 3, 'de', 'Bauen', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 4, 'de', 'Boden', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 5, 'de', 'Chemikalien', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 6, 'de', 'Energie', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 7, 'de', 'Forstwirtschaft', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 8, 'de', 'Gentechnik', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 9, 'de', 'Geologie', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 10, 'de', 'Gesundheit', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 11, 'de', 'Lärm und Erschütterungen', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 12, 'de', 'Landwirtschaft', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 13, 'de', 'Luft und Klima', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 14, 'de', 'Nachhaltige Entwicklung', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 15, 'de', 'Natur und Landschaft', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 16, 'de', 'Strahlung', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 17, 'de', 'Tierschutz', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 18, 'de', 'Umweltinformationen', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 19, 'de', 'Umweltwirtschaft', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 20, 'de', 'Verkehr', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 21, 'de', 'Wasser', 0)");

		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2000, 3100, 'de', 'Methode / Datengrundlage', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2000, 3210, 'de', 'Basisdaten', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2000, 3345, 'de', 'Basisdaten', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2000, 3515, 'de', 'Herstellungsprozess', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2000, 3520, 'de', 'Fachliche Grundlage', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2000, 3535, 'de', 'Schlüsselkatalog', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2000, 3555, 'de', 'Symbolkatalog', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2000, 3570, 'de', 'Datengrundlage', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2000, 5066, 'de', 'Verweis zu Dienst', 0)");

		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2010, 3360, 'de', 'Standort', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2010, 3360, 'en', 'Location', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2010, 3400, 'de', 'Projektleiter', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2010, 3400, 'en', 'Project Manager', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2010, 3410, 'de', 'Beteiligte', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2010, 3410, 'en', 'Participants', 0)");

		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 5100, 1, 'de', 'WMS', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 5100, 2, 'de', 'WFS', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 5110, 1, 'de', 'GetCapabilities', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 5110, 2, 'de', 'GetMap', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 5110, 3, 'de', 'GetFeatureInfo', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 5120, 1, 'de', 'DescribeFeatureType', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 5120, 2, 'de', 'GetFeature', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 5120, 3, 'de', 'GetFeature', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 5120, 4, 'de', 'LockFeature', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 5120, 5, 'de', 'Transaction', 0)");

		// remove old values
		jdbc.executeUpdate("DELETE FROM sys_list WHERE lst_id=2240");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2240, 1, 'de', 'HTML', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2240, 2, 'de', 'JPG', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2240, 3, 'de', 'PNG', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2240, 4, 'de', 'GIF', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2240, 5, 'de', 'PDF', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2240, 6, 'de', 'DOC', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2240, 7, 'de', 'PPT', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2240, 8, 'de', 'XLS', 0)");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2240, 9, 'de', 'ASCII/Text', 0)");

		if (log.isInfoEnabled()) {
			log.info("Importing special values... done.");
		}

		if (log.isInfoEnabled()) {
			log.info("Importing new syslist 100 (Raumbezugsystem), 101 (Vertikaldaten)...");
		}
		// syslist 100 !
		for(Object key : mapNewKeyToNewValueList100.keySet()) {
		    Object value = mapNewKeyToNewValueList100.get(key);
			dataProvider.setId(dataProvider.getId() + 1);
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name) VALUES ("
					+ dataProvider.getId() + ", 100, " + key + ", 'de', '" + value + "')");
			dataProvider.setId(dataProvider.getId() + 1);
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name) VALUES ("
					+ dataProvider.getId() + ", 100, " + key + ", 'en', '" + value + "')");
		}

		// syslist 101 !
		for(Object key : mapNewKeyToNewValueList101.keySet()) {
		    Object value = mapNewKeyToNewValueList101.get(key);
			dataProvider.setId(dataProvider.getId() + 1);
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name) VALUES ("
					+ dataProvider.getId() + ", 101, " + key + ", 'de', '" + value + "')");
			dataProvider.setId(dataProvider.getId() + 1);
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name) VALUES ("
					+ dataProvider.getId() + ", 101, " + key + ", 'en', '" + value + "')");
		}
		if (log.isInfoEnabled()) {
			log.info("Importing new syslist 100 (Raumbezugsystem), 101 (Vertikaldaten)... done");
		}
	}

	protected void importDefaultUserdata() throws Exception {
		sqlStr = "DELETE FROM idc_group";
		jdbc.executeUpdate(sqlStr);
		sqlStr = "DELETE FROM idc_user";
		jdbc.executeUpdate(sqlStr);
		sqlStr = "DELETE FROM permission";
		jdbc.executeUpdate(sqlStr);
		sqlStr = "DELETE FROM idc_user_permission";
		jdbc.executeUpdate(sqlStr);
		
		// import default admin adress
		dataProvider.setId(dataProvider.getId() + 1);
		long adrId = dataProvider.getId();
		String sqlStr = "INSERT INTO t02_address (id, adr_uuid, org_adr_id, "
			+ "adr_type, institution, lastname, firstname, address_value, address_key, title_value, title_key, "
			+ "street, postcode, postbox, postbox_pc, city, country_code, job, "
			+ "descr, lastexport_time, expiry_time, work_state, work_version, "
			+ "mark_deleted, create_time, mod_time, mod_uuid, responsible_uuid) VALUES "
			+ "( " + dataProvider.getId() + ", '" + getCatalogAdminUuidNewCatalog() + "', NULL, 3, NULL, 'admin', 'admin', 'Frau', -1, 'Dr.', -1, "
			+ "NULL, NULL, NULL, NULL, NULL, NULL, 'Administrator of this catalog.', "
			+ "'Administrator of this catalog.', NULL, NULL, 'V', 0, "
			+ "'N', NULL, NULL, NULL, NULL)";
		jdbc.executeUpdate(sqlStr);
		
		dataProvider.setId(dataProvider.getId() + 1);
		sqlStr = "INSERT INTO address_node ( id , addr_uuid , addr_id , addr_id_published , fk_addr_uuid ) VALUES ( " + dataProvider.getId() + ", '" + getCatalogAdminUuidNewCatalog() + "', "+adrId+", "+adrId+", NULL )"; 
		jdbc.executeUpdate(sqlStr);

		// import default admin group
		dataProvider.setId(dataProvider.getId() + 1);
		long groupId = dataProvider.getId();
		sqlStr = "INSERT INTO idc_group ( id, name) VALUES (" + groupId + ", 'administrators')";
		jdbc.executeUpdate(sqlStr);
		
		// import default admin user
		dataProvider.setId(dataProvider.getId() + 1);
		long userId = dataProvider.getId();
		sqlStr = "INSERT INTO idc_user ( id, addr_uuid, idc_group_id, idc_role) VALUES (" + userId + ", '"+getCatalogAdminUuidNewCatalog()+"', "+groupId+", "+ROLE_CATALOG_ADMINISTRATOR+" )";
		jdbc.executeUpdate(sqlStr);
		
		// import permissions
		dataProvider.setId(dataProvider.getId() + 1);
		sqlStr = "INSERT INTO permission ( id , class_name , name , action ) VALUES ( " + dataProvider.getId() + ", 'IdcEntityPermission', 'entity', 'write')";
		jdbc.executeUpdate(sqlStr);
		dataProvider.setId(dataProvider.getId() + 1);
		sqlStr = "INSERT INTO permission ( id , class_name , name , action ) VALUES ( " + dataProvider.getId() + ", 'IdcEntityPermission', 'entity', 'write-tree')";
		jdbc.executeUpdate(sqlStr);
		dataProvider.setId(dataProvider.getId() + 1);
		long permissionCreateCatalodId = dataProvider.getId();
		sqlStr = "INSERT INTO permission ( id , class_name , name , action ) VALUES ( " + dataProvider.getId() + ", 'IdcUserPermission', 'catalog', 'create-root')";
		jdbc.executeUpdate(sqlStr);
		dataProvider.setId(dataProvider.getId() + 1);
		long permissionCreateQaId = dataProvider.getId();
		sqlStr = "INSERT INTO permission ( id , class_name , name , action ) VALUES ( " + dataProvider.getId() + ", 'IdcUserPermission', 'catalog', 'qa')";
		jdbc.executeUpdate(sqlStr);
		
		// import user permissions
		dataProvider.setId(dataProvider.getId() + 1);
		sqlStr = "INSERT INTO idc_user_permission ( id , permission_id , idc_group_id ) VALUES ( " + dataProvider.getId() + ", "+permissionCreateCatalodId+", "+groupId+")";
		jdbc.executeUpdate(sqlStr);
		dataProvider.setId(dataProvider.getId() + 1);
		sqlStr = "INSERT INTO idc_user_permission ( id , permission_id , idc_group_id ) VALUES ( " + dataProvider.getId() + ", "+permissionCreateQaId+", "+groupId+")";
		jdbc.executeUpdate(sqlStr);
	}

	protected void postProcess_generic() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Post processing ...");
		}

		// set the correct obj_node_id to the object index table
		// this is necessary, because the node_id is not yet known, when the index is created
		// ---------------------------------------------
		if (log.isInfoEnabled()) {
			log.info("update obj_node_id in object index ...");
		}
		for (Iterator<Row> i = dataProvider.getRowIterator("t01_object"); i.hasNext();) {
			Row row = i.next();
			if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				String sql = "SELECT id FROM object_node WHERE obj_id="
					+ row.getInteger("primary_key");
				Statement st = jdbc.createStatement();
				ResultSet rs = jdbc.executeQuery(sql, st);
				if (rs.next()) {
					jdbc.executeUpdate("UPDATE full_index_obj SET obj_node_id = " + rs.getLong("id") + " WHERE obj_node_id="
							+ row.getInteger("primary_key"));
				}
				rs.close();
				st.close();
			}
		}
		
		// set the correct addr_node_id to the address index table
		// this is necessary, because the node_id is not yet known, when the index is created
		// ---------------------------------------------
		if (log.isInfoEnabled()) {
			log.info("update addr_node_id in address index ...");
		}
		for (Iterator<Row> i = dataProvider.getRowIterator("t02_address"); i.hasNext();) {
			Row row = i.next();
			if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				String sql = "SELECT id FROM address_node WHERE addr_id="
					+ row.getInteger("primary_key");
				Statement st = jdbc.createStatement();
				ResultSet rs = jdbc.executeQuery(sql, st);
				if (rs.next()) {
					jdbc.executeUpdate("UPDATE full_index_addr SET addr_node_id = " + rs.getLong("id") + " WHERE addr_node_id="
							+ row.getInteger("primary_key"));
				}
				rs.close();
				st.close();
			}
		}		

		// set responsible user to cat-admin in entities
		// ---------------------------------------------
		if (log.isInfoEnabled()) {
			log.info("set responsible_uuid in entities to catadmin ...");
		}
		String catAdminUuid = null;
		String sql = "SELECT addr_uuid FROM idc_user WHERE idc_role=" + ROLE_CATALOG_ADMINISTRATOR;
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		if (rs.next()) {
			catAdminUuid = rs.getString("addr_uuid");
		}
		rs.close();
		st.close();
		
		if (catAdminUuid == null) {
			if (log.isInfoEnabled()) {
				log.info("Couldn't find addr_uuid of CATALOG_ADMINISTRATOR !!!!!!!!!!!!! sql = '" + sql + "'");
			}			
		}

		jdbc.executeUpdate("UPDATE t01_object SET responsible_uuid = '" + catAdminUuid + "'");
		jdbc.executeUpdate("UPDATE t02_address SET responsible_uuid = '" + catAdminUuid + "'");			

		
		// set entities mod-user to cat-admin if address non existent (in objects, addresses, catalogue)
		// -----------------------------------------------------------------------
		if (log.isInfoEnabled()) {
			log.info("set mod_uuid in entities to catadminUuid(" + catAdminUuid + ") if mod_uuid not found ...");
		}
		
		// OBJECTS
		sql = "select distinct obj.obj_uuid, obj.id, obj.mod_uuid " +
			"from t01_object obj left outer join address_node aNode on obj.mod_uuid = aNode.addr_uuid " +
			"where aNode.addr_uuid is null " +
			"ORDER BY obj.obj_uuid, obj.id, obj.mod_uuid";

		st = jdbc.createStatement();
		rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			long objId = rs.getLong("id");

			log.info("Invalid entry in t01_object found: mod_uuid not found, we set catadmin as mod_uuid !!! " +
				"objId('" + objId + "'), obj_uuid('" + rs.getString("obj_uuid") + "'), invalid mod_uuid('" + rs.getString("mod_uuid") + "').");
			
			jdbc.executeUpdate("UPDATE t01_object SET mod_uuid = '" + catAdminUuid + "' where id=" + objId);
		}
		rs.close();
		st.close();

		// ADDRESSES
		sql = "select distinct addr.adr_uuid, addr.id, addr.mod_uuid " +
			"from t02_address addr left outer join address_node aNode on addr.mod_uuid = aNode.addr_uuid " +
			"where aNode.addr_uuid is null " +
			"ORDER BY addr.adr_uuid, addr.id, addr.mod_uuid";

		st = jdbc.createStatement();
		rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			long addrId = rs.getLong("id");

			log.info("Invalid entry in t02_address found: mod_uuid not found, we set catadmin as mod_uuid !!! " +
				"addrId('" + addrId + "'), adr_uuid('" + rs.getString("adr_uuid") + "'), invalid mod_uuid('" + rs.getString("mod_uuid") + "').");
			
			jdbc.executeUpdate("UPDATE t02_address SET mod_uuid = '" + catAdminUuid + "' where id=" + addrId);
		}
		rs.close();
		st.close();

		// CATALOGUE
		sql = "select distinct cat.cat_uuid, cat.id, cat.mod_uuid " +
			"from t03_catalogue cat left outer join address_node aNode on cat.mod_uuid = aNode.addr_uuid " +
			"where aNode.addr_uuid is null " +
			"ORDER BY cat.cat_uuid";

		st = jdbc.createStatement();
		rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			long catId = rs.getLong("id");

			log.info("Invalid entry in t03_catalogue found: mod_uuid not found, we set catadmin as mod_uuid !!! " +
				"cat_uuid('" + rs.getString("cat_uuid") + "'), invalid mod_uuid('" + rs.getString("mod_uuid") + "').");
			
			jdbc.executeUpdate("UPDATE t03_catalogue SET mod_uuid = '" + catAdminUuid + "' where id=" + catId);
		}
		rs.close();
		st.close();


		// set default object "Themenkategorie" if none set
		// ------------------------------------------------
		if (defaultThemenkategorieEntryId != -1) {
			if (log.isInfoEnabled()) {
				log.info("set default \"Themenkategorie\" in objects not categorized ...");
			}
			
			pSqlStr = "INSERT INTO t011_obj_topic_cat (id, obj_id, line, topic_category) VALUES ( ?, ?, ?, ?)";
			PreparedStatement p = jdbc.prepareStatement(pSqlStr);

			sql = "select distinct obj.id " +
				"from t01_object obj left outer join t011_obj_topic_cat topicCat on obj.id = topicCat.obj_id " +
				"where topicCat.obj_id is null " +
				"ORDER BY obj.id";

			st = jdbc.createStatement();
			rs = jdbc.executeQuery(sql, st);
			while (rs.next()) {
				long objId = rs.getLong("id");

				log.info("No \"Themenkategorie\" set for t01_object, we set default category entryid(" + defaultThemenkategorieEntryId +
						"): objId('" + objId + "').");

				int cnt = 1;
				dataProvider.setId(dataProvider.getId() + 1);					
				p.setLong(cnt++, dataProvider.getId()); // id
				p.setLong(cnt++, objId); // obj_id
				p.setInt(cnt++, 1); // line
				JDBCHelper.addInteger(p, cnt++, defaultThemenkategorieEntryId); // topic_category
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
			}
			p.close();
			rs.close();
			st.close();
		}

		// set default entries in sys_lists
		// --------------------------------
		if (log.isInfoEnabled()) {
			log.info("set default entries in sys_lists ...");
		}

		// set default language of metadata entities (=default entry in sys_list 99999999)

		// first check whether defaults set -> ignore localization !
		st = jdbc.createStatement();
		rs = jdbc.executeQuery("SELECT id FROM sys_list WHERE lst_id=99999999 AND is_default = 'Y'", st);
		boolean hasDefaults = rs.next();
		rs.close();
		st.close();
		if (!hasDefaults) {
			// default is german (=121) ! set in all localized versions as default (lang_id='de' -> "Deutsch", lang_id='en' -> "German", ...)
			jdbc.executeUpdate("UPDATE sys_list SET is_default = 'Y' WHERE lst_id=99999999 AND entry_id=121");
		}
		
		// set default publication condition INTERNET (=default entry in sys_list 3571)

		// first check whether defaults set -> ignore localization !
		st = jdbc.createStatement();
		rs = jdbc.executeQuery("SELECT id FROM sys_list WHERE lst_id=3571 AND is_default = 'Y'", st);
		hasDefaults = rs.next();
		rs.close();
		st.close();
		if (!hasDefaults) {
			// default is Internet (=1) ! set in all localized versions as default
			jdbc.executeUpdate("UPDATE sys_list SET is_default = 'Y' WHERE lst_id=3571 AND entry_id=1");
		}
		
		if (log.isInfoEnabled()) {
			log.info("Post processing ... done.");
		}
	}

	protected void setHiLoGenerator() throws SQLException {
		setHiLoGeneratorViaId(dataProvider.getId());
	}


}
