/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2016 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.util;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import de.ingrid.utils.udk.UtilsUDKCodeLists;
import de.ingrid.utils.xml.XMLSerializer;

/**
 * Singleton used handling codelists from serialized file.
 * 
 * @author Martin
 */
public class UtilsUdkCodelistsSerialized {

	private static final Logger LOG = Logger.getLogger(UtilsUdkCodelistsSerialized.class);

	private Map codeListsFromFile = null;

	private static UtilsUdkCodelistsSerialized myInstance;

	/** Get The Singleton */
	public static synchronized UtilsUdkCodelistsSerialized getInstance(String fileName) {
		if (myInstance == null) {
	        myInstance = new UtilsUdkCodelistsSerialized(fileName);
	      }
		return myInstance;
	}

	private UtilsUdkCodelistsSerialized() {}
	/** Constructor: load file at startup ! */
	private UtilsUdkCodelistsSerialized(String fileName) {
        // load xml file which contains information that need to be merged
        loadCodeListFile(fileName);

        removeUnwantedSyslists(new int[] { 1100, 1350, 1370, 3535, 3555 });
	}

    private void loadCodeListFile(String fileName) {
        try {
            InputStream resourceAsStream = UtilsUDKCodeLists.class.getResourceAsStream(fileName);
            if (resourceAsStream == null) {
                resourceAsStream = UtilsUDKCodeLists.class.getClassLoader().getResourceAsStream(fileName);
            }
            XMLSerializer serializer = new XMLSerializer();
            codeListsFromFile = (HashMap) serializer.deSerialize(resourceAsStream);
        } catch (Throwable ex) {
            throw new ExceptionInInitializerError(ex);
        }
    }

    public Map<Long, List<de.ingrid.utils.udk.CodeListEntry>> getAllCodeLists() {
    	Map<Long, List<de.ingrid.utils.udk.CodeListEntry>> allLists = new HashMap<Long, List<de.ingrid.utils.udk.CodeListEntry>>();

        for (Iterator itListIds = codeListsFromFile.keySet().iterator(); itListIds.hasNext();) {
        	Long listId = (Long) itListIds.next();
            List<de.ingrid.utils.udk.CodeListEntry> listEntries = new ArrayList<de.ingrid.utils.udk.CodeListEntry>();
            allLists.put(listId, listEntries);

            Map listEntriesMap = (Map) codeListsFromFile.get(listId);
            for (Iterator itListEntries = listEntriesMap.entrySet().iterator(); itListEntries.hasNext();) {
                Map.Entry listEntry = (Map.Entry) itListEntries.next();
                String entryKey = listEntry.getKey().toString();
                
                Map entryValuesMap = ((Map) listEntry.getValue());
                for (Iterator itEntryLanguages = entryValuesMap.keySet().iterator(); itEntryLanguages.hasNext();) {
                    Long entryLang = (Long) itEntryLanguages.next();
                    String entryValue = (String) entryValuesMap.get(entryLang);
                    listEntries.add(new de.ingrid.utils.udk.CodeListEntry(entryValue, listId, Long.valueOf(entryKey), entryLang));
                }
            }
        }

        return allLists;
    }
/*
    public List<de.ingrid.utils.udk.CodeListEntry> getCodeList(long lstId, long lang) {
        List<de.ingrid.utils.udk.CodeListEntry> result = new ArrayList<de.ingrid.utils.udk.CodeListEntry>();
        HashMap domain = (HashMap) codeListsFromFile.get(lstId);
        for (Iterator it = domain.entrySet().iterator(); it.hasNext();) {
            Map.Entry domainEntry = (Map.Entry) it.next();
            String domainEntryValue = (String) ((HashMap) domainEntry.getValue()).get(lang);
            String domainEntryKey = domainEntry.getKey().toString();
            result.add(new de.ingrid.utils.udk.CodeListEntry(domainEntryValue, lstId, Long.valueOf(domainEntryKey), lang));
        }
        return result;
    }
*/
    public void removeUnwantedSyslists(int[] listIds) {
    	for (int listId : listIds) {
            Object removedObj = codeListsFromFile.remove(new Long(listId));
            LOG.debug("Removed syslist " + listId + " from read syslists from file, removed object = " + removedObj);
    	}
    }
}
