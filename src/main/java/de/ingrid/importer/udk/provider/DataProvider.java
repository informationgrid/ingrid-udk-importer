/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2020 wemove digital solutions GmbH
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
