/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2015 wemove digital solutions GmbH
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
package de.ingrid.importer.udk;


/**
 * UDK HELP Text importer
 * 
 */
public class HelpImporter {

	public static void main(String[] args) {

		System.out.println("DO NOT USE SEPARATE IMPORTER HERE ! BETTER USE MAIN IMPORTER WITH TARGET 1.0.2_help !!!");
/*
		System.out.println("Starting help import.");
		ImportDescriptor descriptor = null;
		try {
			System.out.print("  reading input parameter...");
			descriptor = ImportDescriptorHelper.getDescriptor(args);
			System.out.println("done.");
		} catch (IllegalArgumentException e) {
			System.out.println("Error parsing input parameters.\n\nusage: java -cp ingrid-udk-importer.jar de.ingrid.importer.udk.HelpImporter  [-u <user>] [-p <password>] -c <config file> <file/directory> [file/directory]");
			return;
		}

		System.out.print("  reading data...");
		InMemoryDataProvider data = null;
		try {
			data = new InMemoryDataProvider(descriptor);
		} catch (Exception e) {
			System.out.println("Error setting up DataProvider, see log for details !");
			log.error(e.getMessage());
			return;
		}
		System.out.println(" done.");

		// remove temp dir
		ImportDescriptorHelper.removeTempDir();

		JDBCConnectionProxy jdbc = null;
		try {
			jdbc = new JDBCConnectionProxy(descriptor);
		} catch (Exception e) {
			System.out.println("Error setting up jdbc connection, see log for details !");
			log.error(e.getMessage());
			return;
		}

		IDCStrategyFactory idcStrategyFactory = new IDCStrategyFactory();

		IDCStrategy strategy = null;
		try {
			strategy = idcStrategyFactory.getHelpImporterStrategy();
		} catch (Exception e) {
			log.error(e.getMessage());
		}
		strategy.setImportDescriptor(descriptor);
		strategy.setDataProvider(data);
		strategy.setJDBCConnectionProxy(jdbc);

		try {
			strategy.execute();
		} catch (Exception e) {
			log.error(e.getMessage());
		}
*/
	}
}
