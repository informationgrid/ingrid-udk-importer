package de.ingrid.importer.udk;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.JDBCConnectionProxy;
import de.ingrid.importer.udk.provider.InMemoryDataProvider;
import de.ingrid.importer.udk.strategy.IDCStrategy;
import de.ingrid.importer.udk.strategy.IDCStrategyFactory;

/**
 * UDK HELP Text importer
 * 
 */
public class HelpImporter {

	/**
	 * The logging object
	 */
	private static Log log = LogFactory.getLog(HelpImporter.class);

	public static void main(String[] args) {

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
		InMemoryDataProvider data = new InMemoryDataProvider(descriptor);
		System.out.println(" done.");

		// remove temp dir
		ImportDescriptorHelper.removeTempDir();

		JDBCConnectionProxy jdbc = null;
		try {
			jdbc = new JDBCConnectionProxy(descriptor);
		} catch (Exception e) {
			log.error(e.getMessage());
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
	}
}
