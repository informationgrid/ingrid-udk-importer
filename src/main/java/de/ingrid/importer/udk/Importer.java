package de.ingrid.importer.udk;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.JDBCConnectionProxy;
import de.ingrid.importer.udk.provider.DataProvider;
import de.ingrid.importer.udk.provider.InMemoryDataProvider;
import de.ingrid.importer.udk.provider.LazyInMemoryDataProvider;
import de.ingrid.importer.udk.strategy.IDCStrategy;
import de.ingrid.importer.udk.strategy.IDCStrategyFactory;

/**
 * UDK Data Importer UDK5.0 -> IDC.
 * 
 */
public class Importer {

	/**
	 * The logging object
	 */
	private static Log log = LogFactory.getLog(Importer.class);

	public static void main(String[] args) {

		System.out.println("Starting import.");
		ImportDescriptor descriptor = null;
		try {
			System.out.print("  reading input parameter...");
			descriptor = ImportDescriptorHelper.getDescriptor(args);
			System.out.println("done.");
		} catch (IllegalArgumentException e) {
			System.out.println("Error parsing input parameters.\n\nusage: java ingrid-udk-importer.jar [-u <user>] [-p <password>] [-v <idc version>] -c <config file> <file/directory> [file/directory]");
			return;
		}

		System.out.print("  reading data...");
		DataProvider data = new LazyInMemoryDataProvider(descriptor);
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
			strategy = idcStrategyFactory.getIdcStrategy(descriptor.getIdcVersion());
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
