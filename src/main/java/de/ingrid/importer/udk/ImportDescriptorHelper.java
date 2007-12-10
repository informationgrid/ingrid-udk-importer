/**
 * 
 */
package de.ingrid.importer.udk;

import java.io.File;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.util.Zipper;
import de.ingrid.importer.udk.util.ZipperFilter;

/**
 * @author joachim
 * 
 */
public class ImportDescriptorHelper {

	/**
	 * The logging object
	 */
	private static Log log = LogFactory.getLog(Importer.class);

	/**
	 * Adds all files (suffix '.xml') of the given directory <code>dir</code>
	 * and its sub directories.
	 * 
	 * @param dir
	 *            The directory
	 * @param overwrite
	 *            Overwrite existing data on database?
	 */
	private static void addDirectory(final File dir, List<String> fileList) {
		File[] entries = dir.listFiles();

		String file = "";
		for (int i = 0; i < entries.length; i++) {
			if (entries[i].getName().endsWith(".xml")) {
				file = dir + "/" + entries[i].getName();
				fileList.add(file);
			} else if (entries[i].isDirectory()) {
				addDirectory(entries[i], fileList);
			}
		}
	}

	private static List<String> extractZipFile(final File f, final String targetDir) {

		boolean keepSubfolders = false;

		String[] accFiles = { ".xml" };
		ZipperFilter zipperFilter = new ZipperFilter(accFiles);

		return Zipper.extractZipFile(f, targetDir, keepSubfolders, zipperFilter);
	}

	private static String getTmpDir() {

		final String tmpDirName = "./tmp";

		File tmpFile = new File(tmpDirName);

		if (!tmpFile.exists()) {
			tmpFile.mkdir();
		}

		if (tmpFile.isDirectory() && tmpFile.canWrite()) {
			return tmpDirName;
		} else {
			return null;
		}
	}

	public static ImportDescriptor getDescriptor(String[] args) {
		ImportDescriptor descr = new ImportDescriptor();
		String dbUser = null;
		String dbPasswd = null;
		String udkDbVersion = null;

		for (int i = 0; i < args.length; i++) {
			if (args[i].equalsIgnoreCase("-u")) {
				if (args.length <= i + 1) {
					log.error("Missing argument for parameter '-u'.");
					throw new IllegalArgumentException("Missing argument for parameter '-u'.");
				}
				dbUser = args[++i];
			} else if (args[i].equalsIgnoreCase("-p")) {
				if (args.length <= i + 1) {
					log.error("Missing argument for parameter '-p'.");
					throw new IllegalArgumentException("Missing argument for parameter '-p'.");
				}
				dbPasswd = args[++i];
			} else if (args[i].equalsIgnoreCase("-c")) {
				if (args.length <= i + 1) {
					log.error("Missing argument for parameter '-c'.");
					throw new IllegalArgumentException("Missing argument for parameter '-c'.");
				}
				descr.setConfigurationFile(args[++i]);
			} else if (args[i].equalsIgnoreCase("-v")) {
				if (args.length <= i + 1) {
					log.error("Missing argument for parameter '-c'.");
					throw new IllegalArgumentException("Missing argument for parameter '-c'.");
				}
				udkDbVersion = args[++i];
			} else {
				File f = new File(args[i]);
				if (f.isDirectory()) {
					// read directory
					ImportDescriptorHelper.addDirectory(f, descr.getFiles());
				} else if (f.getName().endsWith(".zip")) {

					List<String> vExtracted = extractZipFile(f, getTmpDir() + "/");

					if (vExtracted != null) {
						for (int j = 0; j < vExtracted.size(); j++) {
							descr.getFiles().add(vExtracted.get(j));
						}
					} else {
						log.error("Error unzipping '" + f.getName() + "'.");
						throw new IllegalArgumentException("Error unzipping '" + f.getName() + "'.");
					}
				} else if (f.isFile()) {
					// read file
					descr.getFiles().add(f.getName());
				}
			}
		}

		// read defaults from config file
		PropertiesConfiguration configuration = new PropertiesConfiguration();

		try {
			configuration.load(new File(descr.getConfigurationFile()));
		} catch (ConfigurationException e) {
			log.error("Configuration file not found (" + descr.getConfigurationFile() + ").", e);
			throw new IllegalArgumentException("Configuration file not found (" + descr.getConfigurationFile() + ").");
		}

		// Load configuration and pass info to the importSet object
		descr.setDbURL(configuration.getString("db.url", ""));
		descr.setDbDriver(configuration.getString("db.driver", ""));
		descr.setDbSchema(configuration.getString("db.schema", ""));
		descr.setDbUser(configuration.getString("db.user", ""));
		descr.setDbPass(configuration.getString("db.password", ""));
		descr.setUdkDbVersion(configuration.getString("udk.db.version", ""));

		if (dbUser != null) {
			descr.setDbUser(dbUser);
		}
		if (dbPasswd != null) {
			descr.setDbPass(dbPasswd);
		}
		if (udkDbVersion != null) {
			descr.setUdkDbVersion(udkDbVersion);
		}
		return descr;
	}

}
