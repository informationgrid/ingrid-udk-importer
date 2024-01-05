/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2024 wemove digital solutions GmbH
 * ==================================================
 * Licensed under the EUPL, Version 1.2 or – as soon they will be
 * approved by the European Commission - subsequent versions of the
 * EUPL (the "Licence");
 * 
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 * 
 * https://joinup.ec.europa.eu/software/page/eupl
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
package de.ingrid.importer.udk;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
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
	private static Log log = LogFactory.getLog(ImportDescriptorHelper.class);

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
	
	private static boolean deleteDirectory(File path) {
		if( path.exists() ) {
	      File[] files = path.listFiles();
	      for(int i=0; i<files.length; i++) {
	         if(files[i].isDirectory()) {
	           deleteDirectory(files[i]);
	         }
	         else {
	           files[i].delete();
	         }
	      }
	    }
	    return( path.delete() );
	  }	
	

	private static List<String> extractZipFile(InputStream inStream, final String targetDir) {

		boolean keepSubfolders = false;

		String[] accFiles = { ".xml" };
		ZipperFilter zipperFilter = new ZipperFilter(accFiles);

		return Zipper.extractZipFile(inStream, targetDir, keepSubfolders, zipperFilter);
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
	
	public static void removeTempDir() {
		deleteDirectory(new File(getTmpDir()));
	}
	

	public static ImportDescriptor getDescriptor(String[] args) {
		ImportDescriptor descr = new ImportDescriptor();
		String dbUrl = null;
		String dbDriver = null;
		String dbUser = null;
		String dbPasswd = null;
		String idcLanguage = null;
		String idcEmail = null;
		String idcName = null;
		String idcPartner = null;
		String idcProvider = null;
		String idcCountry = null;
		String idcVersion = null;

		for (int i = 0; i < args.length; i++) {
			if (args[i].equalsIgnoreCase("-u")) {
				if (args.length <= i + 1) {
					String msg = "Missing argument for parameter '-u' (Database User).";
					System.out.println(msg);
					throw new IllegalArgumentException(msg);
				}
				dbUser = args[++i];
			} else if (args[i].equalsIgnoreCase("-p")) {
				if (args.length <= i + 1) {
					String msg = "Missing argument for parameter '-p' (Database Password).";
					System.out.println(msg);
					throw new IllegalArgumentException(msg);
				}
				dbPasswd = args[++i];
			} else if (args[i].equalsIgnoreCase("-c")) {
				if (args.length <= i + 1) {
					String msg = "Missing argument for parameter '-c' (Configuration File).";
					System.out.println(msg);
					throw new IllegalArgumentException(msg);
				}
				descr.setConfigurationFile(args[++i]);
			} else if (args[i].equalsIgnoreCase("-v")) {
				if (args.length <= i + 1) {
					String msg = "Missing argument for parameter '-v' (IDC Version).";
					System.out.println(msg);
					throw new IllegalArgumentException(msg);
				}
				idcVersion = args[++i];
			} else if (args[i].equalsIgnoreCase("-l")) {
				if (args.length <= i + 1) {
					String msg = "Missing argument for parameter '-l' (Catalogue Language).";
					System.out.println(msg);
					throw new IllegalArgumentException(msg);
				}
				idcLanguage = args[++i];
			} else if (args[i].equalsIgnoreCase("-dburl")) {
				if (args.length <= i + 1) {
					String msg = "Missing argument for parameter '-dburl' (Database Url).";
					System.out.println(msg);
					throw new IllegalArgumentException(msg);
				}
				dbUrl = args[++i];
			} else if (args[i].equalsIgnoreCase("-dbdriver")) {
				if (args.length <= i + 1) {
					String msg = "Missing argument for parameter '-dbdriver' (Database Driver).";
					System.out.println(msg);
					throw new IllegalArgumentException(msg);
				}
				dbDriver = args[++i];
			} else if (args[i].equalsIgnoreCase("-email")) {
				if (args.length <= i + 1) {
					String msg = "Missing argument for parameter '-email' (Catalogue Admin E-Mail).";
					System.out.println(msg);
					throw new IllegalArgumentException(msg);
				}
				idcEmail = args[++i];
			} else if (args[i].equalsIgnoreCase("-name")) {
				if (args.length <= i + 1) {
					String msg = "Missing argument for parameter '-name' (Catalogue Name).";
					System.out.println(msg);
					throw new IllegalArgumentException(msg);
				}
				idcName = args[++i];
			} else if (args[i].equalsIgnoreCase("-partner")) {
				if (args.length <= i + 1) {
					String msg = "Missing argument for parameter '-partner' (Catalogue Partner full name).";
					System.out.println(msg);
					throw new IllegalArgumentException(msg);
				}
				idcPartner = args[++i];
			} else if (args[i].equalsIgnoreCase("-provider")) {
				if (args.length <= i + 1) {
					String msg = "Missing argument for parameter '-provider' (Catalogue Provider full name).";
					System.out.println(msg);
					throw new IllegalArgumentException(msg);
				}
				idcProvider = args[++i];
			} else if (args[i].equalsIgnoreCase("-country")) {
				if (args.length <= i + 1) {
					String msg = "Missing argument for parameter '-country' (Catalogue Country code).";
					System.out.println(msg);
					throw new IllegalArgumentException(msg);
				}
				idcCountry = args[++i];
			} else {
				addDataFile(args[i], descr);
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
		descr.setIdcVersion(configuration.getString("idc.version", ""));
		descr.setIdcCatalogueLanguage(configuration.getString("idc.catalogue.language", ""));
		descr.setIdcEmailDefault(configuration.getString("idc.email.default", ""));
		descr.setIdcProfileFileName(configuration.getString("idc.profile.file", ""));

		descr.setIdcCatalogueName(configuration.getString("idc.catalogue.name", ""));
		descr.setIdcPartnerName(configuration.getString("idc.partner.name", ""));
		descr.setIdcProviderName(configuration.getString("idc.provider.name", ""));
		descr.setIdcCatalogueCountry(configuration.getString("idc.catalogue.country", ""));

		// set passed command line attributes, have highest prio !
		if (dbUser != null) {
			descr.setDbUser(dbUser);
		}
		if (dbPasswd != null) {
			descr.setDbPass(dbPasswd);
		}
		if (idcVersion != null) {
			descr.setIdcVersion(idcVersion);
		}
		if (idcLanguage != null) {
			descr.setIdcCatalogueLanguage(idcLanguage);
		}
		if (dbUrl != null) {
			descr.setDbURL(dbUrl);
		}
		if (dbDriver != null) {
			descr.setDbDriver(dbDriver);
		}
		if (idcEmail != null) {
			descr.setIdcEmailDefault(idcEmail);
		}
		if (idcName != null) {
			descr.setIdcCatalogueName(idcName);
		}
		if (idcPartner != null) {
			descr.setIdcPartnerName(idcPartner);
		}
		if (idcProvider != null) {
			descr.setIdcProviderName(idcProvider);
		}
		if (idcCountry != null) {
			descr.setIdcCatalogueCountry(idcCountry);
		}
		
		return descr;
	}

	/** Analyses the given fileName and adds data files to passed descriptor.
	 * @param fileName file name (with absolute path or not) or directory or ... 
	 * @param descr the descriptor where to add data
	 */
	public static void addDataFile(String fileName, ImportDescriptor descr) {
		if (fileName == null || fileName.trim().length() == 0) {
			return;
		}

		if (log.isInfoEnabled()) {
			log.info("Try adding UDK data file '" + fileName + "'");
		}

		File f = new File(fileName);
		InputStream myInputStream = null;

		// try to load from class path if not existent !
		if (!f.exists()) {
			URL url = descr.getClass().getResource(fileName);
			if (log.isInfoEnabled()) {
				log.info("File not found, try loading via classLoader, resulting url='" + url.getPath() + "'");
			}

			if (url != null) {
				f = new File(url.getPath());
				if (log.isInfoEnabled()) {
					log.info("Resulting file='" + f + "', file exists=" + f.exists());
				}

				if (!f.exists()) {
					myInputStream = descr.getClass().getResourceAsStream(fileName);
					if (log.isInfoEnabled()) {
						log.info("Try loading as stream, e.g. from jar file -> Resulting InputStream=" + myInputStream);
					}					
				}
			}
		}
		
		if (!f.exists() && myInputStream == null) {
			log.warn("File/Directory '" + fileName + "' not found, ignored !");
			return;
		}

		if (f.exists() && f.isDirectory()) {
			// read directory
			ImportDescriptorHelper.addDirectory(f, descr.getFiles());
		} else if (f.getName().endsWith(".zip")) {
			if (log.isDebugEnabled()) {
				log.debug("Zip archive found. Extracting to " + getTmpDir());
			}
			// we always extract via stream to be able to work with zip resources from jar !
			if (myInputStream == null) {
				// we have a file, not a stream from jar, so we get the stream to the file
				try {
					myInputStream = new FileInputStream(f);					
				} catch (Exception ex) {
					log.warn("Problems getting stream from file " + f + ", ignored !");
					return;
				}
			}
			List<String> vExtracted = extractZipFile(myInputStream, getTmpDir() + "/");

			if (vExtracted != null) {
				for (int j = 0; j < vExtracted.size(); j++) {
					descr.getFiles().add(vExtracted.get(j));
				}
			} else {
				log.error("Error unzipping '" + f.getName() + "'.");
				throw new IllegalArgumentException("Error unzipping '" + f.getName() + "'.");
			}
		} else if (f.exists() && f.isFile()) {
			// read file
			descr.getFiles().add(f.getAbsolutePath());
		}
	}

}
