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

/**
 * @author joachim
 *
 */
public class ImporterHelper {

	/**
	 * The logging object
	 */
	private static Log log = LogFactory.getLog(Importer.class);

    /**
     * Adds all files (suffix '.xml') of the given directory
     * <code>dir</code>.
     * 
     * @param dir
     *            The directory
     * @param overwrite
     *            Overwrite existing data on database?
     */
    public static void addDirectory(final File dir, List<String> fileList) {
        String[] entries = dir.list();

        String file = "";
        for (int i = 0; i < entries.length; i++) {
            if (entries[i].endsWith(".xml")) {        
                file = dir + "/" + entries[i];
                fileList.add(file);
            }
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
					ImporterHelper.addDirectory(f, descr.getFiles());
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
