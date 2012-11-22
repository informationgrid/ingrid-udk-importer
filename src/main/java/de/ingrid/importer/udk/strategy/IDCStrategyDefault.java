/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.ImportDescriptor;
import de.ingrid.importer.udk.jdbc.JDBCConnectionProxy;
import de.ingrid.importer.udk.provider.DataProvider;
import de.ingrid.importer.udk.util.UuidGenerator;

/**
 * @author Administrator
 * 
 */
public abstract class IDCStrategyDefault implements IDCStrategy {

	private static Log log = LogFactory.getLog(IDCStrategyDefault.class);

	protected DataProvider dataProvider = null;
	protected ImportDescriptor importDescriptor = null;
	protected JDBCConnectionProxy jdbc = null;

	protected String sqlStr = null;
	protected String pSqlStr = null;

	protected static List<String> invalidModTypes;

	private String catalogLanguage = null;
	private String catalogAdminUuid = null;

	// maps containing new syslists <entryId, name>
	protected HashMap<Integer, String> mapNewKeyToNewValueList100;
	protected HashMap<Integer, String> mapNewKeyToNewValueList101;

	// data for generating id's via hibernate HiLow mechanism (table hibernate_unique_key)
	private long idFromHiLow = -1;
	private long startIdNextHiLowBlock = -1;
	private short numIdsInHiLowBlock = Short.MAX_VALUE;

	public IDCStrategyDefault() {
		super();
		invalidModTypes = Arrays.asList(DataProvider.invalidModTypes);
		
		// default language
		catalogLanguage = "de";

		// new syslist 100
		// KEEP OLD ENTRY IDs, see http://88.198.11.89/jira/browse/INGRIDII-245
		mapNewKeyToNewValueList100 = new HashMap<Integer, String>();
		mapNewKeyToNewValueList100.put(3068, "EPSG 3068: DHDN / Soldner Berlin");
		mapNewKeyToNewValueList100.put(4178, "EPSG 4178: Pulkovo 1942(83) / geographisch");
		mapNewKeyToNewValueList100.put(4230, "EPSG 4230: ED50 / geographisch");
		mapNewKeyToNewValueList100.put(4258, "EPSG 4258: ETRS89 / geographisch");
		mapNewKeyToNewValueList100.put(4284, "EPSG 4284: Pulkovo 1942 / geographisch");
		mapNewKeyToNewValueList100.put(4314, "EPSG 4314: DHDN / geographisch");
		mapNewKeyToNewValueList100.put(4326, "EPSG 4326: WGS 84 / geographisch");
		mapNewKeyToNewValueList100.put(23031, "EPSG 23031: ED50 / UTM Zone 31N");
		mapNewKeyToNewValueList100.put(23032, "EPSG 23032: ED50 / UTM Zone 32N");
		mapNewKeyToNewValueList100.put(23033, "EPSG 23033: ED50 / UTM Zone 33N");
		mapNewKeyToNewValueList100.put(32631, "EPSG 32631: WGS 84 / UTM Zone 31N");
		mapNewKeyToNewValueList100.put(32632, "EPSG 32632: WGS 84 / UTM Zone 32N");
		mapNewKeyToNewValueList100.put(32633, "EPSG 32633: WGS 84 / UTM Zone 33N");
		mapNewKeyToNewValueList100.put(25831, "EPSG 25831: ETRS89 / UTM Zone 31N");
		mapNewKeyToNewValueList100.put(25832, "EPSG 25832: ETRS89 / UTM Zone 32N");
		mapNewKeyToNewValueList100.put(25833, "EPSG 25833: ETRS89 / UTM Zone 33N");
		mapNewKeyToNewValueList100.put(25834, "EPSG 25834: ETRS89 / UTM Zone 34N");
		mapNewKeyToNewValueList100.put(28462, "EPSG 28462: Pulkovo 1942 / Gauss-Krüger 2N");
		mapNewKeyToNewValueList100.put(28463, "EPSG 28463: Pulkovo 1942 / Gauss-Krüger 3N");
		mapNewKeyToNewValueList100.put(31466, "EPSG 31466: DHDN / Gauss-Krüger Zone 2");
		mapNewKeyToNewValueList100.put(31467, "EPSG 31467: DHDN / Gauss-Krüger Zone 3");
		mapNewKeyToNewValueList100.put(31468, "EPSG 31468: DHDN / Gauss-Krüger Zone 4");
		mapNewKeyToNewValueList100.put(31469, "EPSG 31469: DHDN / Gauss-Krüger Zone 5");
		mapNewKeyToNewValueList100.put(9000001, "DE_42/83 / GK_3");
		mapNewKeyToNewValueList100.put(9000002, "DE_DHDN / GK_3");
		mapNewKeyToNewValueList100.put(9000007, "DE_DHDN / GK_3_RDN");
		mapNewKeyToNewValueList100.put(9000008, "DE_DHDN / GK_3_RP101");
		mapNewKeyToNewValueList100.put(9000009, "DE_DHDN / GK_3_RP180");
		mapNewKeyToNewValueList100.put(9000010, "DE_DHDN / GK_3_NW177");
		mapNewKeyToNewValueList100.put(9000011, "DE_DHDN / GK_3_HE100");
		mapNewKeyToNewValueList100.put(9000012, "DE_DHDN / GK_3_BW100");
		mapNewKeyToNewValueList100.put(9000003, "DE_ETRS89 / UTM");
		mapNewKeyToNewValueList100.put(9000005, "DE_PD/83 / GK_3");
		mapNewKeyToNewValueList100.put(9000006, "DE_RD/83 / GK_3");
		mapNewKeyToNewValueList100.put(9000013, "DE_PD/83 / GK_9-15, Bezug 12. Meridian (BY)");

		// new syslist 101
		// KEEP OLD ENTRY IDs, see http://88.198.11.89/jira/browse/INGRIDII-245
		mapNewKeyToNewValueList101 = new HashMap<Integer, String>();
		mapNewKeyToNewValueList101.put(900002, "DE_AMST / NH");
		mapNewKeyToNewValueList101.put(900003, "DE_AMST / NOH");
		mapNewKeyToNewValueList101.put(900005, "DE_DHHN12_NOH");
		mapNewKeyToNewValueList101.put(900006, "DE_DHHN12_RP120");
		mapNewKeyToNewValueList101.put(900007, "DE_DHHN85_NOH");
		mapNewKeyToNewValueList101.put(900008, "DE_DHHN92_NH");
		mapNewKeyToNewValueList101.put(900004, "DE_KRON / NH");
		mapNewKeyToNewValueList101.put(900009, "Horizont 74_NOH");
		mapNewKeyToNewValueList101.put(5129, "European Vertical Reference Frame 2000");
		mapNewKeyToNewValueList101.put(5105, "Baltic Sea");
		mapNewKeyToNewValueList101.put(900010, "Höhe über GRS80 Ellipsoid");
	}

	public void setDataProvider(DataProvider data) {
		dataProvider = data;
	}

	public void setJDBCConnectionProxy(JDBCConnectionProxy jdbc) {
		this.jdbc = jdbc;
	}

	public ImportDescriptor getImportDescriptor() {
		return importDescriptor;
	}
	public void setImportDescriptor(ImportDescriptor descriptor) {
		importDescriptor = descriptor;
		
		String catLang = descriptor.getIdcCatalogueLanguage();
		if (catLang != null && catLang.trim().length() > 0) {
			catalogLanguage = catLang;
		}
	}

	protected String getCatalogAdminUuid() {
		if (catalogAdminUuid == null) {
			catalogAdminUuid = UuidGenerator.getInstance().generateUuid();
		}
		return catalogAdminUuid;
	}
	protected String getCatalogLanguage() {
		return catalogLanguage;
	}

	protected void setHiLoGeneratorViaId(long lastId) throws SQLException {
		sqlStr = "DELETE FROM hibernate_unique_key";
		jdbc.executeUpdate(sqlStr);

		sqlStr = "INSERT INTO hibernate_unique_key (next_hi) VALUES (" + (int)(lastId / numIdsInHiLowBlock + 1) + ")";
		jdbc.executeUpdate(sqlStr);
	}

	protected void setHiLoGeneratorNextHi(long nextHi) throws SQLException {
		sqlStr = "DELETE FROM hibernate_unique_key";
		jdbc.executeUpdate(sqlStr);

		sqlStr = "INSERT INTO hibernate_unique_key (next_hi) VALUES (" + nextHi + ")";
		jdbc.executeUpdate(sqlStr);
	}

	synchronized private void initializeIdFromHiLoGenerator() throws Exception {
		if (idFromHiLow == -1) {
			String sql = "SELECT next_hi FROM hibernate_unique_key";
			try {
				Statement st = jdbc.createStatement();
				ResultSet rs = jdbc.executeQuery(sql, st);
				// has to be there !!!
				rs.next();

				long nextHi = rs.getLong(1);
				idFromHiLow = nextHi * numIdsInHiLowBlock;
				
				// immediately update HiLow generator to guarantee correct state in database (next block of id's) 
				setHiLoGeneratorNextHi(nextHi + 1);
				startIdNextHiLowBlock = (nextHi + 1) * numIdsInHiLowBlock;

				rs.close();
				st.close();
			} catch (SQLException e) {
				log.error("Error executing SQL: " + sql, e);
				throw e;
			}
		}
	}

	/** Get next id ! is initialized once from HiLow generator in database (Hibernate) ! */
	protected long getNextId() throws Exception {
		if (idFromHiLow == -1) {
			initializeIdFromHiLoGenerator();
		}

		// check whether next id is in next HiLow block (more than numIdsInHiLowBlock = 32767 ids generated)
		// then update HiLowGenerator !
		long nextId = idFromHiLow++;
		if (nextId == startIdNextHiLowBlock) {
			setHiLoGeneratorViaId(nextId);
			startIdNextHiLowBlock = startIdNextHiLowBlock + numIdsInHiLowBlock;
		}
		return nextId;
	}

	/** New Version WITH ID column !!! */
	protected void setGenericKey(String key, String value) throws Exception {
		jdbc.executeUpdate("DELETE FROM sys_generic_key WHERE key_name='" + key + "'");

		// use PreparedStatement to avoid problems when value String contains "'" !!!
		sqlStr = "INSERT INTO sys_generic_key (id, key_name, value_string) " +
			"VALUES (" + getNextId() + ", ?, ?)";
		
		PreparedStatement p = jdbc.prepareStatement(sqlStr);
		p.setString(1, key);
		p.setString(2, value);
		p.executeUpdate();

		p.close();
	}

	/** Read string value of given generic key from database.
	 * @param key key name to read value from
	 * @return null if key not found or value of key is null.
	 */
	protected String readGenericKey(String key) throws Exception {
		String retValue = null;

		sqlStr = "SELECT value_string FROM sys_generic_key WHERE key_name = ?";
		PreparedStatement ps = jdbc.prepareStatement(sqlStr);
		ps.setString(1, key);

		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			retValue = rs.getString("value_string");
		}
		rs.close();
		ps.close();

		return retValue;
	}

    /** Convert InputStream to String.
     * @param is the stream
     * @param charsetName pass null to use default "UTF-8"
     * @return String ore "" if passed InputStream is null
     * @throws IOException
     */
    public String convertStreamToString(InputStream is, String charsetName) throws IOException {
    	/*
    	 * To convert the InputStream to String we use the
    	 * Reader.read(char[] buffer) method. We iterate until the
    	 * Reader return -1 which means there's no more data to
    	 * read. We use the StringWriter class to produce the string.
    	 */
    	if (is != null) {
    	    Writer writer = new StringWriter();

    	    char[] buffer = new char[1024];
    	    try {
    	        Reader reader = new BufferedReader(
    	                new InputStreamReader(is, (charsetName == null ? "UTF-8" : charsetName)));
    	        int n;
    	        while ((n = reader.read(buffer)) != -1) {
    	            writer.write(buffer, 0, n);
    	        }
    	    } finally {
    	        is.close();
    	    }
    	    return writer.toString();
    	} else {        
    	    return "";
    	}
    }
}
