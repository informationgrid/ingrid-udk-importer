/**
 * 
 */
package de.ingrid.importer.udk.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategy;

/**
 * @author joachim
 *
 */
public class JDBCHelper {

	private static Log log = LogFactory.getLog(JDBCHelper.class);
	
	public static void addInteger(PreparedStatement p, int cnt, Integer val) throws SQLException {
		if (val == null) {
			p.setNull(cnt, Types.INTEGER);
		} else {
			p.setInt(cnt, val);
		}
	}

	public static void addString(PreparedStatement p, int cnt, String val) throws SQLException {
		if (val == null) {
			p.setNull(cnt, Types.VARCHAR);
		} else {
			p.setString(cnt, val);
		}
	}
	
	public static void addDouble(PreparedStatement p, int cnt, Double val) throws SQLException {
		if (val == null) {
			p.setNull(cnt, Types.DOUBLE);
		} else {
			p.setDouble(cnt, val);
		}
	}
	
	public static void createObjectIndex(long id, long objId, String idxName, JDBCConnectionProxy jdbc) throws Exception {
		jdbc.executeUpdate("DELETE FROM full_index_obj WHERE obj_node_id = " + objId + " AND idx_name = '" + idxName + "'");
		
		String pSqlStr = "INSERT INTO full_index_obj (id, obj_node_id, idx_name, idx_value) VALUES (?, ?, ?, '')";
		PreparedStatement p = jdbc.prepareStatement(pSqlStr);
		p.setLong(1, id);
		p.setLong(2, objId);
		p.setString(3, idxName);
		
		try {
			p.executeUpdate();
		} catch (Exception e) {
			log.error("Error executing SQL: " + p.toString(), e);
			throw e;
		}
	}

	public static void updateObjectIndex(long objId, String token, JDBCConnectionProxy jdbc) throws Exception {
		JDBCHelper.updateObjectIndex(objId, token, "full", jdbc);
	}

	public static void updateObjectIndex(long objId, String token, String idxName, JDBCConnectionProxy jdbc) throws Exception {
		if (token==null || token.length() == 0) {
			return;
		}
		String pSqlStr = "UPDATE full_index_obj SET idx_value = concat(idx_value, ?) WHERE obj_node_id = ? AND idx_name = '" + idxName + "'";
		PreparedStatement p = jdbc.prepareStatement(pSqlStr);
		p.setString(1, "|" + token);
		p.setLong(2, objId);
		
		try {
			p.executeUpdate();
		} catch (Exception e) {
			log.error("Error executing SQL: " + p.toString(), e);
			throw e;
		}
	}

	public static void createAddressIndex(long id, long addrId, String idxName, JDBCConnectionProxy jdbc) throws Exception {
		jdbc.executeUpdate("DELETE FROM full_index_addr WHERE addr_node_id = " + addrId + " AND idx_name = '" + idxName + "'");
		
		String pSqlStr = "INSERT INTO full_index_addr (id, addr_node_id, idx_name, idx_value) VALUES (?, ?, ?, '')";
		PreparedStatement p = jdbc.prepareStatement(pSqlStr);
		p.setLong(1, id);
		p.setLong(2, addrId);
		p.setString(3, idxName);
		
		try {
			p.executeUpdate();
		} catch (Exception e) {
			log.error("Error executing SQL: " + p.toString(), e);
			throw e;
		}
	}

	public static void updateAddressIndex(long addrId, String token, JDBCConnectionProxy jdbc) throws Exception {
		JDBCHelper.updateAddressIndex(addrId, token, "full", jdbc);
	}

	public static void updateAddressIndex(long addrId, String token, String idxName, JDBCConnectionProxy jdbc) throws Exception {
		if (token==null || token.length() == 0) {
			return;
		}
		String pSqlStr = "UPDATE full_index_addr SET idx_value = concat(idx_value, ?) WHERE addr_node_id = ? AND idx_name = '" + idxName + "'";
		PreparedStatement p = jdbc.prepareStatement(pSqlStr);
		p.setString(1, "|" + token);
		p.setLong(2, addrId);
		
		try {
			p.executeUpdate();
		} catch (Exception e) {
			log.error("Error executing SQL: " + p.toString(), e);
			throw e;
		}
	}

	/**
	 * Fetch IDC Version from IDC catalog. Returns null if no version set. Throws Exception if problems. 
	 * @param jdbc connection
	 * @param processVersion process the fetched Version, meaning remove info at end like "_dev" etc.
	 * @return the current version, null if no version set meaning initial state !
	 * @throws Exception
	 */
	public static String getCurrentIDCVersion(JDBCConnectionProxy jdbc, boolean processVersion) throws Exception {
		String currentVersion = null;

		String sql = "SELECT value_string FROM sys_generic_key WHERE key_name='" + IDCStrategy.KEY_IDC_VERSION + "'";
		try {
			ResultSet rs = jdbc.executeQuery(sql);
			if (rs.next()) {
				currentVersion = rs.getString(1);
			}
			rs.close();
		} catch (SQLException e) {
			log.error("Error executing SQL: " + sql, e);
			throw e;
		}
		
		if (processVersion && currentVersion != null) {
			int index = currentVersion.indexOf("_dev");
			if (index != -1) {
				currentVersion = currentVersion.substring(0,index);
			}
		}

		return currentVersion;
	}
}
