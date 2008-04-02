/**
 * 
 */
package de.ingrid.importer.udk.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

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
	
	public static void createObjectIndex(long id, long objId, JDBCConnectionProxy jdbc) throws Exception {
		jdbc.executeUpdate("DELETE FROM full_index_obj WHERE obj_id = " + objId + " AND idx_name = 'full'");
		
		String pSqlStr = "INSERT INTO full_index_obj (id, obj_id, idx_name, idx_value) VALUES (?, ?, 'full', '')";
		PreparedStatement p = jdbc.prepareStatement(pSqlStr);
		p.setLong(1, id);
		p.setLong(2, objId);
		
		try {
			p.executeUpdate();
		} catch (Exception e) {
			log.error("Error executing SQL: " + p.toString(), e);
			throw e;
		}
	}

	public static void updateObjectIndex(long objId, String token, JDBCConnectionProxy jdbc) throws Exception {
		if (token==null || token.length() == 0) {
			return;
		}
		String pSqlStr = "UPDATE full_index_obj SET idx_value = concat(idx_value, ?) WHERE obj_id = ? AND idx_name = 'full'";
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

	public static void createAddressIndex(long id, long addrId, JDBCConnectionProxy jdbc) throws Exception {
		jdbc.executeUpdate("DELETE FROM full_index_addr WHERE addr_id = " + addrId + " AND idx_name = 'full'");
		
		String pSqlStr = "INSERT INTO full_index_addr (id, addr_id, idx_name, idx_value) VALUES (?, ?, 'full', '')";
		PreparedStatement p = jdbc.prepareStatement(pSqlStr);
		p.setLong(1, id);
		p.setLong(2, addrId);
		
		try {
			p.executeUpdate();
		} catch (Exception e) {
			log.error("Error executing SQL: " + p.toString(), e);
			throw e;
		}
	}

	public static void updateAddressIndex(long addrId, String token, JDBCConnectionProxy jdbc) throws Exception {
		if (token==null || token.length() == 0) {
			return;
		}
		String pSqlStr = "UPDATE full_index_addr SET idx_value = concat(idx_value, ?) WHERE addr_id = ? AND idx_name = 'full'";
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

}
