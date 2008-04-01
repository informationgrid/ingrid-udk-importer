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
		jdbc.executeUpdate("DELETE FROM search_index WHERE entity_id = " + objId + " AND entity_type = 'O' AND idx_name = 'full'");
		
		String pSqlStr = "INSERT INTO search_index (id, entity_id, entity_type, idx_name, idx_value) VALUES (?, ?, 'O', 'full', '')";
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
		String pSqlStr = "UPDATE search_index SET idx_value = concat(idx_value, ?) WHERE entity_id = ? AND entity_type = 'O' AND idx_name = 'full'";
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
}
