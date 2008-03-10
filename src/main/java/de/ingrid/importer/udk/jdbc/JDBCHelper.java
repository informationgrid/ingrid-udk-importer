/**
 * 
 */
package de.ingrid.importer.udk.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/**
 * @author joachim
 *
 */
public class JDBCHelper {

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

	
}
