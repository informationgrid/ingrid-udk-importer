/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2018 wemove digital solutions GmbH
 * ==================================================
 * Licensed under the EUPL, Version 1.1 or – as soon they will be
 * approved by the European Commission - subsequent versions of the
 * EUPL (the "Licence");
 * 
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 * 
 * http://ec.europa.eu/idabc/eupl5
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
package de.ingrid.importer.udk.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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

	public static String IDX_TOKEN_SEPARATOR = "|";  

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
	
	public static void createObjectIndex(long id, long objNodeId, String idxName, JDBCConnectionProxy jdbc) throws Exception {
		jdbc.executeUpdate("DELETE FROM full_index_obj WHERE obj_node_id = " + objNodeId + " AND idx_name = '" + idxName + "'");
		
		String pSqlStr = "INSERT INTO full_index_obj (id, obj_node_id, idx_name, idx_value) VALUES (?, ?, ?, '')";
		PreparedStatement p = jdbc.prepareStatement(pSqlStr);
		p.setLong(1, id);
		p.setLong(2, objNodeId);
		p.setString(3, idxName);
		
		try {
			p.executeUpdate();
			p.close();
		} catch (Exception e) {
			log.error("Error executing SQL: " + p.toString(), e);
			throw e;
		}
	}

	/** NOTICE: we add pre- and post-separator to token before writing into index ! */
	public static void updateObjectIndex(long objNodeId, String token, JDBCConnectionProxy jdbc) throws Exception {
		JDBCHelper.updateObjectIndex(objNodeId, token, "full", jdbc);
	}

	/** NOTICE: we add pre- and post-separator to token before writing into index ! */
	public static void updateObjectIndex(long objNodeId, String token, String idxName, JDBCConnectionProxy jdbc) throws Exception {
		if (token==null || token.length() == 0) {
			return;
		}
		String pSqlStr = "UPDATE full_index_obj SET idx_value = concat(idx_value, ?) WHERE obj_node_id = ? AND idx_name = '" + idxName + "'";
		PreparedStatement p = jdbc.prepareStatement(pSqlStr);
		p.setString(1, IDX_TOKEN_SEPARATOR + token + IDX_TOKEN_SEPARATOR);
		p.setLong(2, objNodeId);
		
		try {
			p.executeUpdate();
			p.close();
		} catch (Exception e) {
			log.error("Error executing SQL: " + p.toString(), e);
			throw e;
		}
	}

	public static void createAddressIndex(long id, long addrNodeId, String idxName, JDBCConnectionProxy jdbc) throws Exception {
		jdbc.executeUpdate("DELETE FROM full_index_addr WHERE addr_node_id = " + addrNodeId + " AND idx_name = '" + idxName + "'");
		
		String pSqlStr = "INSERT INTO full_index_addr (id, addr_node_id, idx_name, idx_value) VALUES (?, ?, ?, '')";
		PreparedStatement p = jdbc.prepareStatement(pSqlStr);
		p.setLong(1, id);
		p.setLong(2, addrNodeId);
		p.setString(3, idxName);
		
		try {
			p.executeUpdate();
			p.close();
		} catch (Exception e) {
			log.error("Error executing SQL: " + p.toString(), e);
			throw e;
		}
	}

	/** NOTICE: we add pre- and post-separator to token before writing into index ! */
	public static void updateAddressIndex(long addrNodeId, String token, JDBCConnectionProxy jdbc) throws Exception {
		JDBCHelper.updateAddressIndex(addrNodeId, token, "full", jdbc);
	}

	/** NOTICE: we add pre- and post-separator to token before writing into index ! */
	public static void updateAddressIndex(long addrNodeId, String token, String idxName, JDBCConnectionProxy jdbc) throws Exception {
		if (token==null || token.length() == 0) {
			return;
		}
		String pSqlStr = "UPDATE full_index_addr SET idx_value = concat(idx_value, ?) WHERE addr_node_id = ? AND idx_name = '" + idxName + "'";
		PreparedStatement p = jdbc.prepareStatement(pSqlStr);
		if (token.length() > 3998 && jdbc.isOracle()) {
			p.setString(1, IDX_TOKEN_SEPARATOR + token.substring(0, 3998) + IDX_TOKEN_SEPARATOR);
		} else {
			p.setString(1, IDX_TOKEN_SEPARATOR + token + IDX_TOKEN_SEPARATOR);
		}
		p.setLong(2, addrNodeId);
		
		try {
			p.executeUpdate();
			p.close();
		} catch (Exception e) {
			log.error("Error executing SQL: " + p.toString(), e);
			throw e;
		}
	}

	/**
	 * Fetch IDC Version from IDC catalog. Returns null if no version set. Throws Exception if problems. 
	 * @param jdbc connection
	 * @param processVersion process the fetched Version, meaning remove temporary info "_dev" at end
	 * @return the current version, null if no version set meaning initial state !
	 * @throws Exception
	 */
	public static String getCurrentIDCVersion(JDBCConnectionProxy jdbc, boolean processVersion) throws Exception {
		String currentVersion = null;

		String sql = "SELECT value_string FROM sys_generic_key WHERE key_name='" + IDCStrategy.KEY_IDC_VERSION + "'";
		try {
			Statement st = jdbc.createStatement();
			ResultSet rs = jdbc.executeQuery(sql, st);
			if (rs.next()) {
				currentVersion = rs.getString(1);
			}
			rs.close();
			st.close();
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
