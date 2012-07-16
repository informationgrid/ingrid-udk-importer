/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Changes InGrid 3.2.0: migrate user addresses to separated "hidden" addresses !<br>
 * <p><b>ALSO WRITES .sql FILE for import in mdek database changin the USER ADDRESS UUIDS !!!</b>
 * <p>IGC: User Addresses will be copied to
 * <ul>
 *   <li>new address_node:<br>
 *       - no working version (= published version)<br>
 *       - fk_addr_uuid = "IGE_USER"
 *   <li>new t02_address:<br>
 *       - containing main data, adr_type = 100 (-> IGE_USER)
 *   <li>new t021_communications:<br>
 *       - containing phone and email as normal email and email as free entry emailPointOfContact
 *   <li>new address_metadata (empty, but needed for inner joins)
 * </ul>
 * Then the following tables ARE UPDATED FROM OLD TO NEW ADDR UUID OF USER !
 * <ul>
 *   <li>idc_user.addr_uuid and mod_uuid
 *   <li>idc_group.mod_uuid
 *   <li>t01_object.mod_uuid and responsible_uuid
 *   <li>t02_address.mod_uuid and responsible_uuid
 *   <li>object_metadata.assigner_uuid and reassigner_uuid
 *   <li>address_metadata.assigner_uuid and reassigner_uuid
 *   <li>object_comment.create_uuid
 *   <li>address_comment.create_uuid
 *   <li>sys_job_info.user_uuid
 *   <li>t03_catalogue.mod_uuid
 * </ul>
 * see https://dev.wemove.com/jira/browse/INGRID32-36
 */
public class IDCStrategy3_2_0_migrateUsers extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_2_0_migrateUsers.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_2_0_MIGRATE_USERS;

	// NOTICE: WE DO NOT IMPORT THIS STUFF FROM MdekUtils (mdek_api.jar) to avoid circular dependency for release.
	// (importer is now included in IGE installer !). Also this is the "frozen" state at 3.2.0
	private static final Integer ADDRESS_TYPE_INSTITUTION = 0;
	private static final Integer HIDDEN_ADDRESS_TYPE_IGE_USER = 100;
	private static final String IGE_USER_PARENT_UUID = "IGE_USER";
	private static final String WORK_STATE_IN_BEARBEITUNG = "B";
	/** Entry ID of PHONE in syslist COMM_TYPE (4430) */
	public final static Integer COMM_TYPE_PHONE = 1;
	/** Entry ID of EMAIL in syslist COMM_TYPE (4430) */
	public final static Integer COMM_TYPE_EMAIL = 3;
	/** Free Entry VALUE of EMAIL for point of contact. Stored with key -1 in communication table ! */
	public final static String COMM_VALUE_EMAIL_POINT_OF_CONTACT = "emailPointOfContact";
	
	/** timestamp, partner and catalog uuid will be added to file name */
	String outputFilename = "igc3.2.0_UpdateMdek";
	String outputEncoding = "UTF-8";

	Writer outputWriter = null;

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		// THEN PERFORM DATA MANIPULATIONS !
		// ---------------------------------

		System.out.print("  Migrate User Addresses...");
		migrateUserAddresses();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Migrate User Addresses finished successfully.");
	}

	private void migrateUserAddresses() throws Exception {
		log.info("\nMigrating User Addresses...");

		// Update/Insert Data -> use PreparedStatements to avoid problems when value String contains "'" !!!

		PreparedStatement psInsertAddressMetadata = jdbc.prepareStatement(
				"INSERT INTO address_metadata (id) VALUES (?)");

		PreparedStatement psInsertAddress = jdbc.prepareStatement(
			"INSERT INTO t02_address " +
			"(id, adr_uuid, adr_type, institution, lastname, firstname, street, postcode, city, work_state, addr_metadata_id) VALUES " +
			"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

		PreparedStatement psInsertCommunication = jdbc.prepareStatement(
				"INSERT INTO t021_communication " +
				"(id, adr_id, line, commtype_key, commtype_value, comm_value) VALUES " +
				"(?, ?, ?, ?, ?, ?)");

		PreparedStatement psInsertAddressNode = jdbc.prepareStatement(
				"INSERT INTO address_node " +
				"(id, addr_uuid, addr_id, fk_addr_uuid, tree_path) VALUES " +
				"(?, ?, ?, ?, ?)");

		PreparedStatement psUpdateCurrentUserAddrUuid = jdbc.prepareStatement(
				"UPDATE idc_user SET " +
				"addr_uuid = ? " +
				"WHERE id = ?");

		PreparedStatement psUpdateAllUserModUuid = jdbc.prepareStatement(
				"UPDATE idc_user SET " +
				"mod_uuid = ? " +
				"WHERE mod_uuid = ?");

		PreparedStatement psUpdateAllGroupModUuid = jdbc.prepareStatement(
				"UPDATE idc_group SET " +
				"mod_uuid = ? " +
				"WHERE mod_uuid = ?");

		PreparedStatement psUpdateAllObjectModUuid = jdbc.prepareStatement(
				"UPDATE t01_object SET " +
				"mod_uuid = ? " +
				"WHERE mod_uuid = ?");

		PreparedStatement psUpdateAllObjectResponsibleUuid = jdbc.prepareStatement(
				"UPDATE t01_object SET " +
				"responsible_uuid = ? " +
				"WHERE responsible_uuid = ?");

		PreparedStatement psUpdateAllAddressModUuid = jdbc.prepareStatement(
				"UPDATE t02_address SET " +
				"mod_uuid = ? " +
				"WHERE mod_uuid = ?");

		PreparedStatement psUpdateAllAddressResponsibleUuid = jdbc.prepareStatement(
				"UPDATE t02_address SET " +
				"responsible_uuid = ? " +
				"WHERE responsible_uuid = ?");

		PreparedStatement psUpdateAllObjectMetadataAssignerUuid = jdbc.prepareStatement(
				"UPDATE object_metadata SET " +
				"assigner_uuid = ? " +
				"WHERE assigner_uuid = ?");

		PreparedStatement psUpdateAllObjectMetadataReassignerUuid = jdbc.prepareStatement(
				"UPDATE object_metadata SET " +
				"reassigner_uuid = ? " +
				"WHERE reassigner_uuid = ?");

		PreparedStatement psUpdateAllAddressMetadataAssignerUuid = jdbc.prepareStatement(
				"UPDATE address_metadata SET " +
				"assigner_uuid = ? " +
				"WHERE assigner_uuid = ?");

		PreparedStatement psUpdateAllAddressMetadataReassignerUuid = jdbc.prepareStatement(
				"UPDATE address_metadata SET " +
				"reassigner_uuid = ? " +
				"WHERE reassigner_uuid = ?");

		PreparedStatement psUpdateAllObjectCommentCreateUuid = jdbc.prepareStatement(
				"UPDATE object_comment SET " +
				"create_uuid = ? " +
				"WHERE create_uuid = ?");

		PreparedStatement psUpdateAllAddressCommentCreateUuid = jdbc.prepareStatement(
				"UPDATE address_comment SET " +
				"create_uuid = ? " +
				"WHERE create_uuid = ?");

		PreparedStatement psUpdateAllJobInfoUserUuid = jdbc.prepareStatement(
				"UPDATE sys_job_info SET " +
				"user_uuid = ? " +
				"WHERE user_uuid = ?");

		PreparedStatement psUpdateAllCatalogModUuid = jdbc.prepareStatement(
				"UPDATE t03_catalogue SET " +
				"mod_uuid = ? " +
				"WHERE mod_uuid = ?");

		int numUserProcessed = 0;
		Statement stUser = jdbc.createStatement();
		ResultSet rsUser = jdbc.executeQuery("select id, addr_uuid from idc_user", stUser);
		while (rsUser.next()) {
			// User
			long userId = rsUser.getLong("id");
			String addrUuid = rsUser.getString("addr_uuid");
			
			AddrHelper addrHelper = readAddress(addrUuid, true, null);
			if (addrHelper == null) {
	    		log.error("PROBLEMS READING USER ADDRESS, we continue !!!" + addrUuid);
	    		continue;
			}
			// post process address, add USER tag, see INGRID32-36
			String userTag = " [Nutzer]";
			if (addrHelper.firstname != null && addrHelper.firstname.length() > 0) {
				addrHelper.firstname += userTag;
			} else if (addrHelper.lastname != null && addrHelper.lastname.length() > 0) {
				addrHelper.lastname += userTag;
			} else if (addrHelper.institution != null && addrHelper.institution.length() > 0) {
				addrHelper.institution += userTag;
			}

			// first insert new address_metadata (empty, but needed for inner joins)
			//-------------------------
			long newAddrMetadataId = getNextId();
			psInsertAddressMetadata.setLong(1, newAddrMetadataId); // id
			int numInserted = psInsertAddressMetadata.executeUpdate();
			if (numInserted > 0) {
				log.info("ADDED " + numInserted + " NEW USER address_metadata (empty)");
			} else {
        		log.error("PROBLEMS ADDING NEW USER address_metadata (empty)");
			}
			
			// then insert new address of user (always IN_BEARBEITUNG)
			//-------------------------
			long newAddrId = getNextId();
			String newAddrUuid = generateUuid();
			String oldAddrUuid = addrHelper.uuid;

			psInsertAddress.setLong(1, newAddrId); // id
			psInsertAddress.setString(2, newAddrUuid); // uuid
			psInsertAddress.setInt(3, HIDDEN_ADDRESS_TYPE_IGE_USER); // adr_type
			psInsertAddress.setString(4, addrHelper.institution);
			psInsertAddress.setString(5, addrHelper.lastname);
			psInsertAddress.setString(6, addrHelper.firstname);
			psInsertAddress.setString(7, addrHelper.street);
			psInsertAddress.setString(8, addrHelper.postcode);
			psInsertAddress.setString(9, addrHelper.city);
			psInsertAddress.setString(10, WORK_STATE_IN_BEARBEITUNG); // work_state
			psInsertAddress.setLong(11, newAddrMetadataId); // addr_metadata_id
			numInserted = psInsertAddress.executeUpdate();
			if (numInserted > 0) {
				log.info("ADDED " + numInserted + " NEW USER t02_address migrated from " + oldAddrUuid + " (" + newAddrUuid + ", " + addrHelper + ")");
			} else {
        		log.error("PROBLEMS ADDING NEW USER t02_address migrated from " + oldAddrUuid + " (" + newAddrUuid + ", " + addrHelper + ")");
			}
			
			// then insert new communication
			//--------------------------------
			int line = 1;

			if (addrHelper.email != null) {
				// first add as normal communication email
				psInsertCommunication.setLong(1, getNextId()); // id
				psInsertCommunication.setLong(2, newAddrId); // adr_id
				psInsertCommunication.setInt(3, line++); // line
				psInsertCommunication.setInt(4, COMM_TYPE_EMAIL); // commtype_key
				psInsertCommunication.setString(5, "E-Mail"); // commtype_value
				psInsertCommunication.setString(6, addrHelper.email); // comm_value
				numInserted = psInsertCommunication.executeUpdate();
				if (numInserted > 0) {
					log.info("ADDED " + numInserted + " NEW USER t021_communication email " + addrHelper.email + " migrated from " + oldAddrUuid);
				} else {
					log.error("PROBLEMS ADDING NEW USER t021_communication email " + addrHelper.email + " migrated from " + oldAddrUuid);
				}

				// then as Metadatenauskunft free email entry
				psInsertCommunication.setLong(1, getNextId()); // id
				psInsertCommunication.setLong(2, newAddrId); // adr_id
				psInsertCommunication.setInt(3, line++); // line
				psInsertCommunication.setInt(4, -1); // commtype_key
				psInsertCommunication.setString(5, COMM_VALUE_EMAIL_POINT_OF_CONTACT); // commtype_value
				psInsertCommunication.setString(6, addrHelper.email); // comm_value
				numInserted = psInsertCommunication.executeUpdate();
				if (numInserted > 0) {
					log.info("ADDED " + numInserted + " NEW USER t021_communication emailPointOfContact " + addrHelper.email + " migrated from " + oldAddrUuid);
				} else {
					log.error("PROBLEMS ADDING NEW USER t021_communication emailPointOfContact " + addrHelper.email + " migrated from " + oldAddrUuid);
				}
			}

			if (addrHelper.phone != null) {
				// first add as normal communication email
				psInsertCommunication.setLong(1, getNextId()); // id
				psInsertCommunication.setLong(2, newAddrId); // adr_id
				psInsertCommunication.setInt(3, line++); // line
				psInsertCommunication.setInt(4, COMM_TYPE_PHONE); // commtype_key
				psInsertCommunication.setString(5, "Telefon"); // commtype_value
				psInsertCommunication.setString(6, addrHelper.phone); // comm_value
				numInserted = psInsertCommunication.executeUpdate();
				if (numInserted > 0) {
					log.info("ADDED " + numInserted + " NEW USER t021_communication phone " + addrHelper.phone + " migrated from " + oldAddrUuid);
				} else {
					log.error("PROBLEMS ADDING NEW USER t021_communication phone " + addrHelper.phone + " migrated from " + oldAddrUuid);
				}
			}

			// then insert new address node (addr_id_published ALWAYS NULL)
			//--------------------------------
			psInsertAddressNode.setLong(1, getNextId()); // id
			psInsertAddressNode.setString(2, newAddrUuid); // adr_uuid
			psInsertAddressNode.setLong(3, newAddrId); // addr_id
			psInsertAddressNode.setString(4, IGE_USER_PARENT_UUID); // fk_addr_uuid
			psInsertAddressNode.setString(5, ""); // tree_path
			numInserted = psInsertAddressNode.executeUpdate();
			if (numInserted > 0) {
				log.info("ADDED " + numInserted + " NEW USER address_node " + newAddrUuid + " with addrId " + newAddrId + " as work / publish");
			} else {
				log.error("PROBLEMS ADDING NEW USER address_node " + newAddrUuid + " with addrId " + newAddrId + " as work / publish");
			}

			// then update idc_user with new address (addr_uuid , mod_uuid)
			//--------------------------------
			// WE UPDATE ONLY CURRENT USER WITH NEW ADDRESS !
			// if the old address uuid is multiple times used for a user (should not happen!) it is copied to a new uuid multiple times !
			psUpdateCurrentUserAddrUuid.setString(1, newAddrUuid); // addr_uuid
			psUpdateCurrentUserAddrUuid.setLong(2, userId); // id
			numInserted = psUpdateCurrentUserAddrUuid.executeUpdate();
			if (numInserted > 0) {
				log.info("UPDATED " + numInserted + " idc_user (id:" + userId + ") -> addr_uuid from " + oldAddrUuid + " to " + newAddrUuid);
			} else {
				log.error("PROBLEMS UPDATING idc_user (id:" + userId + ") -> addr_uuid from " + oldAddrUuid + " to " + newAddrUuid);
			}

			// but for last modification we update ALL users !
			psUpdateAllUserModUuid.setString(1, newAddrUuid); // new mod_uuid
			psUpdateAllUserModUuid.setString(2, oldAddrUuid); // old mod_uuid
			numInserted = psUpdateAllUserModUuid.executeUpdate();
			log.info("UPDATED " + numInserted + " idc_user -> mod_uuid from " + oldAddrUuid + " to " + newAddrUuid);

			// then update all idc_group with new address (mod_uuid)
			//--------------------------------
			psUpdateAllGroupModUuid.setString(1, newAddrUuid); // new mod_uuid
			psUpdateAllGroupModUuid.setString(2, oldAddrUuid); // old mod_uuid
			numInserted = psUpdateAllGroupModUuid.executeUpdate();
			log.info("UPDATED " + numInserted + " idc_group -> mod_uuid from " + oldAddrUuid + " to " + newAddrUuid);

			// then update all objects (mod_uuid, responsible_uuid)
			//--------------------------------
			psUpdateAllObjectModUuid.setString(1, newAddrUuid); // new mod_uuid
			psUpdateAllObjectModUuid.setString(2, oldAddrUuid); // old mod_uuid
			numInserted = psUpdateAllObjectModUuid.executeUpdate();
			log.info("UPDATED " + numInserted + " t01_object -> mod_uuid from " + oldAddrUuid + " to " + newAddrUuid);

			psUpdateAllObjectResponsibleUuid.setString(1, newAddrUuid); // new responsible_uuid
			psUpdateAllObjectResponsibleUuid.setString(2, oldAddrUuid); // old responsible_uuid
			numInserted = psUpdateAllObjectResponsibleUuid.executeUpdate();
			log.info("UPDATED " + numInserted + " t01_object -> responsible_uuid from " + oldAddrUuid + " to " + newAddrUuid);

			// then update all addresses (mod_uuid, responsible_uuid)
			//--------------------------------
			psUpdateAllAddressModUuid.setString(1, newAddrUuid); // new mod_uuid
			psUpdateAllAddressModUuid.setString(2, oldAddrUuid); // old mod_uuid
			numInserted = psUpdateAllAddressModUuid.executeUpdate();
			log.info("UPDATED " + numInserted + " t02_address -> mod_uuid from " + oldAddrUuid + " to " + newAddrUuid);

			psUpdateAllAddressResponsibleUuid.setString(1, newAddrUuid); // new responsible_uuid
			psUpdateAllAddressResponsibleUuid.setString(2, oldAddrUuid); // old responsible_uuid
			numInserted = psUpdateAllAddressResponsibleUuid.executeUpdate();
			log.info("UPDATED " + numInserted + " t02_address -> responsible_uuid from " + oldAddrUuid + " to " + newAddrUuid);

			// then update all object_metadata (assigner_uuid, reassigner_uuid)
			//--------------------------------
			psUpdateAllObjectMetadataAssignerUuid.setString(1, newAddrUuid); // new assigner_uuid
			psUpdateAllObjectMetadataAssignerUuid.setString(2, oldAddrUuid); // old assigner_uuid
			numInserted = psUpdateAllObjectMetadataAssignerUuid.executeUpdate();
			log.info("UPDATED " + numInserted + " object_metadata -> assigner_uuid from " + oldAddrUuid + " to " + newAddrUuid);

			psUpdateAllObjectMetadataReassignerUuid.setString(1, newAddrUuid); // new reassigner_uuid
			psUpdateAllObjectMetadataReassignerUuid.setString(2, oldAddrUuid); // old reassigner_uuid
			numInserted = psUpdateAllObjectMetadataReassignerUuid.executeUpdate();
			log.info("UPDATED " + numInserted + " object_metadata -> reassigner_uuid from " + oldAddrUuid + " to " + newAddrUuid);

			// then update all address_metadata (assigner_uuid, reassigner_uuid)
			//--------------------------------
			psUpdateAllAddressMetadataAssignerUuid.setString(1, newAddrUuid); // new assigner_uuid
			psUpdateAllAddressMetadataAssignerUuid.setString(2, oldAddrUuid); // old assigner_uuid
			numInserted = psUpdateAllAddressMetadataAssignerUuid.executeUpdate();
			log.info("UPDATED " + numInserted + " address_metadata -> assigner_uuid from " + oldAddrUuid + " to " + newAddrUuid);

			psUpdateAllAddressMetadataReassignerUuid.setString(1, newAddrUuid); // new reassigner_uuid
			psUpdateAllAddressMetadataReassignerUuid.setString(2, oldAddrUuid); // old reassigner_uuid
			numInserted = psUpdateAllAddressMetadataReassignerUuid.executeUpdate();
			log.info("UPDATED " + numInserted + " address_metadata -> reassigner_uuid from " + oldAddrUuid + " to " + newAddrUuid);

			// then update all object_comment with new address (create_uuid)
			//--------------------------------
			psUpdateAllObjectCommentCreateUuid.setString(1, newAddrUuid); // new create_uuid
			psUpdateAllObjectCommentCreateUuid.setString(2, oldAddrUuid); // old create_uuid
			numInserted = psUpdateAllObjectCommentCreateUuid.executeUpdate();
			log.info("UPDATED " + numInserted + " object_comment -> create_uuid from " + oldAddrUuid + " to " + newAddrUuid);

			// then update all address_comment with new address (create_uuid)
			//--------------------------------
			psUpdateAllAddressCommentCreateUuid.setString(1, newAddrUuid); // new create_uuid
			psUpdateAllAddressCommentCreateUuid.setString(2, oldAddrUuid); // old create_uuid
			numInserted = psUpdateAllAddressCommentCreateUuid.executeUpdate();
			log.info("UPDATED " + numInserted + " address_comment -> create_uuid from " + oldAddrUuid + " to " + newAddrUuid);

			// then update all sys_job_info with new address (user_uuid)
			//--------------------------------
			psUpdateAllJobInfoUserUuid.setString(1, newAddrUuid); // new user_uuid
			psUpdateAllJobInfoUserUuid.setString(2, oldAddrUuid); // old user_uuid
			numInserted = psUpdateAllJobInfoUserUuid.executeUpdate();
			log.info("UPDATED " + numInserted + " sys_job_info -> user_uuid from " + oldAddrUuid + " to " + newAddrUuid);

			// then update all t03_catalogue with new address (mod_uuid)
			//--------------------------------
			psUpdateAllCatalogModUuid.setString(1, newAddrUuid); // new mod_uuid
			psUpdateAllCatalogModUuid.setString(2, oldAddrUuid); // old mod_uuid
			numInserted = psUpdateAllCatalogModUuid.executeUpdate();
			log.info("UPDATED " + numInserted + " t03_catalogue -> mod_uuid from " + oldAddrUuid + " to " + newAddrUuid);
			
			// finally write our sql statement to file for updating mdek !
			if (outputWriter == null) {
				// add timestamp
				outputFilename = outputFilename + "_" + (System.currentTimeMillis() / 1000);
				// add partner
				String catPartner = getCatalogPartner();
				if (catPartner != null) {
					outputFilename = outputFilename + "_" + catPartner.trim().replaceAll(" ", "");
				}
				// add catalog uuid
				String catUuid = getCatalogUuid();
				if (catUuid != null) {
					outputFilename = outputFilename + "_" + catUuid.trim();
				}
				outputFilename = outputFilename + ".sql";
				log.info("Writing to file: " + outputFilename);

				outputWriter = new OutputStreamWriter(new FileOutputStream(outputFilename), outputEncoding);
			}

			outputWriter.write(
				"UPDATE user_data " +
				"SET addr_uuid = '" + newAddrUuid + "' " +
				"WHERE addr_uuid = '" + oldAddrUuid + "';\n");

			numUserProcessed++;
		}
		rsUser.close();
		stUser.close();
		
		// close Writer if something written !
		if (outputWriter != null) {
			outputWriter.close();

			String msgAlarm = "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";
			String msg = "USERDATA IN IGC CHANGED ! IMPORT the written file '" + outputFilename + "' into your 'mdek' database !!!";

			System.out.println("\n\n" + msgAlarm);
			System.out.println(msg);
			System.out.println(msgAlarm + "\n");

			log.warn(msgAlarm);
			log.warn(msg);
			log.warn(msgAlarm + "\n");
		}

		log.info("Migrated " + numUserProcessed + " User Addresses");
		log.info("Migrating User Addresses... done\n");
	}

	/** Called recursively to extract full institution path ! */
	private AddrHelper readAddress(String addrUuid, boolean alsoReadCommunication, AddrHelper myAddressToExtend) throws Exception {
    	Integer addrType = null;
    	String addrParentUuid = null;

		Statement stNode = jdbc.createStatement();
		ResultSet rsNode = jdbc.executeQuery("select * from address_node where addr_uuid = '" + addrUuid + "'", stNode);
		while (rsNode.next()) {
			long addrNodeId = rsNode.getLong("id");
			long addrIdWork = rsNode.getLong("addr_id");
			long addrIdPubl = rsNode.getLong("addr_id_published");
			addrParentUuid = rsNode.getString("fk_addr_uuid");

			// we migrate published version !
			long addrId = addrIdPubl;
	    	if (addrId == 0) {
	    		log.error("User Address (or Parent) NOT PUBLISHED, we migrate working version !!! " + addrUuid);
	    		addrId = addrIdWork;
	    	} else {
	        	if (addrIdWork != addrIdPubl) {
	        		log.warn("User Address (or Parent) HAS WORKING VERSION, we migrate published version !!! " + addrUuid);
	        	}
	    	}
	    	
	    	// Read Address
    		Statement stAddress = jdbc.createStatement();
    		ResultSet rsAddress = jdbc.executeQuery("select * from t02_address where id = " + addrId, stAddress);
    		while (rsAddress.next()) {
    	    	addrType = rsAddress.getInt("adr_type");
		    	String addrInstitution = rsAddress.getString("institution");
		    	
    			// read / extend data
    	    	if (myAddressToExtend == null) {
    	    		// initial call, this is the user address
    		    	myAddressToExtend = new AddrHelper(addrUuid, addrId, addrNodeId);
    		    	myAddressToExtend.addrType = addrType;
    		    	myAddressToExtend.firstname = rsAddress.getString("firstname");
    		    	myAddressToExtend.lastname = rsAddress.getString("lastname");
    		    	myAddressToExtend.street = rsAddress.getString("street");
    		    	myAddressToExtend.postcode = rsAddress.getString("postcode");
    		    	myAddressToExtend.city = rsAddress.getString("city");
    		    	myAddressToExtend.institution = addrInstitution;
    	    	} else {
    	    		// just extend institution
    	    		if (addrInstitution != null && addrInstitution.trim().length() > 0) {
    	    			if (myAddressToExtend.institution == null) {
    	    				myAddressToExtend.institution = addrInstitution.trim();
    	    			} else {
    	    				myAddressToExtend.institution = addrInstitution.trim() + " / " + myAddressToExtend.institution;
    	    			}
    	    		}
    	    	}

            	// Read Email
        		if (alsoReadCommunication) {
            		Statement stComm = jdbc.createStatement();
            		ResultSet rsComm = jdbc.executeQuery("select * from t021_communication where adr_id = " + addrId, stComm);
            		String freeEmail = null;
            		while (rsComm.next()) {
            			Integer commtypeKey = rsComm.getInt("commtype_key");
            			String commtypeValue = rsComm.getString("commtype_value");
            			String commValue = rsComm.getString("comm_value");
            			if (commValue == null) {
            				continue;
            			}
            			
            			if (commtypeKey.equals(COMM_TYPE_EMAIL)) {
            				myAddressToExtend.email = commValue;
            			} else if (commtypeKey.equals(COMM_TYPE_PHONE)) {
            				myAddressToExtend.phone = commValue;            				
            			} else if (commtypeKey.equals(-1)) {
                			// just for sure, remember email entered as free entry !
                			if (commtypeValue != null && commtypeValue.toLowerCase().contains("mail") && commValue.contains("@")) {
                				freeEmail = commValue;
                			}
            			}
            		}
            		rsComm.close();
            		stComm.close();
            		
            		// add free email entry if no regular email !
            		if (myAddressToExtend.email == null) {
        				myAddressToExtend.email = freeEmail;            			
            		}
        		}
    		}
    		rsAddress.close();
    		stAddress.close();
		}
		rsNode.close();
		stNode.close();

		// read parent if not institution yet to extract full institution !
    	if (addrParentUuid != null && !ADDRESS_TYPE_INSTITUTION.equals(addrType)) {
    		readAddress(addrParentUuid, false, myAddressToExtend);
    	}

		return myAddressToExtend;
	}

	private String generateUuid() {
		UUID uuid = java.util.UUID.randomUUID();
		StringBuffer idcUuid = new StringBuffer(uuid.toString().toUpperCase());
		while (idcUuid.length() < 36) {
			idcUuid.append("0");
		}

		return idcUuid.toString();
	}

	/** Helper class encapsulating all needed data of an address ! */
	class AddrHelper {
		String uuid;
		long nodeId;
		long addrId;
		Integer addrType = null;
		String institution = null;
		String lastname = null;
		String firstname = null;
		String street = null;
		String postcode = null;
		String city = null;
		String work_state = null;
		String email = null;
		String phone = null;

		AddrHelper(String uuid, long addrId, long nodeId) {
			this.uuid = uuid;
			this.addrId = addrId;
			this.nodeId = nodeId;
		}
		public String toString() {
			return "" + firstname + ", " + lastname + ", " + email + ", " + institution; 
		}
	}

	private String getCatalogUuid() throws Exception {
		String retValue = null;
		String sql = "SELECT cat_uuid  FROM t03_catalogue";
		try {
			Statement st = jdbc.createStatement();
			ResultSet rs = jdbc.executeQuery(sql, st);
			// has to be there !!!
			rs.next();

			retValue = rs.getString(1);
			
			rs.close();
			st.close();

		} catch (SQLException e) {
			log.error("Error executing SQL: " + sql, e);
			throw e;
		}
		return retValue;
	}

	private String getCatalogPartner() throws Exception {
		String retValue = null;
		String sql = "SELECT partner_name FROM t03_catalogue";
		try {
			Statement st = jdbc.createStatement();
			ResultSet rs = jdbc.executeQuery(sql, st);
			// has to be there !!!
			rs.next();

			retValue = rs.getString(1);
			
			rs.close();
			st.close();

		} catch (SQLException e) {
			log.error("Error executing SQL: " + sql, e);
			throw e;
		}
		return retValue;
	}
}
