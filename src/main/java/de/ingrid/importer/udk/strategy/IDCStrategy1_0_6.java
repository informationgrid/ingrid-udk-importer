/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;

/**
 * Extend all VARCHAR database fields to 255 !
 */
public class IDCStrategy1_0_6 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy1_0_6.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_106;

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// then write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		// THEN EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit (e.g. on MySQL)
		// ---------------------------------
		System.out.print("  Extend datastructure...");
		extendDataStructure();
		System.out.println("done.");

		// THEN PERFORM DATA MANIPULATIONS !
		// ---------------------------------


		// FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause commit (e.g. on MySQL)
		// ---------------------------------


		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void extendDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Extending datastructure -> CAUSES COMMIT ! ...");
		}

		// extend all varchars to 255 !
		List<String[]> tableColumns = new ArrayList<String[]>();
		tableColumns.add(new String[] { "t011_obj_serv_op_platform", "platform" });
		tableColumns.add(new String[] { "t011_obj_serv_op_para", "name" });
		tableColumns.add(new String[] { "t011_obj_serv_op_para", "direction" });
		tableColumns.add(new String[] { "t011_obj_serv_op_depends", "depends_on" });
		tableColumns.add(new String[] { "t011_obj_serv_version", "serv_version" });
		tableColumns.add(new String[] { "t011_obj_serv_operation", "name_value" });
		tableColumns.add(new String[] { "t011_obj_geo_symc", "symbol_cat_value" });
		tableColumns.add(new String[] { "t011_obj_geo_symc", "edition" });
		tableColumns.add(new String[] { "t011_obj_geo_keyc", "keyc_value" });
		tableColumns.add(new String[] { "t011_obj_geo_keyc", "edition" });
		tableColumns.add(new String[] { "t015_legist", "legist_value" });
		tableColumns.add(new String[] { "t014_info_impart", "impart_value" });
		tableColumns.add(new String[] { "t012_obj_adr", "special_name" });
		tableColumns.add(new String[] { "t0110_avail_format", "format_value" });
		tableColumns.add(new String[] { "t0110_avail_format", "ver" });
		tableColumns.add(new String[] { "t0110_avail_format", "file_decompression_technique" });
		tableColumns.add(new String[] { "t0110_avail_format", "specification" });
		tableColumns.add(new String[] { "t011_obj_project", "leader" });
		tableColumns.add(new String[] { "t011_obj_literature", "type_value" });
		tableColumns.add(new String[] { "t011_obj_literature", "publish_in" });
		tableColumns.add(new String[] { "t011_obj_literature", "volume" });
		tableColumns.add(new String[] { "t011_obj_literature", "sides" });
		tableColumns.add(new String[] { "t011_obj_literature", "publish_year" });
		tableColumns.add(new String[] { "t011_obj_literature", "publish_loc" });
		tableColumns.add(new String[] { "t011_obj_literature", "loc" });
		tableColumns.add(new String[] { "t011_obj_literature", "isbn" });
		tableColumns.add(new String[] { "t011_obj_literature", "publishing" });
		tableColumns.add(new String[] { "t011_obj_data_para", "parameter" });
		tableColumns.add(new String[] { "t011_obj_data_para", "unit" });
		tableColumns.add(new String[] { "object_reference", "special_name" });
		tableColumns.add(new String[] { "t01_object", "org_obj_id" });
		tableColumns.add(new String[] { "t01_object", "dataset_alternate_name" });
		tableColumns.add(new String[] { "t01_object", "metadata_standard_name" });
		tableColumns.add(new String[] { "t01_object", "metadata_standard_version" });
		tableColumns.add(new String[] { "t021_communication", "commtype_value" });
		tableColumns.add(new String[] { "t021_communication", "descr" });
		tableColumns.add(new String[] { "spatial_ref_value", "nativekey" });
		tableColumns.add(new String[] { "spatial_ref_value", "topic_type" });
		tableColumns.add(new String[] { "t08_attr_type", "name" });
		tableColumns.add(new String[] { "t02_address", "org_adr_id" });
		tableColumns.add(new String[] { "t02_address", "lastname" });
		tableColumns.add(new String[] { "t02_address", "firstname" });
		tableColumns.add(new String[] { "t02_address", "address_value" });
		tableColumns.add(new String[] { "t02_address", "title_value" });
		tableColumns.add(new String[] { "t02_address", "street" });
		tableColumns.add(new String[] { "t02_address", "postcode" });
		tableColumns.add(new String[] { "t02_address", "postbox" });
		tableColumns.add(new String[] { "t02_address", "postbox_pc" });
		tableColumns.add(new String[] { "t02_address", "city" });
		tableColumns.add(new String[] { "spatial_ref_sns", "sns_id" });
		tableColumns.add(new String[] { "searchterm_sns", "sns_id" });
		tableColumns.add(new String[] { "searchterm_sns", "gemet_id" });
		tableColumns.add(new String[] { "idc_group", "name" });
		
		for (String[] tC : tableColumns) {
			if (log.isInfoEnabled()) {
				log.info("Change field type of '" + tC[0] + "." + tC[1] + "' to VARCHAR(255) ...");
			}

			jdbc.getDBLogic().modifyColumn(tC[1], ColumnType.VARCHAR255, tC[0], false, jdbc);
		}

		if (log.isInfoEnabled()) {
			log.info("Extending datastructure... done");
		}
	}
}
