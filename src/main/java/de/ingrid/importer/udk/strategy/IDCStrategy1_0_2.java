/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Administrator
 * 
 */
public class IDCStrategy1_0_2 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy1_0_2.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.ingrid.importer.udk.strategy.IDCStrategy#execute()
	 */
	public void execute() {

		try {

			jdbc.setAutoCommit(false);

			System.out.print("  Importing sys_list...");
			// must be processed first because other methods depend on that data
			processSysList();
			System.out.println("done.");
			System.out.print("  Importing t03_catalogue...");
			processT03Catalogue();
			System.out.println("done.");
			System.out.print("  Importing t01_object...");
			processT01Object();
			System.out.println("done.");
			System.out.print("  Importing t02_address...");
			processT02Address();
			System.out.println("done.");
			System.out.print("  Importing t022_adr_adr...");
			processT022AdrAdr();
			System.out.println("done.");
			System.out.print("  Importing t021_communication...");
			processT021Communication();
			System.out.println("done.");
			System.out.print("  Importing t012_obj_obj...");
			processT012ObjObj();
			System.out.println("done.");
			System.out.print("  Importing t012_obj_adr...");
			processT012ObjAdr();
			System.out.println("done.");
			System.out.print("  Importing t04_search...");
			processT04Search();
			System.out.println("done.");
			System.out.print("  Importing t011_obj_project...");
			processT011ObjProject();
			System.out.println("done.");
			System.out.print("  Importing t011_obj_literatur...");
			processT011ObjLiteratur();
			System.out.println("done.");
			System.out.print("  Importing t011_obj_data...");
			processT011ObjData();
			System.out.println("done.");
			System.out.print("  Importing t011_obj_data_param...");
			processT011ObjDataParam();
			System.out.println("done.");
			System.out.print("  Importing t011_obj_geo...");
			processT011ObjGeo();
			System.out.println("done.");
			System.out.print("  Importing t011_obj_geo_vector...");
			processT011ObjGeoVector();
			System.out.println("done.");
			System.out.print("  Importing t011_obj_geo_symc...");
			processT011ObjGeoSymc();
			System.out.println("done.");
			System.out.print("  Importing t011_obj_geo_topic_cat...");
			processT011ObjGeoTopicCat();
			System.out.println("done.");
			System.out.print("  Importing t011_obj_geo_suppl_info...");
			processT011ObjGeoSupplInfo();
			System.out.println("done.");
			System.out.print("  Importing t011_obj_geo_spatial_ref...");
			processT011ObjGeoSpatialRep();
			System.out.println("done.");
			System.out.print("  Importing t011_obj_geo_scale...");
			processT011ObjGeoScale();
			System.out.println("done.");
			System.out.print("  Importing t011_obj_geo_keyc...");
			processT011ObjGeoKeyc();
			System.out.println("done.");

			System.out.print("  Importing t011_obj_serv...");
			processT011ObjServ();
			System.out.println("done.");
			System.out.print("  Importing t011_obj_serv_version...");
			processT011ObjServVersion();
			System.out.println("done.");
			System.out.print("  Importing t011_obj_serv_operation...");
			processT011ObjServOperation();
			System.out.println("done.");
			System.out.print("  Importing t011_obj_serv_op_platform...");
			processT011ObjServOpPlatform();
			System.out.println("done.");
			System.out.print("  Importing t011_obj_serv_op_para...");
			processT011ObjServOpPara();
			System.out.println("done.");
			System.out.print("  Importing t011_obj_serv_op_depends...");
			processT011ObjServOpDepends();
			System.out.println("done.");
			System.out.print("  Importing t011_obj_serv_op_connpoint...");
			processT011ObjServOpConnpoint();
			System.out.println("done.");
			System.out.print("  Importing t015_legist...");
			processT015Legist();
			System.out.println("done.");
			System.out.print("  Importing t0113_dataset_references...");
			processT0113DatasetReference();
			System.out.println("done.");
			System.out.print("  Importing t0110_avail_format...");
			processT0110AvailFormat();
			System.out.println("done.");
			System.out.print("  Importing t0112_media_operation...");
			processT0112MediaOption();
			System.out.println("done.");
			System.out.print("  Importing t017_url_ref...");
			processT017UrlRef();
			System.out.println("done.");
			System.out.print("  Importing t011_township...");
			processT011Township();
			System.out.println("done.");
			System.out.print("  Importing t019_coordinates...");
			processT019Coordinates();
			System.out.println("done.");
			System.out.print("  Importing t08_attrtyp...");
			processT08AttrTyp();
			System.out.println("done.");
			System.out.print("  Importing t08_attrlist...");
			processT08AttrList();
			System.out.println("done.");
			System.out.print("  Importing t08_attr...");
			processT08Attr();
			System.out.println("done.");
			System.out.print("  Importing t014_info_impart...");
			processT014InfoImpart();
			System.out.println("done.");
			System.out.print("  Importing default address/permission for admin...");
			importDefaultUserdata();
			System.out.println("done.");
			jdbc.commit();
			
			jdbc.setAutoCommit(false);
			System.out.print("  Post processing...");
			postProcess();
			System.out.println("done.");
			System.out.print("  Set HI/LO table...");
			setHiLoGenerator();
			System.out.println("done.");
			jdbc.commit();
			System.out.println("Import finished successfully.");

		} catch (Exception e) {
			System.out.println("Error executing sql! See log file for further information.");
			log.error("Error executing SQL!", e);
			if (jdbc != null) {
				try {
					jdbc.rollback();
				} catch (SQLException e1) {
					log.error("Error rolling back transaction!", e);
				}
			}
		} finally {
			if (jdbc != null) {
				try {
					jdbc.close();
				} catch (SQLException e) {
					log.error("Error closing DB connection!", e);
				}
			}
		}
	}

}
