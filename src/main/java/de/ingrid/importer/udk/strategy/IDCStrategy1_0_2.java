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

			processT03Catalogue();
			processT01Object();
			processT02Address();
			processT022AdrAdr();
			processT021Communication();
			processT012ObjObj();
			processT012ObjAdr();
			processT04Search();
			processT011ObjProject();
			processT011ObjLiteratur();
			processT011ObjData();
			processT011ObjDataParam();
			processT011ObjGeo();
			processT011ObjGeoVector();
			processT011ObjGeoSymc();
			processT011ObjGeoTopicCat();
			processT011ObjGeoSupplInfo();
			processT011ObjGeoSpatialRep();
			processT011ObjGeoScale();
			processT011ObjGeoKeyc();
			
			processT011ObjServ();
			processT011ObjServVersion();
			processT011ObjServOperation();
			processT011ObjServOpPlatform();
			processT011ObjServOpPara();
			processT011ObjServOpDepends();
			processT011ObjServOpConnpoint();
			processT015Legist();
			processT0113DatasetReference();
			processT0110AvailFormat();
			processT0112MediaOption();
			processT017UrlRef();
			
			processT011Township();
			processT019Coordinates();
			
			setHiLoGenerator();

			jdbc.commit();
		} catch (Exception e) {
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
