/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2024 wemove digital solutions GmbH
 * ==================================================
 * Licensed under the EUPL, Version 1.2 or – as soon they will be
 * approved by the European Commission - subsequent versions of the
 * EUPL (the "Licence");
 * 
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 * 
 * https://joinup.ec.europa.eu/software/page/eupl
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
package de.ingrid.importer.udk.strategy.v420;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 4.2.0
 * <p>
 * <ul>
 * <li>add new columns for storing spatial representation info (grid), see
 * https://dev.informationgrid.eu/redmine/issues/381
 * </ul>
 */
public class IDCStrategy4_2_0_a extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy4_2_0_a.class );

    private static final String MY_VERSION = VALUE_IDC_VERSION_4_2_0_a;

    public String getIDCVersion() {
        return MY_VERSION;
    }

    public void execute() throws Exception {
        jdbc.setAutoCommit( false );

        // write version of IGC structure !
        setGenericKey( KEY_IDC_VERSION, MY_VERSION );

        // THEN EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit
        // (e.g. on MySQL)
        // ---------------------------------

        System.out.print( "  Extend datastructure..." );
        extendDataStructure();
        System.out.println( "done." );

        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private void extendDataStructure() throws Exception {
        log.info( "\nExtending datastructure t011_obj_geo -> CAUSES COMMIT ! ..." );

        jdbc.getDBLogic().addColumn( "transformation_parameter", ColumnType.VARCHAR1, "t011_obj_geo", false, null, jdbc );
        jdbc.getDBLogic().addColumn( "num_dimensions", ColumnType.INTEGER, "t011_obj_geo", false, null, jdbc );
        jdbc.getDBLogic().addColumn( "axis_dim_name", ColumnType.VARCHAR255, "t011_obj_geo", false, null, jdbc );
        jdbc.getDBLogic().addColumn( "axis_dim_size", ColumnType.VARCHAR255, "t011_obj_geo", false, null, jdbc );
        jdbc.getDBLogic().addColumn( "cell_geometry", ColumnType.VARCHAR255, "t011_obj_geo", false, null, jdbc );
        jdbc.getDBLogic().addColumn( "geo_rectified", ColumnType.VARCHAR1, "t011_obj_geo", false, null, jdbc );
        jdbc.getDBLogic().addColumn( "geo_rect_checkpoint", ColumnType.VARCHAR1, "t011_obj_geo", false, null, jdbc );
        jdbc.getDBLogic().addColumn( "geo_rect_description", ColumnType.TEXT, "t011_obj_geo", false, null, jdbc );
        jdbc.getDBLogic().addColumn( "geo_rect_corner_point", ColumnType.VARCHAR255, "t011_obj_geo", false, null, jdbc );
        jdbc.getDBLogic().addColumn( "geo_rect_point_in_pixel", ColumnType.VARCHAR255, "t011_obj_geo", false, null, jdbc );
        jdbc.getDBLogic().addColumn( "geo_ref_control_point", ColumnType.VARCHAR1, "t011_obj_geo", false, null, jdbc );
        jdbc.getDBLogic().addColumn( "geo_ref_orientation_parameter", ColumnType.VARCHAR1, "t011_obj_geo", false, null, jdbc );
        jdbc.getDBLogic().addColumn( "geo_ref_parameter", ColumnType.VARCHAR255, "t011_obj_geo", false, null, jdbc );

        log.info( "Extending datastructure... done\n" );
    }

}
