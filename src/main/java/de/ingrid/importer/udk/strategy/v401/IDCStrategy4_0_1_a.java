/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2016 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v401;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 4.0.1
 * <p>
 * <ul>
 * <li>Add column 'adv_compatible' to table 't01_object', see https://dev.informationgrid.eu/redmine/issues/369</li> 
 * <li>Add column 'administrative_area' to table 't02_address', see https://dev.informationgrid.eu/redmine/issues/375</li> 
 * <li>Add column 'inspire_conform' to table 't01_object', see https://dev.informationgrid.eu/redmine/issues/367</li> 
 * </ul>
 */
public class IDCStrategy4_0_1_a extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy4_0_1_a.class );

    private static final String MY_VERSION = VALUE_IDC_VERSION_4_0_1_a;

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
        try {
            log.info( "Add syslist '6250' ..." );
            addSyslist();
        } catch (Exception ex) {
            log.warn("Problems adding syslist 6250:", ex);
        }
        
        System.out.println( "done." );

        // FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause
        // commit (e.g. on MySQL)
        // ---------------------------------

        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private void extendDataStructure() throws Exception {
        log.info( "\nExtending datastructure -> CAUSES COMMIT ! ..." );

        log.info( "Add column 'adv_compatible' to table 't01_object' ..." );
        jdbc.getDBLogic().addColumn( "is_adv_compatible", ColumnType.VARCHAR1, "t01_object", false, "'N'", jdbc );
        
        log.info( "Add column 'inspire_conform' to table 't01_object' ..." );
        jdbc.getDBLogic().addColumn( "is_inspire_conform", ColumnType.VARCHAR1, "t01_object", false, "'N'", jdbc );
        
        log.info( "Add column 'administrative_area_key' and 'administrative_area_value' to table 't02_address' ..." );
        jdbc.getDBLogic().addColumn( "administrative_area_key", ColumnType.INTEGER, "t02_address", false, null, jdbc );
        jdbc.getDBLogic().addColumn( "administrative_area_value", ColumnType.VARCHAR255, "t02_address", false, null, jdbc );

        log.info( "Extending datastructure... done\n" );
    }
    
    private void addSyslist() throws Exception {
        String psSql = "INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default, description) " +
                "VALUES (?,?,?,?,?,?,?,?)";     
        PreparedStatement psInsert = jdbc.prepareStatement(psSql);
        
        
        // german syslist
        LinkedHashMap<Integer, String> syslistMap_de = prepareSyslistDE(); 
        
        // english syslist
        LinkedHashMap<Integer, String> syslistMap_en = prepareSyslistEN();
        

        Iterator<Integer> itr = syslistMap_de.keySet().iterator();
        while (itr.hasNext()) {
            int key = itr.next();
            // german version
            String isDefault = "N";
            if (key == 0) {
                isDefault = "Y";                
            }
            psInsert.setLong(1, getNextId());
            psInsert.setInt(2, 6250);
            psInsert.setInt(3, key);
            psInsert.setString(4, "de");
            psInsert.setString(5, syslistMap_de.get(key));
            psInsert.setInt(6, 1);
            psInsert.setString(7, isDefault);
            psInsert.setString(8, null);
            psInsert.executeUpdate();

            // english version
            psInsert.setLong(1, getNextId());
            psInsert.setString(4, "en");
            psInsert.setString(5, syslistMap_en.get(key));
            psInsert.setString(7, isDefault);
            psInsert.setString(8, null);
            psInsert.executeUpdate();
        }

        psInsert.close();
    }

    private LinkedHashMap<Integer, String> prepareSyslistEN() {
        // english syslist
        LinkedHashMap<Integer, String> newSyslistMap_en = new LinkedHashMap<Integer, String>();
        newSyslistMap_en.put(0, "Federal Republic of Germany");
        newSyslistMap_en.put(1, "Baden Wurttemberg");
        newSyslistMap_en.put(2, "Bavaria");
        newSyslistMap_en.put(3, "Berlin");
        newSyslistMap_en.put(4, "Brandenburg");
        newSyslistMap_en.put(5, "Bremen");
        newSyslistMap_en.put(6, "Hamburg");
        newSyslistMap_en.put(7, "Hessen");
        newSyslistMap_en.put(8, "Mecklenburg-West Pomerania");
        newSyslistMap_en.put(9, "Lower Saxony");
        newSyslistMap_en.put(10, "North Rhine Westphalia");
        newSyslistMap_en.put(11, "Rhineland Palatinate");
        newSyslistMap_en.put(12, "Saarland");
        newSyslistMap_en.put(13, "Saxony ");
        newSyslistMap_en.put(14, "Saxony Anhalt");
        newSyslistMap_en.put(15, "Schleswig-Holstein");
        newSyslistMap_en.put(16, "Thuringia");
        
        return newSyslistMap_en;
    }

    private LinkedHashMap<Integer, String> prepareSyslistDE() {
        // german syslist
        LinkedHashMap<Integer, String> newSyslistMap_de = new LinkedHashMap<Integer, String>();
        newSyslistMap_de.put(0, "Bundesrepublik Deutschland");
        newSyslistMap_de.put(1, "Baden-Württemberg");
        newSyslistMap_de.put(2, "Bayern");
        newSyslistMap_de.put(3, "Berlin");
        newSyslistMap_de.put(4, "Brandenburg");
        newSyslistMap_de.put(5, "Bremen");
        newSyslistMap_de.put(6, "Hamburg");
        newSyslistMap_de.put(7, "Hessen");
        newSyslistMap_de.put(8, "Mecklenburg-Vorpommern");
        newSyslistMap_de.put(9, "Niedersachsen");
        newSyslistMap_de.put(10, "Nordrhein-Westfalen");
        newSyslistMap_de.put(11, "Rheinland-Pfalz");
        newSyslistMap_de.put(12, "Saarland");
        newSyslistMap_de.put(13, "Sachsen");
        newSyslistMap_de.put(14, "Sachsen-Anhalt");
        newSyslistMap_de.put(15, "Schleswig-Holstein");
        newSyslistMap_de.put(16, "Thüringen");
        
        return newSyslistMap_de;
        
    }

}
