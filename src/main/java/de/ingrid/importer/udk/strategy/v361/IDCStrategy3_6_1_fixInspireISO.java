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
package de.ingrid.importer.udk.strategy.v361;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 3.6.1
 * <p>
 * <ul>
 * <li>Map INSPIRE to ISO Categories and add ISO category if missing, see
 * https://dev.informationgrid.eu/redmine/issues/13 3.)
 * </ul>
 * Writes NO Catalog Schema Version to catalog and can be executed on its own !
 */
public class IDCStrategy3_6_1_fixInspireISO extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy3_6_1_fixInspireISO.class );

    /**
     * Deliver NO Version, this strategy should NOT trigger a strategy workflow
     * (of missing former versions) and can be executed on its own ! NOTICE: BUT
     * may be executed in workflow (part of workflow array) !
     * 
     * @see de.ingrid.importer.udk.strategy.IDCStrategy#getIDCVersion()
     */
    public String getIDCVersion() {
        return null;
    }

    public void execute() throws Exception {
        jdbc.setAutoCommit( false );

        // THEN PERFORM DATA MANIPULATIONS !
        // ---------------------------------

        // migrate data
        System.out.print( "  Adding ISO-Categories according to INSPIRE topics (t011_obj_topic_cat.topic_category)..." );
        migrateINSPIRE2ISO();
        System.out.println( "done." );

        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private void migrateINSPIRE2ISO() throws Exception {
        log.info( "\nAdding missing ISO categories in t011_obj_topic_cat.topic_category..." );

        // map inspire to iso
        Map<Integer, Integer> inspire2ISOMap = new HashMap<Integer, Integer>();
        inspire2ISOMap.put( 101, 13 );
        inspire2ISOMap.put( 103, 13 );
        inspire2ISOMap.put( 104, 3 );
        inspire2ISOMap.put( 105, 13 );
        inspire2ISOMap.put( 106, 15 );
        inspire2ISOMap.put( 107, 18 );
        inspire2ISOMap.put( 108, 12 );
        inspire2ISOMap.put( 109, 7 );
        inspire2ISOMap.put( 201, 6 );
        inspire2ISOMap.put( 202, 10 );
        inspire2ISOMap.put( 203, 10 );
        inspire2ISOMap.put( 204, 8 );
        inspire2ISOMap.put( 301, 3 );
        inspire2ISOMap.put( 302, 17 );
        inspire2ISOMap.put( 303, 8 );
        inspire2ISOMap.put( 304, 15 );
        inspire2ISOMap.put( 305, 9 );
        inspire2ISOMap.put( 306, 19 );
        inspire2ISOMap.put( 307, 17 );
        inspire2ISOMap.put( 308, 17 );
        inspire2ISOMap.put( 309, 1 );
        inspire2ISOMap.put( 310, 16 );
        inspire2ISOMap.put( 311, 15 );
        inspire2ISOMap.put( 312, 8 );
        inspire2ISOMap.put( 313, 4 );
        inspire2ISOMap.put( 315, 14 );
        inspire2ISOMap.put( 316, 14 );
        inspire2ISOMap.put( 317, 2 );
        inspire2ISOMap.put( 318, 2 );
        inspire2ISOMap.put( 319, 2 );
        inspire2ISOMap.put( 320, 5 );
        inspire2ISOMap.put( 321, 5 );

        PreparedStatement psSelectISO = jdbc.prepareStatement( "SELECT topic_category FROM t011_obj_topic_cat WHERE obj_id = ?" );
        PreparedStatement psInsertISO = jdbc.prepareStatement( "INSERT INTO t011_obj_topic_cat " + "(id, obj_id, line, topic_category) " + "VALUES (?,?,?,?)" );

        String sql = "SELECT obj.obj_id, val.entry_id, val.term " + "FROM searchterm_obj obj, searchterm_value val " + "WHERE obj.searchterm_id = val.id " + "AND val.type='I' "
                + "ORDER BY obj.obj_id, val.entry_id";

        Statement st = jdbc.createStatement();
        ResultSet rs = jdbc.executeQuery( sql, st );

        int numAdded = 0;
        long formerObjId = -1;
        int line = 10;
        Set<Integer> isoIds = new HashSet<Integer>();

        while (rs.next()) {
            long currObjId = rs.getLong( "obj_id" );
            int inspireId = rs.getInt( "entry_id" );
            String inspireTerm = rs.getString( "term" );

            // prepare new object
            if (currObjId != formerObjId) {
                // new object

                // fetch all ISO categories of object
                isoIds = new HashSet<Integer>();
                psSelectISO.setLong( 1, currObjId );
                ResultSet rs2 = psSelectISO.executeQuery();
                while (rs2.next()) {
                    isoIds.add( rs2.getInt( "topic_category" ) );
                }
                rs2.close();

                // we reset line. Always start at 10 to be at end of table.
                line = 10;
            }
            formerObjId = currObjId;

            // get ISO from INSPIRE
            Integer isoId = inspire2ISOMap.get( inspireId );

            // add ISO if missing
            if (isoId != null && !isoIds.contains( isoId )) {
                psInsertISO.setLong( 1, getNextId() ); // id
                psInsertISO.setLong( 2, currObjId ); // obj_id
                psInsertISO.setInt( 3, line++ ); // line
                psInsertISO.setInt( 4, isoId ); // topic_category
                int numUpdated = psInsertISO.executeUpdate();
                if (numUpdated > 0) {
                    log.info( "OBJECT [id:" + currObjId + "]: Insert new ISO category [id:" + isoId + "] from INSPIRE theme [key:" + inspireId + "/value:'" + inspireTerm
                            + "'] (t011_obj_topic_cat) !" );
                    numAdded++;
                    // we add to our set, so it will not be added again (just
                    // for sure)
                    isoIds.add( isoId );
                } else {
                    log.warn( "OBJECT [id:" + currObjId + "]: PROBLEMS inserting new ISO category [id:" + isoId + "] from INSPIRE theme [key:" + inspireId + "/value:'"
                            + inspireTerm + "'] (t011_obj_topic_cat) !" );
                }
            }
        }
        rs.close();
        st.close();
        psSelectISO.close();
        psInsertISO.close();

        log.info( "Added " + numAdded + " new ISO categories (t011_obj_topic_cat)." );
        log.info( "\nAdding missing ISO categories in t011_obj_topic_cat.topic_category...done\n" );
    }
}
