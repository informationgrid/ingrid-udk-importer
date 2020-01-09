/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2020 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v520;

import de.ingrid.codelists.model.CodeListEntry;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.importer.udk.util.InitialCodeListServiceFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Optional;

/**
 * <p>
 * Changes InGrid 5.2.0_a
 * <p>
 * Migrate conformity data since several entries were removed from codelist (Redmine #1274), see https://redmine.informationgrid.eu/issues/1274<br>
 */
public class IDCStrategy5_2_0_a extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog(IDCStrategy5_2_0_a.class);

    private static final String MY_VERSION = VALUE_IDC_VERSION_5_2_0_a;

    public String getIDCVersion() {
        // Returning version here enables strategy workflow !
        // So all former versions in IDCStrategy.STRATEGY_WORKFLOW are executed !
        // Returning null disables version tracking ...
        // Well we keep version here having a special strategy:
        // - no version written to catalog
        // - but all former versions in workflow are executed, if catalog version is below this one !
        // - return null here if you want to execute this one on its own without strategy workflow (can be changed later on when higher strategy added !)
        return MY_VERSION;
    }

    public void execute() throws Exception {
        jdbc.setAutoCommit(false);

        // NOTICE:
        // This is a "fix strategy" writing no version !

        // do write version of IGC structure, since migration shall only be run once!
         setGenericKey(KEY_IDC_VERSION, MY_VERSION);

        // THEN PERFORM DATA MANIPULATIONS !
        // ---------------------------------

        System.out.print("Migrate conformity data ...");
        migrateConformityData();
        removeInspireRichtlinie();
        migrateToFreeEntries();
        System.out.println("done.");

        jdbc.commit();
        System.out.println("Update finished successfully.");
    }

    private void migrateConformityData() throws Exception {

        // get all classes 1 and 3
        PreparedStatement psClasses1And3 = jdbc.prepareStatement("SELECT * FROM t01_object WHERE obj_class='1' OR obj_class='3'");
        PreparedStatement psConformity = jdbc.prepareStatement("SELECT * FROM object_conformity WHERE obj_id=?");

        ResultSet resultSetClasses1And3 = psClasses1And3.executeQuery();
        while (resultSetClasses1And3.next()) {
            // get isInspireRelevant and isInspireConform information
            long objID = resultSetClasses1And3.getLong("id");
            long objClass = resultSetClasses1And3.getLong("obj_class");
            boolean isInspireRelevant = "Y".equals(resultSetClasses1And3.getString("is_inspire_relevant"));
            boolean isInspireConform = "Y".equals(resultSetClasses1And3.getString("is_inspire_conform"));

            // get all conformity entries for document
            psConformity.setLong(1, objID);
            ResultSet resultSetConformities = psConformity.executeQuery();

            if (isInspireRelevant) {

                boolean hasNeededEntry = false;
                int fallBackDegree = -1;
                while (resultSetConformities.next()) {
                    int specificationKey = resultSetConformities.getInt("specification_key");
                    if ((objClass == 1 && specificationKey == 12 ) || (objClass == 3 && specificationKey == 10)) {
                        hasNeededEntry = true;
                        // check and fix conform value
                        handleConformityValue(resultSetConformities, isInspireConform);
                    } else {
                        int degreeKey = resultSetConformities.getInt("degree_key");
                        if (degreeKey == 3) { // nicht evaluiert
                            fallBackDegree = 3;
                        } else if (fallBackDegree != 3) {
                            fallBackDegree = degreeKey;
                        }
                    }
                }

                // if no VO is found, then add a correct one
                if (!hasNeededEntry) {

                    if (objClass == 1) {

                        addConformity(objID, 12, fallBackDegree);

                    } else if (objClass == 3) {

                        addConformity(objID, 10, fallBackDegree);

                    }
                }

            } else {
                // make sure if VO is chosen, that it is "not conform"
                while (resultSetConformities.next()) {
                    int specificationKey = resultSetConformities.getInt("specification_key");
                    if (specificationKey >= 10 && specificationKey <= 12) {
                        // make sure the degree is set to "not conform"
                        handleConformityValue(resultSetConformities, false);
                    }
                }
            }

            resultSetConformities.close();
        }
        resultSetClasses1And3.close();
    }

    private void addConformity(long objID, int specificationKey, Integer degreeKey) throws Exception {
        PreparedStatement psAddConformity = jdbc.prepareStatement(
                "INSERT INTO object_conformity (id, obj_id, degree_key, degree_value, specification_key, specification_value, publication_date) VALUES (?, ?, ?, ?, ?, ?, ?)");

        String specificationKeyString = specificationKey + "";
        String degreeKeyString = degreeKey + "";

        Optional<CodeListEntry> clEntry6005 = InitialCodeListServiceFactory.instance().getCodeList("6005")
                .getEntries().stream()
                .filter(entry -> specificationKeyString.equals(entry.getId()))
                .findFirst();

        Optional<CodeListEntry> clEntry6000 = InitialCodeListServiceFactory.instance().getCodeList("6000")
                .getEntries().stream()
                .filter(entry -> degreeKeyString.equals(entry.getId()))
                .findFirst();

        if (clEntry6005.isPresent()) {

            String specificationValue = clEntry6005.get().getField("de");
            String specificationData = clEntry6005.get().getData();
            String degreeValue = clEntry6000.get().getField("de");

            log.info("Adding conformity '" + specificationValue + "' with degree '" + degreeValue + "' to document with ID: " + objID);

            psAddConformity.setLong(1, getNextId());
            psAddConformity.setLong(2, objID);
            psAddConformity.setInt(3, degreeKey);
            psAddConformity.setString(4, degreeValue);
            psAddConformity.setInt(5, specificationKey);
            psAddConformity.setString(6, specificationValue);
            psAddConformity.setString(7, this.convertTimestamp(specificationData));

            psAddConformity.execute();
        }

        psAddConformity.close();
    }

    private String convertTimestamp(String dateFromCodelistData) throws ParseException {
        return new SimpleDateFormat("yyyyMMddHHmmssSSS")
                .format(new SimpleDateFormat("yyyy-MM-dd").parse(dateFromCodelistData));
    }


    /**
     * make sure if VO is chosen, that it is "conform" if is_inspire_conform
     * make sure if VO is chosen, that it is "not conform" unless is_inspire_conform
     *
     * @param resultSetConformities
     * @param isInspireConform
     * @throws SQLException
     */
    private void handleConformityValue(ResultSet resultSetConformities, boolean isInspireConform) throws SQLException {
        PreparedStatement psFixConformity = jdbc.prepareStatement(
                "UPDATE object_conformity SET degree_key=?, degree_value=? WHERE id=?");
        int degreeKey = resultSetConformities.getInt("degree_key");
        long id = resultSetConformities.getLong("id");

        // if INSPIRE conform but degree is not "conform"
        if (isInspireConform && degreeKey != 1) {

            log.info("Update conformity to 'conform' for id: " + id);

            // fix conformity value
            psFixConformity.setInt(1, 1);
            psFixConformity.setString(2, "konform");
            psFixConformity.setLong(3, id);
            psFixConformity.executeUpdate();
        }
        // if not INSPIRE conform but degree is not "not conform"
        else if (!isInspireConform && degreeKey != 2) {

            log.info("Update conformity to 'not conform' for id: " + id);

            // fix conformity value
            psFixConformity.setInt(1, 2);
            psFixConformity.setString(2, "nicht konform");
            psFixConformity.setLong(3, resultSetConformities.getLong("id"));
            psFixConformity.executeUpdate();
        }

        psFixConformity.close();
    }

    private void removeInspireRichtlinie() throws SQLException {
        String sql = "SELECT oc.id, obj_uuid FROM t01_object JOIN object_conformity oc on t01_object.id = oc.obj_id WHERE oc.specification_key=13 OR oc.specification_value='INSPIRE-Richtlinie'";
        PreparedStatement psInspireRichtlinie = jdbc.prepareStatement(sql);
        PreparedStatement psDeleteQuery = jdbc.prepareStatement("DELETE FROM object_conformity WHERE id=?");

        ResultSet resultSet = psInspireRichtlinie.executeQuery();

        while (resultSet.next()) {
            log.info("Remove INSPIRE Richtlinie from: " + resultSet.getString("obj_uuid"));
            psDeleteQuery.setLong(1, resultSet.getLong("id"));
            psDeleteQuery.executeUpdate();
        }
        psDeleteQuery.close();
        psInspireRichtlinie.close();
    }

    private void migrateToFreeEntries() throws SQLException {
        String sql = "SELECT oc.id FROM t01_object JOIN object_conformity oc on t01_object.id = oc.obj_id " +
                "WHERE oc.specification_key>-1 AND (oc.specification_key<10 OR oc.specification_key>13)";
        PreparedStatement psUpdateConformityToFree = jdbc.prepareStatement(
                "UPDATE object_conformity SET specification_key=-1 WHERE id=?");

        PreparedStatement psFreeEntries = jdbc.prepareStatement(sql);

        ResultSet resultSet = psFreeEntries.executeQuery();
        while (resultSet.next()) {
            log.info("Migrate conformity to free entry: " + resultSet.getLong("id"));
            psUpdateConformityToFree.setLong(1, resultSet.getLong("id"));
            psUpdateConformityToFree.executeUpdate();
        }

        psUpdateConformityToFree.close();
        psFreeEntries.close();
    }
}
