/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2022 wemove digital solutions GmbH
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

        System.out.println("Migrate conformity data ...");
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
            String objUUID = resultSetClasses1And3.getString("obj_uuid");
            String objName = resultSetClasses1And3.getString("obj_name");
            long objClass = resultSetClasses1And3.getLong("obj_class");

            boolean isInspireRelevant = "Y".equals(resultSetClasses1And3.getString("is_inspire_relevant"));
            boolean isInspireConform = "Y".equals(resultSetClasses1And3.getString("is_inspire_conform"));

            // get all conformity entries for document
            psConformity.setLong(1, objID);
            ResultSet resultSetConformities = psConformity.executeQuery();

            if (isInspireRelevant) {

                log.info("Working on dataset with class '" + objClass + "' '" + objName + "' (" + objUUID + ") ");
                boolean hasNeededEntry = false;
                int fallBackDegree = -1;
                String lastConformity = null;
                while (resultSetConformities.next()) {
                    int specificationKey = resultSetConformities.getInt("specification_key");
                    String specificationValue = resultSetConformities.getString("specification_value");
                    int degreeKey = resultSetConformities.getInt("degree_key");
                    if ((objClass == 1 && specificationKey == 12 ) || (objClass == 3 && specificationKey == 10)) {
                        log.info("Analyse conformities: INSPIRE VO '" + specificationValue + "' with degree '" + getConformityDegreeFromId(degreeKey) + "' in document with ID '" + objID + "' found. Not changed! Further analysis skipped.");
                        hasNeededEntry = true;
                        // do nothing, keep the existing VO entries since
                        // we do not know if a editor has deliberately changed
                        // the degree of the conformity
                        break;
                    } else {
                        if (degreeKey == 3) { // nicht evaluiert
                            fallBackDegree = 3;
                            log.info("Analyse conformities: Conformity '" + specificationValue + "' with degree 'nicht evaluiert' in document with ID '" + objID + "' found. Use degree 'nicht evaluiert' for later use.");
                            // one degree of "nicht evaluiert" of other conformities is sufficient
                            // to specify the VO as "nicht evaluiert"
                            break;
                        } else if (fallBackDegree != degreeKey && fallBackDegree != -1) {
                            log.info("Analyse conformities: Different degrees found in conformities '" + lastConformity +
                                    "' ('" + getConformityDegreeFromId(fallBackDegree) + "') and '" + specificationValue +
                                    "' ('" + getConformityDegreeFromId(degreeKey) + "') found in document with ID '" + objID +
                                    "' found. Use degree 'nicht evaluiert' for later use.");
                            // degrees differ in conformities of the dataset
                            // set degree to "nicht evaluiert"
                            fallBackDegree = 3;
                            break;
                        } else {
                            log.info("Analyse conformities: Conformity '" + specificationValue + "' with degree '" + getConformityDegreeFromId(degreeKey) + "' in document with ID '" + objID + "' found.");
                            fallBackDegree = degreeKey;
                        }
                    }
                    lastConformity =  specificationValue;
                }

                // if no VO is found, then add a correct one
                if (!hasNeededEntry && fallBackDegree != -1) {

                    if (objClass == 1) {

                        addConformity(objID, 12, fallBackDegree);

                    } else if (objClass == 3) {

                        addConformity(objID, 10, fallBackDegree);

                    }
                }
                log.info("");
                log.info("");
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

        if (clEntry6005.isPresent()) {

            String specificationValue = clEntry6005.get().getField("de");
            String specificationData = clEntry6005.get().getData();
            String degreeValue = getConformityDegreeFromId(degreeKey);

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

    private String getConformityDegreeFromId(int degreeId) {
        Optional<CodeListEntry> clEntry6000 = InitialCodeListServiceFactory.instance().getCodeList("6000")
                .getEntries().stream()
                .filter(entry -> (degreeId + "").equals(entry.getId()))
                .findFirst();
        if (clEntry6000.isPresent()) {
            return clEntry6000.get().getField("de");
        } else {
            return null;
        }
    }


    private void removeInspireRichtlinie() throws SQLException {
        String sql = "SELECT oc.id, obj_uuid, obj_name FROM t01_object JOIN object_conformity oc on t01_object.id = oc.obj_id WHERE oc.specification_key=13 OR oc.specification_value='INSPIRE-Richtlinie'";
        PreparedStatement psInspireRichtlinie = jdbc.prepareStatement(sql);
        PreparedStatement psDeleteQuery = jdbc.prepareStatement("DELETE FROM object_conformity WHERE id=?");

        ResultSet resultSet = psInspireRichtlinie.executeQuery();

        while (resultSet.next()) {
            log.info("Remove INSPIRE Richtlinie from: '" + resultSet.getString("obj_name") + "' (" + resultSet.getString("obj_uuid") + ")");
            psDeleteQuery.setLong(1, resultSet.getLong("id"));
            psDeleteQuery.executeUpdate();
        }
        psDeleteQuery.close();
        psInspireRichtlinie.close();
    }

    private void migrateToFreeEntries() throws SQLException {
        String sql = "SELECT oc.id, oc.specification_value, obj_uuid, obj_name FROM t01_object JOIN object_conformity oc on t01_object.id = oc.obj_id " +
                "WHERE oc.specification_key>-1 AND (oc.specification_key<10 OR oc.specification_key>13)";
        PreparedStatement psUpdateConformityToFree = jdbc.prepareStatement(
                "UPDATE object_conformity SET specification_key=-1 WHERE id=?");

        PreparedStatement psFreeEntries = jdbc.prepareStatement(sql);

        ResultSet resultSet = psFreeEntries.executeQuery();
        while (resultSet.next()) {
            log.info("Migrate conformity '" + resultSet.getString("specification_value") + "' (" + resultSet.getLong("id") + ") to free entry for dataset '" + resultSet.getString("obj_name") + "' (" + resultSet.getString("obj_uuid") + ").");
            psUpdateConformityToFree.setLong(1, resultSet.getLong("id"));
            psUpdateConformityToFree.executeUpdate();
        }

        psUpdateConformityToFree.close();
        psFreeEntries.close();
    }
}
