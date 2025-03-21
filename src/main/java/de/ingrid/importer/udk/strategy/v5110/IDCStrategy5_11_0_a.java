/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2025 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v5110;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * <p>
 * Changes InGrid 5.11.0_a
 * <p>
 * <ul>
 * <li>Migrate opendata categories to GovData categories</li>
 * </ul>
 */
public class IDCStrategy5_11_0_a extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog(IDCStrategy5_11_0_a.class);

    private static final String MY_VERSION = VALUE_IDC_VERSION_5_11_0_a;

    private final List<String> DCAT_CATEGORIES = Arrays.asList(
            "Landwirtschaft, Fischerei, Forstwirtschaft und Nahrungsmittel",
            "Wirtschaft und Finanzen",
            "Bildung, Kultur und Sport",
            "Energie",
            "Umwelt",
            "Gesundheit",
            "Internationale Themen",
            "Justiz, Rechtssystem und öffentliche Sicherheit",
            "Bevölkerung und Gesellschaft",
            "Regierung und öffentlicher Sektor",
            "Regionen und Städte",
            "Wissenschaft und Technologie",
            "Verkehr"
    );

    private final Map<String, String> OGDDtoDCAT = Stream.of(new String[][]{
            {"Bevölkerung", "Bevölkerung und Gesellschaft"},
            {"Bildung und Wissenschaft", "Wissenschaft und Technologie"},
            {"Geographie, Geologie und Geobasisdaten", "Umwelt"},
            {"Gesetze und Justiz", "Justiz, Rechtssystem und öffentliche Sicherheit"},
            {"Gesundheit", "Gesundheit"},
            {"Infrastruktur, Bauen und Wohnen", "Regionen und Städte"},
            {"Kultur, Freizeit, Sport und Tourismus", "Bildung, Kultur und Sport"},
            {"Politik und Wahlen", "Regierung und öffentlicher Sektor"},
            {"Soziales", "Bevölkerung und Gesellschaft"},
            {"Transport und Verkehr", "Verkehr"},
            {"Umwelt und Klima", "Umwelt"},
            {"Verbraucherschutz", "Wirtschaft und Finanzen"},
            {"Öffentliche Verwaltung, Haushalt und Steuern", "Regionen und Städte"},
            {"Wirtschaft und Arbeit", "Wirtschaft und Finanzen"},
    }).collect(Collectors.toMap(data -> data[0], data -> data[1]));
    ;

    public String getIDCVersion() {
        return MY_VERSION;
    }

    public void execute() throws Exception {


        jdbc.setAutoCommit(false);

        // write version of IGC structure !
        setGenericKey(KEY_IDC_VERSION, MY_VERSION);

        this.updateOpendataCategory();

        jdbc.commit();
        System.out.println("Update finished successfully.");
    }


    private void updateOpendataCategory() throws Exception {
        // get all german entries
        String getSql = "SELECT id, category_value FROM object_open_data_category";
        PreparedStatement psGet = jdbc.prepareStatement(getSql);


        String updateSql = "UPDATE object_open_data_category SET category_key=?, category_value=? WHERE id=?";
        PreparedStatement psUpdate = jdbc.prepareStatement(updateSql);

        // iterate over entries
        ResultSet rsGet = psGet.executeQuery();
        while (rsGet.next()) {
            int entry_id = rsGet.getInt("id");
            String ogddCategory = rsGet.getString("category_value");

            String dcatCategory = OGDDtoDCAT.get(ogddCategory);
            // add 1 as codelist keys starts with 1 index
            int category_key = DCAT_CATEGORIES.indexOf(dcatCategory) + 1;
            if (category_key == 0) {
                log.warn("No category found for ogdd category: " + ogddCategory);
                log.warn("Skipping entry with id: " + entry_id);
                continue;
            }

            // update index and value to new category
            psUpdate.setInt(1, category_key);
            psUpdate.setString(2, dcatCategory);
            psUpdate.setInt(3, entry_id);
            psUpdate.executeUpdate();
        }
        psUpdate.close();
    }
}
