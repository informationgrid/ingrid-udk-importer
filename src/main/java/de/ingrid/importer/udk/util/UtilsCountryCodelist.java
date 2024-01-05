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
package de.ingrid.importer.udk.util;

import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * This class contains the UDK IMPORTER/UPDATER IGC Country Codelist and helper
 * functions.
 * NOTICE: this codelist represent a special state (IGC Version). The MOST RECENT 
 * COUNTRY CODELIST to be used in IGE etc. can be found in project ingrid-utils !  
 */
public class UtilsCountryCodelist {

	public static int COUNTRY_SYSLIST_ID = 6200;

	public static final Integer NEW_COUNTRY_KEY_GERMANY = 276;
	public static final String NEW_COUNTRY_VALUE_GERMANY_DE = "Deutschland";

	public static final Integer NEW_COUNTRY_KEY_GBR = 826;

	/** MAP: syslist with german names of countries <IGC country code, german name> */
	static public HashMap<Integer, String> countryCodelist_de = new LinkedHashMap<Integer, String>();
	static {
		countryCodelist_de.put(Integer.valueOf("008"), "Albanien");
		countryCodelist_de.put(Integer.valueOf("020"), "Andorra");
		countryCodelist_de.put(Integer.valueOf("040"), "\u00d6sterreich");
		countryCodelist_de.put(Integer.valueOf("112"), "Wei\u00dfrussland");
		countryCodelist_de.put(Integer.valueOf("056"), "Belgien");
		countryCodelist_de.put(Integer.valueOf("070"), "Bosnien und Herzegowina");
		countryCodelist_de.put(Integer.valueOf("100"), "Bulgarien");
		countryCodelist_de.put(Integer.valueOf("191"), "Kroatien");
		countryCodelist_de.put(Integer.valueOf("196"), "Zypern");
		countryCodelist_de.put(Integer.valueOf("203"), "Tschechische Republik");
		countryCodelist_de.put(Integer.valueOf("208"), "D\u00e4nemark");
		countryCodelist_de.put(Integer.valueOf("233"), "Estland");
		countryCodelist_de.put(Integer.valueOf("246"), "Finnland");
		countryCodelist_de.put(Integer.valueOf("250"), "Frankreich");
		countryCodelist_de.put(NEW_COUNTRY_KEY_GERMANY, NEW_COUNTRY_VALUE_GERMANY_DE);
		countryCodelist_de.put(Integer.valueOf("292"), "Gibraltar");
		countryCodelist_de.put(Integer.valueOf("300"), "Griechenland");
		countryCodelist_de.put(Integer.valueOf("348"), "Ungarn");
		countryCodelist_de.put(Integer.valueOf("352"), "Island");
		countryCodelist_de.put(Integer.valueOf("372"), "Irland");
		countryCodelist_de.put(Integer.valueOf("380"), "Italien");
		countryCodelist_de.put(Integer.valueOf("428"), "Lettland");
		countryCodelist_de.put(Integer.valueOf("438"), "Liechtenstein");
		countryCodelist_de.put(Integer.valueOf("440"), "Litauen");
		countryCodelist_de.put(Integer.valueOf("442"), "Luxemburg");
		countryCodelist_de.put(Integer.valueOf("807"), "Mazedonien");
		countryCodelist_de.put(Integer.valueOf("450"), "Madagaskar");
		countryCodelist_de.put(Integer.valueOf("470"), "Malta");
		countryCodelist_de.put(Integer.valueOf("498"), "Moldawien");
		countryCodelist_de.put(Integer.valueOf("492"), "Monaco");
		countryCodelist_de.put(Integer.valueOf("499"), "Montenegro");
		countryCodelist_de.put(Integer.valueOf("528"), "Niederlande");
		countryCodelist_de.put(Integer.valueOf("578"), "Norwegen");
		countryCodelist_de.put(Integer.valueOf("616"), "Polen");
		countryCodelist_de.put(Integer.valueOf("620"), "Portugal");
		countryCodelist_de.put(Integer.valueOf("642"), "Rum\u00e4nien");
		countryCodelist_de.put(Integer.valueOf("643"), "Russische F\u00f6deration");
		countryCodelist_de.put(Integer.valueOf("688"), "Serbien");
		countryCodelist_de.put(Integer.valueOf("703"), "Slowakei");
		countryCodelist_de.put(Integer.valueOf("705"), "Slowenien");
		countryCodelist_de.put(Integer.valueOf("724"), "Spanien");
		countryCodelist_de.put(Integer.valueOf("752"), "Schweden");
		countryCodelist_de.put(Integer.valueOf("756"), "Schweiz");
		countryCodelist_de.put(Integer.valueOf("792"), "T\u00fcrkei");
		countryCodelist_de.put(Integer.valueOf("804"), "Ukraine");
		countryCodelist_de.put(Integer.valueOf("826"), "Vereinigtes K\u00f6nigreich");
	}

	/** MAP: syslist with english names of countries <IGC country code, english name> */
	static public HashMap<Integer, String> countryCodelist_en = new LinkedHashMap<Integer, String>();
	static {
		countryCodelist_en.put(Integer.valueOf("008"), "Albania");
		countryCodelist_en.put(Integer.valueOf("020"), "Andorra");
		countryCodelist_en.put(Integer.valueOf("040"), "Austria");
		countryCodelist_en.put(Integer.valueOf("112"), "Belarus");
		countryCodelist_en.put(Integer.valueOf("056"), "Belgium");
		countryCodelist_en.put(Integer.valueOf("070"), "Bosnia and Herzegovina");
		countryCodelist_en.put(Integer.valueOf("100"), "Bulgaria");
		countryCodelist_en.put(Integer.valueOf("191"), "Croatia");
		countryCodelist_en.put(Integer.valueOf("196"), "Cyprus");
		countryCodelist_en.put(Integer.valueOf("203"), "Czech Republic");
		countryCodelist_en.put(Integer.valueOf("208"), "Denmark");
		countryCodelist_en.put(Integer.valueOf("233"), "Estonia");
		countryCodelist_en.put(Integer.valueOf("246"), "Finland");
		countryCodelist_en.put(Integer.valueOf("250"), "France");
		countryCodelist_en.put(NEW_COUNTRY_KEY_GERMANY, "Germany");
		countryCodelist_en.put(Integer.valueOf("292"), "Gibraltar");
		countryCodelist_en.put(Integer.valueOf("300"), "Greece");
		countryCodelist_en.put(Integer.valueOf("348"), "Hungary");
		countryCodelist_en.put(Integer.valueOf("352"), "Iceland");
		countryCodelist_en.put(Integer.valueOf("372"), "Ireland");
		countryCodelist_en.put(Integer.valueOf("380"), "Italy");
		countryCodelist_en.put(Integer.valueOf("428"), "Latvia");
		countryCodelist_en.put(Integer.valueOf("438"), "Liechtenstein");
		countryCodelist_en.put(Integer.valueOf("440"), "Lithuania");
		countryCodelist_en.put(Integer.valueOf("442"), "Luxembourg");
		countryCodelist_en.put(Integer.valueOf("807"), "Macedonia");
		countryCodelist_en.put(Integer.valueOf("450"), "Madagascar");
		countryCodelist_en.put(Integer.valueOf("470"), "Malta");
		countryCodelist_en.put(Integer.valueOf("498"), "Moldova, Republic of");
		countryCodelist_en.put(Integer.valueOf("492"), "Monaco");
		countryCodelist_en.put(Integer.valueOf("499"), "Montenegro");
		countryCodelist_en.put(Integer.valueOf("528"), "Netherlands");
		countryCodelist_en.put(Integer.valueOf("578"), "Norway");
		countryCodelist_en.put(Integer.valueOf("616"), "Poland");
		countryCodelist_en.put(Integer.valueOf("620"), "Portugal");
		countryCodelist_en.put(Integer.valueOf("642"), "Romania");
		countryCodelist_en.put(Integer.valueOf("643"), "Russian Federation");
		countryCodelist_en.put(Integer.valueOf("688"), "Serbia");
		countryCodelist_en.put(Integer.valueOf("703"), "Slovakia");
		countryCodelist_en.put(Integer.valueOf("705"), "Slovenia");
		countryCodelist_en.put(Integer.valueOf("724"), "Spain");
		countryCodelist_en.put(Integer.valueOf("752"), "Sweden");
		countryCodelist_en.put(Integer.valueOf("756"), "Switzerland");
		countryCodelist_en.put(Integer.valueOf("792"), "Turkey");
		countryCodelist_en.put(Integer.valueOf("804"), "Ukraine");
		countryCodelist_en.put(Integer.valueOf("826"), "United Kingdom");
	}

	/** MAP: country shortcut ("de", "at") to country code <ISO country shortcut, IGC country code>*/
	static private HashMap<String, Integer> countryShortcutToCode = new HashMap<String, Integer>(); 
	static {
		countryShortcutToCode.put("al", Integer.valueOf("008"));
		countryShortcutToCode.put("ad", Integer.valueOf("020"));
		countryShortcutToCode.put("at", Integer.valueOf("040"));
		countryShortcutToCode.put("by", Integer.valueOf("112"));
		countryShortcutToCode.put("be", Integer.valueOf("056"));
		countryShortcutToCode.put("ba", Integer.valueOf("070"));
		countryShortcutToCode.put("bg", Integer.valueOf("100"));
		countryShortcutToCode.put("hr", Integer.valueOf("191"));
		countryShortcutToCode.put("cy", Integer.valueOf("196"));
		countryShortcutToCode.put("cz", Integer.valueOf("203"));
		countryShortcutToCode.put("dk", Integer.valueOf("208"));
		countryShortcutToCode.put("ee", Integer.valueOf("233"));
		countryShortcutToCode.put("fi", Integer.valueOf("246"));
		countryShortcutToCode.put("fr", Integer.valueOf("250"));
		countryShortcutToCode.put("de", NEW_COUNTRY_KEY_GERMANY);
		countryShortcutToCode.put("gi", Integer.valueOf("292"));
		countryShortcutToCode.put("gr", Integer.valueOf("300"));
		countryShortcutToCode.put("hu", Integer.valueOf("348"));
		countryShortcutToCode.put("is", Integer.valueOf("352"));
		countryShortcutToCode.put("ie", Integer.valueOf("372"));
		countryShortcutToCode.put("it", Integer.valueOf("380"));
		countryShortcutToCode.put("lv", Integer.valueOf("428"));
		countryShortcutToCode.put("li", Integer.valueOf("438"));
		countryShortcutToCode.put("lt", Integer.valueOf("440"));
		countryShortcutToCode.put("lu", Integer.valueOf("442"));
		countryShortcutToCode.put("mk", Integer.valueOf("807"));
		countryShortcutToCode.put("mg", Integer.valueOf("450"));
		countryShortcutToCode.put("mt", Integer.valueOf("470"));
		countryShortcutToCode.put("md", Integer.valueOf("498"));
		countryShortcutToCode.put("mc", Integer.valueOf("492"));
		countryShortcutToCode.put("me", Integer.valueOf("499"));
		countryShortcutToCode.put("nl", Integer.valueOf("528"));
		countryShortcutToCode.put("no", Integer.valueOf("578"));
		countryShortcutToCode.put("pl", Integer.valueOf("616"));
		countryShortcutToCode.put("pt", Integer.valueOf("620"));
		countryShortcutToCode.put("ro", Integer.valueOf("642"));
		countryShortcutToCode.put("ru", Integer.valueOf("643"));
		countryShortcutToCode.put("rs", Integer.valueOf("688"));
		countryShortcutToCode.put("sk", Integer.valueOf("703"));
		countryShortcutToCode.put("sl", Integer.valueOf("705"));
		countryShortcutToCode.put("es", Integer.valueOf("724"));
		countryShortcutToCode.put("se", Integer.valueOf("752"));
		countryShortcutToCode.put("ch", Integer.valueOf("756"));
		countryShortcutToCode.put("tr", Integer.valueOf("792"));
		countryShortcutToCode.put("ua", Integer.valueOf("804"));
		countryShortcutToCode.put("gb", Integer.valueOf("826"));
		countryShortcutToCode.put("uk", Integer.valueOf("826"));
	}

	/** Determine IGC country code from country shortcut. ONLY EUROPEAN COUNTRIES ARE HANDLED !
	 * @param countryShortcut e.g. "de" or "at" or "uk" or "gb"
	 * @return IGC country code or null if not found
	 */
	static public Integer getCodeFromShortcut(String countryShortcut) {
		return countryShortcutToCode.get(countryShortcut);
	}

	/** Determine country name from IGC country code. ONLY EUROPEAN COUNTRIES ARE HANDLED !
	 * @param countryCode IGC code of country
	 * @param languageShortcut in which language should the name be returned, e.g. "de" for german name
	 * @return country name of IGC country code or null if not found !
	 */
	static public String getNameFromCode(Integer countryCode, String languageShortcut) {
		String countryName = null;

		if ("de".equals(languageShortcut)) {
			countryName = countryCodelist_de.get(countryCode);
		} else if ("en".equals(languageShortcut)) {
			countryName = countryCodelist_en.get(countryCode);			
		}

		return countryName;
	}
}
