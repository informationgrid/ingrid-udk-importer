/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2018 wemove digital solutions GmbH
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
		countryCodelist_de.put(new Integer("008"), "Albanien");
		countryCodelist_de.put(new Integer("020"), "Andorra");
		countryCodelist_de.put(new Integer("040"), "\u00d6sterreich");
		countryCodelist_de.put(new Integer("112"), "Wei\u00dfrussland");
		countryCodelist_de.put(new Integer("056"), "Belgien");
		countryCodelist_de.put(new Integer("070"), "Bosnien und Herzegowina");
		countryCodelist_de.put(new Integer("100"), "Bulgarien");
		countryCodelist_de.put(new Integer("191"), "Kroatien");
		countryCodelist_de.put(new Integer("196"), "Zypern");
		countryCodelist_de.put(new Integer("203"), "Tschechische Republik");
		countryCodelist_de.put(new Integer("208"), "D\u00e4nemark");
		countryCodelist_de.put(new Integer("233"), "Estland");
		countryCodelist_de.put(new Integer("246"), "Finnland");
		countryCodelist_de.put(new Integer("250"), "Frankreich");
		countryCodelist_de.put(NEW_COUNTRY_KEY_GERMANY, NEW_COUNTRY_VALUE_GERMANY_DE);
		countryCodelist_de.put(new Integer("292"), "Gibraltar");
		countryCodelist_de.put(new Integer("300"), "Griechenland");
		countryCodelist_de.put(new Integer("348"), "Ungarn");
		countryCodelist_de.put(new Integer("352"), "Island");
		countryCodelist_de.put(new Integer("372"), "Irland");
		countryCodelist_de.put(new Integer("380"), "Italien");
		countryCodelist_de.put(new Integer("428"), "Lettland");
		countryCodelist_de.put(new Integer("438"), "Liechtenstein");
		countryCodelist_de.put(new Integer("440"), "Litauen");
		countryCodelist_de.put(new Integer("442"), "Luxemburg");
		countryCodelist_de.put(new Integer("807"), "Mazedonien");
		countryCodelist_de.put(new Integer("450"), "Madagaskar");
		countryCodelist_de.put(new Integer("470"), "Malta");
		countryCodelist_de.put(new Integer("498"), "Moldawien");
		countryCodelist_de.put(new Integer("492"), "Monaco");
		countryCodelist_de.put(new Integer("499"), "Montenegro");
		countryCodelist_de.put(new Integer("528"), "Niederlande");
		countryCodelist_de.put(new Integer("578"), "Norwegen");
		countryCodelist_de.put(new Integer("616"), "Polen");
		countryCodelist_de.put(new Integer("620"), "Portugal");
		countryCodelist_de.put(new Integer("642"), "Rum\u00e4nien");
		countryCodelist_de.put(new Integer("643"), "Russische F\u00f6deration");
		countryCodelist_de.put(new Integer("688"), "Serbien");
		countryCodelist_de.put(new Integer("703"), "Slowakei");
		countryCodelist_de.put(new Integer("705"), "Slowenien");
		countryCodelist_de.put(new Integer("724"), "Spanien");
		countryCodelist_de.put(new Integer("752"), "Schweden");
		countryCodelist_de.put(new Integer("756"), "Schweiz");
		countryCodelist_de.put(new Integer("792"), "T\u00fcrkei");
		countryCodelist_de.put(new Integer("804"), "Ukraine");
		countryCodelist_de.put(new Integer("826"), "Vereinigtes K\u00f6nigreich");
	}

	/** MAP: syslist with english names of countries <IGC country code, english name> */
	static public HashMap<Integer, String> countryCodelist_en = new LinkedHashMap<Integer, String>();
	static {
		countryCodelist_en.put(new Integer("008"), "Albania");
		countryCodelist_en.put(new Integer("020"), "Andorra");
		countryCodelist_en.put(new Integer("040"), "Austria");
		countryCodelist_en.put(new Integer("112"), "Belarus");
		countryCodelist_en.put(new Integer("056"), "Belgium");
		countryCodelist_en.put(new Integer("070"), "Bosnia and Herzegovina");
		countryCodelist_en.put(new Integer("100"), "Bulgaria");
		countryCodelist_en.put(new Integer("191"), "Croatia");
		countryCodelist_en.put(new Integer("196"), "Cyprus");
		countryCodelist_en.put(new Integer("203"), "Czech Republic");
		countryCodelist_en.put(new Integer("208"), "Denmark");
		countryCodelist_en.put(new Integer("233"), "Estonia");
		countryCodelist_en.put(new Integer("246"), "Finland");
		countryCodelist_en.put(new Integer("250"), "France");
		countryCodelist_en.put(NEW_COUNTRY_KEY_GERMANY, "Germany");
		countryCodelist_en.put(new Integer("292"), "Gibraltar");
		countryCodelist_en.put(new Integer("300"), "Greece");
		countryCodelist_en.put(new Integer("348"), "Hungary");
		countryCodelist_en.put(new Integer("352"), "Iceland");
		countryCodelist_en.put(new Integer("372"), "Ireland");
		countryCodelist_en.put(new Integer("380"), "Italy");
		countryCodelist_en.put(new Integer("428"), "Latvia");
		countryCodelist_en.put(new Integer("438"), "Liechtenstein");
		countryCodelist_en.put(new Integer("440"), "Lithuania");
		countryCodelist_en.put(new Integer("442"), "Luxembourg");
		countryCodelist_en.put(new Integer("807"), "Macedonia");
		countryCodelist_en.put(new Integer("450"), "Madagascar");
		countryCodelist_en.put(new Integer("470"), "Malta");
		countryCodelist_en.put(new Integer("498"), "Moldova, Republic of");
		countryCodelist_en.put(new Integer("492"), "Monaco");
		countryCodelist_en.put(new Integer("499"), "Montenegro");
		countryCodelist_en.put(new Integer("528"), "Netherlands");
		countryCodelist_en.put(new Integer("578"), "Norway");
		countryCodelist_en.put(new Integer("616"), "Poland");
		countryCodelist_en.put(new Integer("620"), "Portugal");
		countryCodelist_en.put(new Integer("642"), "Romania");
		countryCodelist_en.put(new Integer("643"), "Russian Federation");
		countryCodelist_en.put(new Integer("688"), "Serbia");
		countryCodelist_en.put(new Integer("703"), "Slovakia");
		countryCodelist_en.put(new Integer("705"), "Slovenia");
		countryCodelist_en.put(new Integer("724"), "Spain");
		countryCodelist_en.put(new Integer("752"), "Sweden");
		countryCodelist_en.put(new Integer("756"), "Switzerland");
		countryCodelist_en.put(new Integer("792"), "Turkey");
		countryCodelist_en.put(new Integer("804"), "Ukraine");
		countryCodelist_en.put(new Integer("826"), "United Kingdom");
	}

	/** MAP: country shortcut ("de", "at") to country code <ISO country shortcut, IGC country code>*/
	static private HashMap<String, Integer> countryShortcutToCode = new HashMap<String, Integer>(); 
	static {
		countryShortcutToCode.put("al", new Integer("008"));
		countryShortcutToCode.put("ad", new Integer("020"));
		countryShortcutToCode.put("at", new Integer("040"));
		countryShortcutToCode.put("by", new Integer("112"));
		countryShortcutToCode.put("be", new Integer("056"));
		countryShortcutToCode.put("ba", new Integer("070"));
		countryShortcutToCode.put("bg", new Integer("100"));
		countryShortcutToCode.put("hr", new Integer("191"));
		countryShortcutToCode.put("cy", new Integer("196"));
		countryShortcutToCode.put("cz", new Integer("203"));
		countryShortcutToCode.put("dk", new Integer("208"));
		countryShortcutToCode.put("ee", new Integer("233"));
		countryShortcutToCode.put("fi", new Integer("246"));
		countryShortcutToCode.put("fr", new Integer("250"));
		countryShortcutToCode.put("de", NEW_COUNTRY_KEY_GERMANY);
		countryShortcutToCode.put("gi", new Integer("292"));
		countryShortcutToCode.put("gr", new Integer("300"));
		countryShortcutToCode.put("hu", new Integer("348"));
		countryShortcutToCode.put("is", new Integer("352"));
		countryShortcutToCode.put("ie", new Integer("372"));
		countryShortcutToCode.put("it", new Integer("380"));
		countryShortcutToCode.put("lv", new Integer("428"));
		countryShortcutToCode.put("li", new Integer("438"));
		countryShortcutToCode.put("lt", new Integer("440"));
		countryShortcutToCode.put("lu", new Integer("442"));
		countryShortcutToCode.put("mk", new Integer("807"));
		countryShortcutToCode.put("mg", new Integer("450"));
		countryShortcutToCode.put("mt", new Integer("470"));
		countryShortcutToCode.put("md", new Integer("498"));
		countryShortcutToCode.put("mc", new Integer("492"));
		countryShortcutToCode.put("me", new Integer("499"));
		countryShortcutToCode.put("nl", new Integer("528"));
		countryShortcutToCode.put("no", new Integer("578"));
		countryShortcutToCode.put("pl", new Integer("616"));
		countryShortcutToCode.put("pt", new Integer("620"));
		countryShortcutToCode.put("ro", new Integer("642"));
		countryShortcutToCode.put("ru", new Integer("643"));
		countryShortcutToCode.put("rs", new Integer("688"));
		countryShortcutToCode.put("sk", new Integer("703"));
		countryShortcutToCode.put("sl", new Integer("705"));
		countryShortcutToCode.put("es", new Integer("724"));
		countryShortcutToCode.put("se", new Integer("752"));
		countryShortcutToCode.put("ch", new Integer("756"));
		countryShortcutToCode.put("tr", new Integer("792"));
		countryShortcutToCode.put("ua", new Integer("804"));
		countryShortcutToCode.put("gb", new Integer("826"));
		countryShortcutToCode.put("uk", new Integer("826"));
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
