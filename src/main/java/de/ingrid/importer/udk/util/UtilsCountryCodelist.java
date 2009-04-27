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
}
