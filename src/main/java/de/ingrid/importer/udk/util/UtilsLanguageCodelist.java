/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2017 wemove digital solutions GmbH
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
 * This class contains the UDK IMPORTER/UPDATER IGC Language Codelist and helper
 * functions, e.g. mapping of language shortcut to language code.
 * NOTICE: this codelist represent a special state (IGC Version). The MOST RECENT 
 * LANGUAGE CODELIST to be used in IGE etc. can be found in project ingrid-utils !  
 */
public class UtilsLanguageCodelist {

	/** Syslist ID of language codelist. */
	static public int LANGUAGE_SYSLIST_ID = 99999999;

	/** IGC code of german language (= entryId in syslist) */
	static public int IGC_CODE_GERMAN = 150;
	/** IGC code of english language (= entryId in syslist) */
	static public int IGC_CODE_ENGLISH = 123;

	/** MAP: syslist with german names of languages <IGC language code, german language name> */
	static public HashMap<Integer, String> languageCodelist_de = new LinkedHashMap<Integer, String>();
	static {
		languageCodelist_de.put(IGC_CODE_GERMAN, "Deutsch");
		languageCodelist_de.put(IGC_CODE_ENGLISH, "Englisch");
	}

	/** MAP: syslist with english names of languages <IGC language code, english language name> */
	static public HashMap<Integer, String> languageCodelist_en = new LinkedHashMap<Integer, String>();
	static {
		languageCodelist_en.put(IGC_CODE_GERMAN, "German");
		languageCodelist_en.put(IGC_CODE_ENGLISH, "English");
	}

	/** MAP: language shortcut ("de", "en") to language code <ISO language shortcut, IGC language code>*/
	static private HashMap<String, Integer> languageShortcutToCode = new HashMap<String, Integer>(); 
	static {
		languageShortcutToCode.put("de", IGC_CODE_GERMAN);
		languageShortcutToCode.put("en", IGC_CODE_ENGLISH);
	}


	/** Determine IGC language code from language shortcut.
	 * @param languageShortcut e.g. "de" or "en"
	 * @return IGC language code or null if not found
	 */
	static public Integer getCodeFromShortcut(String languageShortcut) {
		return languageShortcutToCode.get(languageShortcut);
	}

	/** Determine language name from IGC language code.
	 * @param languageCode IGC code of language
	 * @param languageShortcut in which language should the name be returned, e.g. "de" for german name
	 * @return language name of IGC language code or null if not found !
	 */
	static public String getNameFromCode(Integer languageCode, String languageShortcut) {
		String langName = null;

		if ("de".equals(languageShortcut)) {
			langName = languageCodelist_de.get(languageCode);
		} else if ("en".equals(languageShortcut)) {
			langName = languageCodelist_en.get(languageCode);			
		}

		return langName;
	}
}
