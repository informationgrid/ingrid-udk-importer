/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2021 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.provider;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author joachim
 *
 */
public class Row extends HashMap<String, String> {

	private static Log log = LogFactory.getLog(Row.class);
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -5678481012226552594L;
	
	public Integer getInteger(String key) {
		if (key == null || this.get(key) == null) {
			return null;
		} else {
			try {
				return Integer.parseInt(this.get(key));
			} catch (NumberFormatException e) {
				log.warn("Unable to convert '" + key + "'->'" + this.get(key) + "' into int, returning 0.");
				return null;
			}
		}
	}
	
	public Double getDouble(String key) {
		if (key == null || this.get(key) == null) {
			return null;
		} else {
			try {
				return Double.parseDouble(this.get(key));
			} catch (NumberFormatException e) {
				log.warn("Unable to convert '" + key + "'->'" + this.get(key) + "' into double, returning 0.");
				return null;
			}
		}
	}
	
}
