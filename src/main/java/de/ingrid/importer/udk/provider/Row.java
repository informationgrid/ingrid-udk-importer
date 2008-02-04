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
