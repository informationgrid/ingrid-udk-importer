/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2019 wemove digital solutions GmbH
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

import java.util.UUID;

import org.apache.log4j.Logger;

/**
 * Singleton used for generating unique uuids.
 * 
 * @author Martin
 */
public class UuidGenerator {

	private static final Logger LOG = Logger.getLogger(UuidGenerator.class);

	private static UuidGenerator myInstance;

	/** Get The Singleton */
	public static synchronized UuidGenerator getInstance() {
		if (myInstance == null) {
	        myInstance = new UuidGenerator();
	      }
		return myInstance;
	}

	private UuidGenerator() {}
	
	public String generateUuid() {
		UUID uuid = java.util.UUID.randomUUID();
		StringBuffer idcUuid = new StringBuffer(uuid.toString().toUpperCase());
		while (idcUuid.length() < 36) {
			idcUuid.append("0");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Generated new UUID: " + idcUuid);
		}
		return idcUuid.toString();
	}

}
