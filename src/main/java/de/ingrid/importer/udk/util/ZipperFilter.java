
package de.ingrid.importer.udk.util;

public class ZipperFilter {

	private String[] acceptedFiles;


	public ZipperFilter(String[] acceptedFiles) {
		this.acceptedFiles = acceptedFiles;
	}


	public String[] getAcceptedFiles() {
		return acceptedFiles;
	}

}
