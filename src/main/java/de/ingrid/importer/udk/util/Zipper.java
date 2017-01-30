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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Zipper {

	private static final String separator = "/";
	private static Log log = LogFactory.getLog(Zipper.class);

	/** The Size of the Buffer, reading and writing the files */
	private static int buffersize = 1024;

	/**
	 * Extracts the given ZIP-File and return a vector containing String
	 * representation of the extracted files.
	 * 
	 * @param zipFileName
	 *            the name of the ZIP-file
	 * @param targetDir
	 *            the target directory
	 * @param keepSubfolders
	 *            boolean parameter, whether to keep the folder structure
	 * 
	 * @return a Vector<String> containing String representation of the allowed
	 *         files from the ZipperFilter.
	 * @return null if the input data was invalid or an Exception occurs
	 */
	public static final List<String> extractZipFile(InputStream myInputStream, final String targetDir, final boolean keepSubfolders, ZipperFilter zipperFilter) {
		if (log.isDebugEnabled()) {
			log.debug("extractZipFile: inputStream=" + myInputStream + ", targetDir='" + targetDir + "', keepSubfolders=" + keepSubfolders);				
		}

		ArrayList<String> extractedFiles = new ArrayList<String>();

		FileOutputStream fos = null;

		File outdir = new File(targetDir);

		// make some checks
		if (outdir.isFile()) {
			String msg = "The Target Directory \"" + outdir + "\" must not be a file .";
			log.error(msg);
			System.err.println(msg);
			return null;
		}

		// create necessary directories for the output directory
		outdir.mkdirs();

		// Start Unzipping ...
		try {
			if (log.isDebugEnabled()) {
				log.debug("Start unzipping");
			}

			ZipInputStream zis = new ZipInputStream(myInputStream);
			if (log.isDebugEnabled()) {
				log.debug("ZipInputStream from InputStream=" + zis);
			}			

			// for every zip-entry
			ZipEntry zEntry;
			String name;
			while ((zEntry = zis.getNextEntry()) != null) {
				name = zEntry.toString();
				if (log.isDebugEnabled()) {
					log.debug("Zip Entry name: " + name + ", size:" + zEntry.getSize());
				}

				boolean isDir = name.endsWith(separator);
				// System.out.println("------------------------------");
				// System.out.println((isDir? "<d>":"< >")+name);

				String[] nameSplitted = name.split(separator);

				// If it's a File, take all Splitted Names except the last one
				int len = (isDir ? nameSplitted.length : nameSplitted.length - 1);

				String currStr = targetDir;

				if (keepSubfolders) {

					// create all directories from the entry
					for (int j = 0; j < len; j++) {
						// System.out.println("Dirs: " + nameSplitted[j]);
						currStr += nameSplitted[j] + File.separator;
						// System.out.println("currStr: "+currStr);

						File currDir = new File(currStr);
						currDir.mkdirs();

					}
				}

				// if the entry is a file, then create it.
				if (!isDir) {

					// set the file name of the output file.
					String outputFileName = null;
					if (keepSubfolders) {
						outputFileName = currStr + nameSplitted[nameSplitted.length - 1];
					} else {
						outputFileName = targetDir + nameSplitted[nameSplitted.length - 1];
					}

					File outputFile = new File(outputFileName);
					fos = new FileOutputStream(outputFile);

					// write the File
					if (log.isDebugEnabled()) {
						log.debug("Write Zip Entry '" + name + "' to file '" + outputFile + "'");
					}
					writeFile(zis, fos, buffersize);
					if (log.isDebugEnabled()) {
						log.debug("FILE WRITTEN: " + outputFile.getAbsolutePath());
					}

					// add the file to the vector to be returned

					if (zipperFilter != null) {
						String[] accFiles = zipperFilter.getAcceptedFiles();
						String currFileName = outputFile.getAbsolutePath();

						for (int i = 0; i < accFiles.length; i++) {
							if (currFileName.endsWith(accFiles[i])) {
								extractedFiles.add(currFileName);
							}
						}

					} else {
						extractedFiles.add(outputFile.getAbsolutePath());
					}

				}

			} // end while

			zis.close();

		} catch (Exception e) {
			log.error("Problems unzipping file", e);
			e.printStackTrace();
			return null;
		}

		return extractedFiles;
	}

	/**
	 * Simply writes from the InputStream to the OutputStream with the given
	 * Buffer Size. Does NOT close the InputStream. 
	 * 
	 * @param is
	 *            The InputStream
	 * @param os
	 *            The OutputStream
	 * @param buffersize
	 *            The Size of the Bugger for reading and writing
	 * @throws IOException
	 *             IOException thrown if an I/O error occurs
	 */
	private static void writeFile(InputStream is, OutputStream os, int buffersize) throws IOException {

		byte[] buf = new byte[buffersize];
		int len = 0;
		while ((len = is.read(buf)) != -1) {
//			if (log.isDebugEnabled()) {
//				log.debug("Read " + len + " bytes from input stream.");
//			}
			os.write(buf, 0, len);
//			if (log.isDebugEnabled()) {
//				log.debug("Wrote " + len + " bytes to output stream.");
//			}
		}
		os.close();
	}

	public void setBuffersize(int size) {
		buffersize = size;
	}
}
