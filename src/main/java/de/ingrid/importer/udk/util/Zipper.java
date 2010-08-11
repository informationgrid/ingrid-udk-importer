package de.ingrid.importer.udk.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

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
	public static final List<String> extractZipFile(final File zipFile, final String targetDir, final boolean keepSubfolders, ZipperFilter zipperFilter) {

		ArrayList<String> extractedFiles = new ArrayList<String>();

		FileOutputStream fos = null;

		File outdir = new File(targetDir);

		// make some checks
		if (!zipFile.exists()) {
			System.err.println("The given ZIP-file does NOT exists: " + zipFile + ".");
			return null;
		} else if (!zipFile.canRead()) {
			System.err.println("Can't read the file: " + zipFile + ".");
			return null;
		} else if (outdir.isFile()) {
			System.err.println("The Target Directory \"" + outdir + "\" must not be a file .");
			return null;
		}

		// create necessary directories for the output directory
		outdir.mkdirs();

		// Start Unzipping ...
		try {
			ZipFile zfile = new ZipFile(zipFile, ZipFile.OPEN_READ);

			Enumeration<? extends ZipEntry> en = zfile.entries();

			// for every zip-entry
			while (en.hasMoreElements()) {
				ZipEntry zEntry = (ZipEntry) en.nextElement();

				String name = zEntry.toString();

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
					writeFile(zfile.getInputStream(zEntry), fos, buffersize);
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

			zfile.close();

		} catch (ZipException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

		return extractedFiles;
	}

	/**
	 * Simply writes from the InputStream to the OutputStream with the given
	 * Buffer Size.
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
			os.write(buf, 0, len);
		}
		is.close();
		os.close();
	}

	public void setBuffersize(int size) {
		buffersize = size;
	}
}