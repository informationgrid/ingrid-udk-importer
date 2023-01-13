/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2023 wemove digital solutions GmbH
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

import java.io.FilterReader;
import java.io.IOException;
import java.io.Reader;

/**
 * Reader replacing carriage return /end of line inside attributes (inside "" or '') with &#xA; 
 * so parser won't ignore them (normally normalizing them and replacing with blank)
 * 
 * @author Martin
 */
public class UdkXMLReader extends FilterReader
{
	private String leftover = null;
	
	boolean quotationRead = false;

  public UdkXMLReader( Reader in ) {
    super( in );
  }

  public int read() throws IOException {
    char buf[] = new char[1];
    return read(buf, 0, 1) == -1 ? -1 : buf[0];
  }

  public int read( char[] cbuf, int off, int len )
  throws IOException  {
	  String retString = new String();
	  
	  // do we have leftovers from last read ?
	  if (leftover != null) {
		  retString = leftover;
		  leftover = null;
	  }

	  if (len < retString.length()) {
		  // more leftovers than chars requested
		  leftover = retString.substring(len);
		  retString = retString.substring(0, len);

	  } else if (len == retString.length()) {
		  // length matches exactly, nothing to do

	  } else {
		  // number requested chars > leftovers, read next ones ! 
		  int numToRead = len - retString.length();
		  
		  char myBuf[] = new char[numToRead];
		  int numRead = in.read( myBuf, 0, numToRead );
		  
		  // EOF?
		  if ( numRead != -1 ) {
			  StringBuilder retStringBuilder = new StringBuilder(retString);
		      for( int i = 0; i < numRead; i++ )
		      {
		    	  if ( myBuf[i] == '\r' ) {
			    	  // skip carriage return inside quotations !
		    		  if (quotationRead) {
			    		  continue;
		    		  }
		    		  retStringBuilder.append(myBuf[i]);		    		  
		    	  } else if ( myBuf[i] == '"' || myBuf[i] == '\'') {
		    		  // NOTICE: escaping with backslash not possible in xml !
	    			  quotationRead = !quotationRead;		    			  
		    		  retStringBuilder.append(myBuf[i]);		    		  
		    	  } else if ( myBuf[i] == '\n' ) {
			    	  // replace line feed inside quotations !
		    		  if (quotationRead) {
			    		  retStringBuilder.append("&#xA;");		    			  
		    		  }
		    	  } else {
		    		  retStringBuilder.append(myBuf[i]);		    		  
		    	  }
		      }
		      if (retStringBuilder.length() > len) {
				  leftover = retStringBuilder.substring(len);
				  retString = retStringBuilder.substring(0, len);
		      } else {
			      retString = retStringBuilder.toString();
		      }
		  }
	  }
	  
	  int numRetChars = retString.length();
	  if (numRetChars == 0 && len != 0) {
//		  System.out.println("\n\n\n");

		  return -1;
	  } else {
//		  System.out.print(retString);

		  retString.getChars(0, numRetChars, cbuf, off);
		  return numRetChars;
	  }
  }
}


