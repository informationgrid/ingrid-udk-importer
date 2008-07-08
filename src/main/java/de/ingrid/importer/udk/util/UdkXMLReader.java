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
	  
	  if (leftover != null) {
		  retString = leftover;
		  leftover = null;
	  }

	  if (len < retString.length()) {
		  leftover = retString.substring(len);
		  retString = retString.substring(0, len);

	  } else if (len == retString.length()) {
		  leftover = null;

	  } else {
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
	  if (numRetChars == 0) {
//		  System.out.println("\n\n\n");

		  return -1;
	  } else {
//		  System.out.print(retString);

		  retString.getChars(0, numRetChars, cbuf, off);
		  return numRetChars;
	  }
  }
}


