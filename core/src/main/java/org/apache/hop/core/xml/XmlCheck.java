/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.xml;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.InputStream;

public class XmlCheck {

  public static class XMLTreeHandler extends DefaultHandler {

  }

  /**
   * Checks an xml file is well formed.
   *
   * @param file The file to check
   * @return true if the file is well formed.
   */
  public static final boolean isXmlFileWellFormed(FileObject file ) throws HopException {
    boolean retval = false;
    try {
      retval = isXmlWellFormed( file.getContent().getInputStream() );
    } catch ( Exception e ) {
      throw new HopException( e );
    }

    return retval;
  }

  /**
   * Checks an xml string is well formed.
   *
   * @param is inputstream
   * @return true if the xml is well formed.
   */
  public static boolean isXmlWellFormed(InputStream is ) throws HopException {
    boolean retval = false;
    try {
      SAXParserFactory factory = XmlParserFactoryProducer.createSecureSAXParserFactory();
      XMLTreeHandler handler = new XMLTreeHandler();

      // Parse the input.
      SAXParser saxParser = factory.newSAXParser();
      saxParser.parse( is, handler );
      retval = true;
    } catch ( Exception e ) {
      throw new HopException( e );
    }
    return retval;
  }

}
