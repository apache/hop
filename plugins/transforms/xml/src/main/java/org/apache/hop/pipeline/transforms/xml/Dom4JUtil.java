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
package org.apache.hop.pipeline.transforms.xml;

import javax.xml.XMLConstants;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.dom4j.io.SAXReader;
import org.xml.sax.SAXException;

public class Dom4JUtil {

  private static final String DISALLOW_DOCTYPE_DECL =
      "http://apache.org/xml/features/disallow-doctype-decl";

  /** Features that keep external DTDs and entities out, and the values they need. */
  private static final Object[][] PROTECTIVE_FEATURES = {
    {XMLConstants.FEATURE_SECURE_PROCESSING, true},
    {"http://xml.org/sax/features/external-general-entities", false},
    {"http://xml.org/sax/features/external-parameter-entities", false},
    {"http://apache.org/xml/features/nonvalidating/load-external-dtd", false},
  };

  private Dom4JUtil() {}

  public static SAXReader getSAXReader() {
    SAXReader reader = new SAXReader();
    boolean protectedParser = true;
    for (Object[] feature : PROTECTIVE_FEATURES) {
      try {
        reader.setFeature((String) feature[0], (Boolean) feature[1]);
      } catch (SAXException e) {
        protectedParser = false;
        logUnsupportedFeature((String) feature[0]);
      }
    }

    // A DOCTYPE is only read when XML_ALLOW_DOCTYPE_DECL allows it, and only by a parser that
    // keeps external DTDs and entities out.
    boolean allowDoctype = protectedParser && XmlParserFactoryProducer.isDoctypeAllowed();
    try {
      reader.setFeature(DISALLOW_DOCTYPE_DECL, !allowDoctype);
    } catch (SAXException e) {
      logUnsupportedFeature(DISALLOW_DOCTYPE_DECL);
    }
    if (allowDoctype) {
      reader.setXMLFilter(new EntityExpansionLimitFilter());
    }

    reader.setIncludeExternalDTDDeclarations(false);
    reader.setIncludeInternalDTDDeclarations(false);
    return reader;
  }

  private static void logUnsupportedFeature(String feature) {
    ILogChannel log = HopLogStore.getLogChannelFactory().create("Xml");
    log.logError("XML parser feature not supported: " + feature);
  }
}
