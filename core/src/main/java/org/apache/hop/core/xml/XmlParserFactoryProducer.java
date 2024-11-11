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

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import org.apache.hop.core.Const;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;

public class XmlParserFactoryProducer {
  private XmlParserFactoryProducer() {
    // Static class
  }

  /**
   * Creates an instance of {@link DocumentBuilderFactory} class with enabled {@link
   * XMLConstants#FEATURE_SECURE_PROCESSING} property. Enabling this feature protects us from some
   * XXE attacks (e.g. XML bomb).
   *
   * @throws ParserConfigurationException if feature can't be enabled
   */
  public static DocumentBuilderFactory createSecureDocBuilderFactory()
      throws ParserConfigurationException {
    DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
    docBuilderFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    docBuilderFactory.setFeature(
        "http://apache.org/xml/features/disallow-doctype-decl",
        "N".equals(Const.XML_ALLOW_DOCTYPE_DECL));

    return docBuilderFactory;
  }

  /**
   * Creates an instance of {@link SAXParserFactory} class with enabled {@link
   * XMLConstants#FEATURE_SECURE_PROCESSING} property. Enabling this feature prevents from some XXE
   * attacks (e.g. XML bomb)
   *
   * @throws ParserConfigurationException if a parser cannot be created which satisfies the
   *     requested configuration.
   * @throws SAXNotRecognizedException When the underlying XMLReader does not recognize the property
   *     name.
   * @throws SAXNotSupportedException When the underlying XMLReader recognizes the property name but
   *     doesn't support the property.
   */
  public static SAXParserFactory createSecureSAXParserFactory()
      throws SAXNotSupportedException, SAXNotRecognizedException, ParserConfigurationException {
    SAXParserFactory factory = SAXParserFactory.newInstance();
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
    factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
    factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

    return factory;
  }
}
