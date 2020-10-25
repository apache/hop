/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.core.xml;

import org.junit.Test;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParserFactory;

import static org.junit.Assert.assertEquals;


public class XmlUtilsTest {
  @Test
  public void secureFeatureEnabledAfterDocBuilderFactoryCreation() throws Exception {
    DocumentBuilderFactory documentBuilderFactory = XmlParserFactoryProducer.createSecureDocBuilderFactory();

    assertEquals( true, documentBuilderFactory.getFeature( XMLConstants.FEATURE_SECURE_PROCESSING ) );
  }

  @Test
  public void secureFeatureEnabledAfterSAXParserFactoryCreation() throws Exception {
    SAXParserFactory saxParserFactory = XmlParserFactoryProducer.createSecureSAXParserFactory();

    assertEquals( true, saxParserFactory.getFeature( XMLConstants.FEATURE_SECURE_PROCESSING ) );
  }

}
