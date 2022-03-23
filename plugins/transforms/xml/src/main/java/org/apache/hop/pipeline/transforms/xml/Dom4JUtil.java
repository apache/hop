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

import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.dom4j.io.SAXReader;
import org.xml.sax.EntityResolver;
import org.xml.sax.SAXException;
import javax.xml.XMLConstants;

public class Dom4JUtil {
  
  private static final ILogChannel log = HopLogStore.getLogChannelFactory().create("Xml");

  private Dom4JUtil() {    
  }
  
  public static SAXReader getSAXReader(final EntityResolver resolver) {
    SAXReader reader = new SAXReader();
    if (resolver != null) {
      reader.setEntityResolver(resolver);
    }
    try {
      reader.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
      reader.setFeature("http://xml.org/sax/features/external-general-entities", false);
      reader.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
      reader.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
    } catch (SAXException e) {
      log.logError("Some parser properties are not supported.");
    }
    reader.setIncludeExternalDTDDeclarations(false);
    reader.setIncludeInternalDTDDeclarations(false);
    return reader;
  }
}
