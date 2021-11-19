/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.svg;

import org.apache.hop.core.exception.HopException;
import org.w3c.dom.Document;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;

/** Container for SVG image. */
public class SvgImage {
  private final Document document;

  public SvgImage(Document doc) {
    this.document = doc;
  }

  public Document getDocument() {
    return document;
  }

  public String getSvgXml() throws HopException {
    return getSvgXml(document);
  }

  public static final String getSvgXml(Document document) throws HopException {
    try {
      DOMSource domSource = new DOMSource(document);
      StringWriter stringWriter = new StringWriter();
      StreamResult streamResult = new StreamResult(stringWriter);
      TransformerFactory transformerFactory = TransformerFactory.newInstance();
      Transformer transformer = transformerFactory.newTransformer();
      transformer.transform(domSource, streamResult);
      return stringWriter.toString();
    } catch (Exception e) {
      throw new HopException("Error serializing SVG Image to SVG XML", e);
    }
  }
}
