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

import java.io.IOException;
import java.util.Set;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.ext.DeclHandler;
import org.xml.sax.ext.LexicalHandler;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 * Caps the text that the entities of a document expand to. The parser only caps how often entities
 * expand, so a single large entity referenced many times could still exhaust memory.
 *
 * <p>Text counts when it comes from an entity. Once a document declares an entity, attribute values
 * count too, since the parser does not report which of them came from one.
 */
class EntityExpansionLimitFilter extends XMLFilterImpl implements LexicalHandler, DeclHandler {

  /** The default of the JDK's own parser ({@code jdk.xml.totalEntitySizeLimit}). */
  static final long DEFAULT_LIMIT = 50_000_000L;

  private static final String LEXICAL_HANDLER = "http://xml.org/sax/properties/lexical-handler";
  private static final String LEXICAL_HANDLER_ALIAS = "http://xml.org/sax/handlers/LexicalHandler";
  private static final String DECL_HANDLER = "http://xml.org/sax/properties/declaration-handler";

  private static final Set<String> PREDEFINED_ENTITIES = Set.of("amp", "lt", "gt", "quot", "apos");

  private final long limit;
  private LexicalHandler lexicalHandler;
  private DeclHandler declHandler;
  private int entityDepth;
  private boolean entitiesDeclared;
  private long expanded;

  EntityExpansionLimitFilter() {
    this(DEFAULT_LIMIT);
  }

  EntityExpansionLimitFilter(long limit) {
    this.limit = limit;
  }

  @Override
  public void setProperty(String name, Object value)
      throws SAXNotRecognizedException, SAXNotSupportedException {
    if (LEXICAL_HANDLER.equals(name) || LEXICAL_HANDLER_ALIAS.equals(name)) {
      lexicalHandler = (LexicalHandler) value;
    } else if (DECL_HANDLER.equals(name)) {
      declHandler = (DeclHandler) value;
    } else {
      super.setProperty(name, value);
    }
  }

  @Override
  public Object getProperty(String name)
      throws SAXNotRecognizedException, SAXNotSupportedException {
    if (LEXICAL_HANDLER.equals(name) || LEXICAL_HANDLER_ALIAS.equals(name)) {
      return lexicalHandler;
    }
    if (DECL_HANDLER.equals(name)) {
      return declHandler;
    }
    return super.getProperty(name);
  }

  @Override
  public void parse(InputSource input) throws SAXException, IOException {
    entityDepth = 0;
    entitiesDeclared = false;
    expanded = 0;
    getParent().setProperty(LEXICAL_HANDLER, this);
    getParent().setProperty(DECL_HANDLER, this);
    super.parse(input);
  }

  private void count(long length) throws SAXException {
    expanded += length;
    if (expanded > limit) {
      throw new SAXException(
          "The entities of this XML document expand to more than " + limit + " characters");
    }
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes atts)
      throws SAXException {
    if (entitiesDeclared) {
      for (int i = 0; i < atts.getLength(); i++) {
        count(atts.getValue(i).length());
      }
    }
    super.startElement(uri, localName, qName, atts);
  }

  @Override
  public void characters(char[] ch, int start, int length) throws SAXException {
    if (entityDepth > 0) {
      count(length);
    }
    super.characters(ch, start, length);
  }

  @Override
  public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
    if (entityDepth > 0) {
      count(length);
    }
    super.ignorableWhitespace(ch, start, length);
  }

  /**
   * Whether the text of an entity counts: not the external DTD subset ({@code [dtd]}), a parameter
   * entity ({@code %name}) or a predefined entity such as {@code amp}, which is one character.
   */
  private static boolean counts(String entityName) {
    return !entityName.startsWith("[")
        && !entityName.startsWith("%")
        && !PREDEFINED_ENTITIES.contains(entityName);
  }

  // LexicalHandler

  @Override
  public void startEntity(String name) throws SAXException {
    if (counts(name)) {
      entityDepth++;
    }
    if (lexicalHandler != null) {
      lexicalHandler.startEntity(name);
    }
  }

  @Override
  public void endEntity(String name) throws SAXException {
    if (counts(name)) {
      entityDepth--;
    }
    if (lexicalHandler != null) {
      lexicalHandler.endEntity(name);
    }
  }

  @Override
  public void startDTD(String name, String publicId, String systemId) throws SAXException {
    if (lexicalHandler != null) {
      lexicalHandler.startDTD(name, publicId, systemId);
    }
  }

  @Override
  public void endDTD() throws SAXException {
    if (lexicalHandler != null) {
      lexicalHandler.endDTD();
    }
  }

  @Override
  public void startCDATA() throws SAXException {
    if (lexicalHandler != null) {
      lexicalHandler.startCDATA();
    }
  }

  @Override
  public void endCDATA() throws SAXException {
    if (lexicalHandler != null) {
      lexicalHandler.endCDATA();
    }
  }

  @Override
  public void comment(char[] ch, int start, int length) throws SAXException {
    if (lexicalHandler != null) {
      lexicalHandler.comment(ch, start, length);
    }
  }

  // DeclHandler

  @Override
  public void elementDecl(String name, String model) throws SAXException {
    if (declHandler != null) {
      declHandler.elementDecl(name, model);
    }
  }

  @Override
  public void attributeDecl(String eName, String aName, String type, String mode, String value)
      throws SAXException {
    if (declHandler != null) {
      declHandler.attributeDecl(eName, aName, type, mode, value);
    }
  }

  @Override
  public void internalEntityDecl(String name, String value) throws SAXException {
    if (!name.startsWith("%")) {
      entitiesDeclared = true;
    }
    if (declHandler != null) {
      declHandler.internalEntityDecl(name, value);
    }
  }

  @Override
  public void externalEntityDecl(String name, String publicId, String systemId)
      throws SAXException {
    if (!name.startsWith("%")) {
      entitiesDeclared = true;
    }
    if (declHandler != null) {
      declHandler.externalEntityDecl(name, publicId, systemId);
    }
  }
}
