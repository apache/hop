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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URI;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.validation.SchemaFactory;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.EnvUtil;
import org.w3c.dom.ls.LSInput;
import org.w3c.dom.ls.LSResourceResolver;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;

public class XmlParserFactoryProducer {

  /**
   * Value for {@link XMLConstants#ACCESS_EXTERNAL_SCHEMA} that keeps schema resolution on the local
   * file system. {@code xs:include} and {@code xs:import} of a local schema document still resolve,
   * while a fetch over http, https or ftp is refused.
   */
  private static final String LOCAL_FILE_ACCESS_ONLY = "file";

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
  @SuppressWarnings("java:S2755")
  public static DocumentBuilderFactory createSecureDocBuilderFactory()
      throws ParserConfigurationException {
    DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
    docBuilderFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    docBuilderFactory.setFeature(
        "http://apache.org/xml/features/disallow-doctype-decl",
        "N".equals(EnvUtil.getSystemProperty(Const.XML_ALLOW_DOCTYPE_DECL)));

    String[] featuresToDisable = {
      // Xerces 1 - http://xerces.apache.org/xerces-j/features.html#external-general-entities
      // Xerces 2 - http://xerces.apache.org/xerces2-j/features.html#external-general-entities
      // JDK7+ - http://xml.org/sax/features/external-general-entities
      // This feature has to be used together with the following one, otherwise it will not protect
      // you from XXE for sure
      "http://xml.org/sax/features/external-general-entities",

      // Xerces 1 - http://xerces.apache.org/xerces-j/features.html#external-parameter-entities
      // Xerces 2 - http://xerces.apache.org/xerces2-j/features.html#external-parameter-entities
      // JDK7+ - http://xml.org/sax/features/external-parameter-entities
      // This feature has to be used together with the previous one, otherwise it will not protect
      // you from XXE for sure
      "http://xml.org/sax/features/external-parameter-entities",

      // Disable external DTDs as well
      "http://apache.org/xml/features/nonvalidating/load-external-dtd"
    };
    for (String feature : featuresToDisable) {
      try {
        docBuilderFactory.setFeature(feature, false);
      } catch (ParserConfigurationException e) {
        // This should catch a failed setFeature feature
        if (LogChannel.GENERAL.isDetailed()) {
          LogChannel.GENERAL.logDetailed(
              "ParserConfigurationException was thrown. The feature '"
                  + feature
                  + "' is probably not supported by your XML processor.");
        }
      }
    }

    docBuilderFactory.setXIncludeAware(false);
    docBuilderFactory.setExpandEntityReferences(false);
    docBuilderFactory.setValidating(false);
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

  /**
   * Creates an instance of {@link SchemaFactory} class with enabled {@link
   * XMLConstants#FEATURE_SECURE_PROCESSING} property, external DTD access denied and external
   * schema access restricted to the local file system.
   *
   * <p>Hardening the factory matters separately from hardening the {@link
   * javax.xml.validation.Validator} it produces: the factory is what resolves the schema document
   * itself, so without these restrictions a schema is free to pull in a DTD or another schema over
   * the network before any validation begins.
   *
   * <p>Not every {@link SchemaFactory} implementation recognizes the JAXP access properties --
   * Xerces' standalone {@code XMLSchemaFactory} does not. For those, a resource resolver enforces
   * the same local-file-only policy.
   *
   * @param schemaLanguage the schema language URI, e.g. {@link XMLConstants#W3C_XML_SCHEMA_NS_URI}
   * @throws SAXNotRecognizedException When the underlying parser does not recognize the property
   *     name.
   * @throws SAXNotSupportedException When the underlying parser recognizes the property name but
   *     doesn't support the property.
   */
  public static SchemaFactory createSecureSchemaFactory(String schemaLanguage)
      throws SAXNotRecognizedException, SAXNotSupportedException {
    SchemaFactory factory = SchemaFactory.newInstance(schemaLanguage);
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    try {
      factory.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
      factory.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, LOCAL_FILE_ACCESS_ONLY);
    } catch (SAXNotRecognizedException | SAXNotSupportedException e) {
      // Xerces' standalone XMLSchemaFactory, the schema factory on Hop's runtime classpath, does
      // not know the JAXP access properties. Refusing the factory outright would break every
      // validation, so apply the same "local file system only" policy through a resource resolver.
      factory.setResourceResolver(new LocalFileOnlyResourceResolver());
    }

    return factory;
  }

  /**
   * Resolves {@code xs:include} and {@code xs:import} on the local file system only, and refuses
   * external DTDs outright. It is the fallback for parsers that do not recognize {@link
   * XMLConstants#ACCESS_EXTERNAL_DTD} and {@link XMLConstants#ACCESS_EXTERNAL_SCHEMA}: a schema
   * reference that resolves to any other scheme (http, https, ftp, jar, ...) is refused instead of
   * fetched.
   */
  private static class LocalFileOnlyResourceResolver implements LSResourceResolver {

    /** The {@code type} {@link LSResourceResolver} gets for an external DTD or entity. */
    private static final String DTD_RESOURCE_TYPE = "http://www.w3.org/TR/REC-xml";

    @Override
    public LSInput resolveResource(
        String type, String namespaceUri, String publicId, String systemId, String baseUri) {
      if (systemId == null) {
        // Nothing to fetch: let the parser resolve it as it normally would.
        return null;
      }
      if (DTD_RESOURCE_TYPE.equals(type)) {
        // Mirrors ACCESS_EXTERNAL_DTD="": no external DTD, local file system included.
        return new RefusedInput(publicId, systemId, baseUri);
      }
      return isLocalFile(systemId, baseUri) ? null : new RefusedInput(publicId, systemId, baseUri);
    }

    private boolean isLocalFile(String systemId, String baseUri) {
      try {
        URI uri = URI.create(systemId);
        if (baseUri != null && !uri.isAbsolute()) {
          uri = URI.create(baseUri).resolve(uri);
        }
        // A reference that stays relative is resolved against the schema's own location.
        return !uri.isAbsolute() || LOCAL_FILE_ACCESS_ONLY.equalsIgnoreCase(uri.getScheme());
      } catch (IllegalArgumentException e) {
        // Not a URI we can reason about, so don't hand it to the parser either.
        return false;
      }
    }
  }

  /**
   * An {@link LSInput} that fails as soon as the parser reads from it, so a blocked reference ends
   * up as a validation error instead of a network call.
   */
  private static class RefusedInput implements LSInput {

    private final String publicId;
    private final String systemId;
    private final String baseUri;

    RefusedInput(String publicId, String systemId, String baseUri) {
      this.publicId = publicId;
      this.systemId = systemId;
      this.baseUri = baseUri;
    }

    @Override
    public InputStream getByteStream() {
      return new InputStream() {
        @Override
        public int read() throws IOException {
          throw new IOException(
              "External schema or DTD access is not allowed, refused to resolve: " + systemId);
        }
      };
    }

    @Override
    public Reader getCharacterStream() {
      return null;
    }

    @Override
    public void setCharacterStream(Reader characterStream) {
      // Read-only
    }

    @Override
    public void setByteStream(InputStream byteStream) {
      // Read-only
    }

    @Override
    public String getStringData() {
      return null;
    }

    @Override
    public void setStringData(String stringData) {
      // Read-only
    }

    @Override
    public String getSystemId() {
      return systemId;
    }

    @Override
    public void setSystemId(String systemId) {
      // Read-only
    }

    @Override
    public String getPublicId() {
      return publicId;
    }

    @Override
    public void setPublicId(String publicId) {
      // Read-only
    }

    @Override
    public String getBaseURI() {
      return baseUri;
    }

    @Override
    public void setBaseURI(String baseUri) {
      // Read-only
    }

    @Override
    public String getEncoding() {
      return null;
    }

    @Override
    public void setEncoding(String encoding) {
      // Read-only
    }

    @Override
    public boolean getCertifiedText() {
      return false;
    }

    @Override
    public void setCertifiedText(boolean certifiedText) {
      // Read-only
    }
  }

  /**
   * Creates an instance of {@link XMLInputFactory} with DTD processing and external entity
   * resolution disabled to protect against XML External Entity (XXE) attacks and XML entity
   * expansion bombs.
   *
   * <p>{@link XMLConstants#ACCESS_EXTERNAL_DTD} and {@link XMLConstants#ACCESS_EXTERNAL_SCHEMA} are
   * set when the StAX provider recognizes them. Woodstox (the factory on Hop's runtime classpath)
   * does not, so those two calls are best-effort.
   */
  public static XMLInputFactory createSecureXmlInputFactory() {
    XMLInputFactory factory = XMLInputFactory.newInstance();
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
    try {
      factory.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
      factory.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
    } catch (IllegalArgumentException e) {
      // Property not supported by this StAX provider
    }
    return factory;
  }
}
