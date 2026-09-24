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

package org.apache.hop.pipeline.transforms.webservices.wsdl;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import javax.wsdl.xml.WSDLLocator;
import org.apache.commons.lang3.Strings;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.xml.sax.InputSource;

/**
 * Finds a WSDL and everything it imports, the WSDL imports and the XML schemas it includes, through
 * Hop VFS or over http(s).
 *
 * <p>Without a locator WSDL4J opens every import with {@link java.net.URL}, which knows file: and
 * http(s) but none of the other file systems Hop reads from: a WSDL on S3, Azure or HDFS loaded
 * fine and then failed on its first relative import.
 *
 * <p>A relative location is resolved against the document that names it, so an import can import in
 * turn. The user name and password go along with http(s) requests to the host the WSDL itself came
 * from, and to no other.
 *
 * <p>Everything this opens stays open until {@link #close()}, which the caller must call once the
 * WSDL has been read: WSDL4J reads an import after asking for it, not while.
 */
final class HopVfsWsdlLocator implements WSDLLocator, AutoCloseable {

  private final String baseUri;
  private final IVariables variables;
  private final String username;
  private final String password;
  private final List<AutoCloseable> opened = new ArrayList<>();
  private String latestImportUri;

  /**
   * @param baseUri the absolute location of the WSDL: an http(s) URL or a Hop VFS URI
   * @param variables to find the named VFS connections with, can be null
   * @param username for HTTP authentication with the WSDL's own host, can be null
   * @param password for HTTP authentication with the WSDL's own host, can be null
   */
  HopVfsWsdlLocator(String baseUri, IVariables variables, String username, String password) {
    this.baseUri = baseUri;
    this.variables = variables;
    this.username = username;
    this.password = password;
  }

  @Override
  public InputSource getBaseInputSource() {
    try {
      return inputSource(baseUri);
    } catch (HopException e) {
      throw new HopRuntimeException(e.getMessage(), e);
    }
  }

  /**
   * The stream of the WSDL itself, closed by {@link #close()} at the latest.
   *
   * @throws HopException when the WSDL does not exist or cannot be read
   */
  InputStream openBase() throws HopException {
    return open(baseUri);
  }

  @Override
  public InputSource getImportInputSource(String parentLocation, String importLocation) {
    String location = resolve(parentLocation == null ? baseUri : parentLocation, importLocation);
    latestImportUri = location;
    try {
      return inputSource(location);
    } catch (HopException e) {
      throw new HopRuntimeException(
          "Unable to read " + importLocation + ", imported by " + parentLocation, e);
    }
  }

  @Override
  public String getBaseURI() {
    return baseUri;
  }

  @Override
  public String getLatestImportURI() {
    return latestImportUri;
  }

  @Override
  public void close() {
    for (AutoCloseable closeable : opened) {
      try {
        closeable.close();
      } catch (Exception e) {
        // Nothing left to read from it; closing is best effort.
      }
    }
    opened.clear();
  }

  private InputSource inputSource(String location) throws HopException {
    InputSource source = new InputSource(open(location));
    source.setSystemId(location);
    return source;
  }

  private InputStream open(String location) throws HopException {
    if (Wsdl.isHttpLocation(location)) {
      return remember(openHttp(location));
    }
    FileObject file = remember(fileObject(location));
    try {
      if (!file.exists()) {
        throw new HopException("WSDL file " + location + " does not exist");
      }
      return remember(HopVfs.getInputStream(file));
    } catch (IOException e) {
      throw new HopException("Unable to read WSDL file " + location, e);
    }
  }

  private InputStream openHttp(String url) throws HopException {
    try {
      URLConnection connection = new URL(url).openConnection();
      if (username != null && !username.isEmpty() && sameOrigin(url, baseUri)) {
        String raw = username + ":" + (password == null ? "" : password);
        String encoded = Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
        connection.setRequestProperty("Authorization", "Basic " + encoded);
      }
      return connection.getInputStream();
    } catch (IOException e) {
      throw new HopException("Unable to read WSDL from " + url, e);
    }
  }

  /**
   * An absolute location as it stands; a relative one next to the document that names it: over
   * http(s) by URL resolution, anywhere else through Hop VFS, which knows how the file system of
   * the parent spells a path.
   */
  private String resolve(String parentLocation, String location) {
    if (isAbsolute(location)) {
      return location;
    }
    try {
      if (Wsdl.isHttpLocation(parentLocation)) {
        return URI.create(parentLocation).resolve(location).toString();
      }
      try (FileObject parent = fileObject(parentLocation);
          FileObject folder = parent.getParent();
          FileObject resolved = folder.resolveFile(location)) {
        return resolved.getName().getURI();
      }
    } catch (Exception e) {
      throw new HopRuntimeException(
          "Unable to resolve " + location + " relative to " + parentLocation, e);
    }
  }

  /** True for a location with a scheme of its own: http:, file:, s3:, a named VFS connection. */
  private static boolean isAbsolute(String location) {
    try {
      // A single letter is a Windows drive, not a scheme.
      String scheme = new URI(location).getScheme();
      return scheme != null && scheme.length() > 1;
    } catch (URISyntaxException e) {
      return false;
    }
  }

  private static boolean sameOrigin(String url, String other) {
    if (!Wsdl.isHttpLocation(other)) {
      return false;
    }
    URI a = URI.create(url);
    URI b = URI.create(other);
    return Strings.CI.equals(a.getScheme(), b.getScheme())
        && Strings.CI.equals(a.getHost(), b.getHost())
        && port(a) == port(b);
  }

  private static int port(URI uri) {
    if (uri.getPort() != -1) {
      return uri.getPort();
    }
    return "https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80;
  }

  private FileObject fileObject(String location) throws HopException {
    return variables == null
        ? HopVfs.getFileObject(location)
        : HopVfs.getFileObject(location, variables);
  }

  private <T extends AutoCloseable> T remember(T closeable) {
    opened.add(Objects.requireNonNull(closeable));
    return closeable;
  }
}
