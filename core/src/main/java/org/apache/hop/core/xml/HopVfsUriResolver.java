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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.file.Path;
import java.util.regex.Pattern;
import javax.xml.transform.Source;
import javax.xml.transform.URIResolver;
import javax.xml.transform.stream.StreamSource;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;

/**
 * Resolves {@code xsl:include}, {@code xsl:import} and {@code document()} references through Hop
 * VFS, so they work on every file system Hop can read, not only the ones {@link java.net.URL}
 * knows.
 *
 * <p>A relative reference resolves next to the file that holds it. When nothing exists there, it
 * resolves as it did before the stylesheet's location was known: an absolute path as it is, a
 * relative one against the working directory.
 */
public class HopVfsUriResolver implements URIResolver {

  /** A URI scheme of two or more characters, so a Windows drive letter is not taken for one. */
  private static final Pattern URI_SCHEME = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+.-]+:.*");

  private final IVariables variables;

  public HopVfsUriResolver(IVariables variables) {
    this.variables = variables;
  }

  @Override
  public Source resolve(String href, String base) {
    if (Utils.isEmpty(href) || href.startsWith("#")) {
      // The stylesheet itself, or a fragment of it.
      return null;
    }
    try {
      if (URI_SCHEME.matcher(href).matches()) {
        // An absolute URI. The transformer opens those that java.net.URL supports, as it always
        // did; Hop VFS opens the others (s3://, gs://, named connections, ...).
        return isUrl(href) ? null : source(HopVfs.getFileObject(href, variables));
      }
      if (!Utils.isEmpty(base)) {
        FileObject parent = HopVfs.getFileObject(base, variables).getParent();
        FileObject file = parent == null ? null : parent.resolveFile(href);
        if (file != null && file.exists()) {
          return source(file);
        }
      }
    } catch (Exception e) {
      // Fall back to the resolution below.
    }
    return againstWorkingDirectory(href);
  }

  private static boolean isUrl(String href) {
    try {
      URI.create(href).toURL();
      return true;
    } catch (IllegalArgumentException | MalformedURLException e) {
      return false;
    }
  }

  /**
   * A local file goes to the transformer as a URI, so it opens and closes the file itself. Any
   * other file system is read through Hop VFS.
   */
  private static Source source(FileObject file) throws Exception {
    String uri = file.getName().getURI();
    if ("file".equalsIgnoreCase(file.getName().getScheme())) {
      return new StreamSource(uri);
    }
    try (InputStream inputStream = HopVfs.getInputStream(file)) {
      return new StreamSource(new ByteArrayInputStream(inputStream.readAllBytes()), uri);
    }
  }

  /** How a reference resolved while the transformer did not know where the stylesheet was. */
  private static Source againstWorkingDirectory(String href) {
    Path workingDirectory = Path.of(Const.NVL(System.getProperty("user.dir"), ""));
    try {
      return new StreamSource(workingDirectory.toUri().resolve(href).toString());
    } catch (IllegalArgumentException e) {
      // Not URI syntax, for example a space or a Windows path: take it as a file path.
      try {
        return new StreamSource(workingDirectory.resolve(href).toUri().toString());
      } catch (Exception notAPath) {
        return null;
      }
    }
  }
}
