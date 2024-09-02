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
 *
 */

package org.apache.hop.vfs.azure;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.HostFileNameParser;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.apache.hop.vfs.azure.config.AzureConfig;
import org.apache.hop.vfs.azure.config.AzureConfigSingleton;

public class AzureFileNameParser extends HostFileNameParser {

  private static final AzureFileNameParser instance = new AzureFileNameParser();

  public static AzureFileNameParser getInstance() {
    return instance;
  }

  public AzureFileNameParser() {
    super(443);
  }

  @Override
  public FileName parseUri(final VfsComponentContext context, FileName base, String uri)
      throws FileSystemException {
    StringBuilder sb = new StringBuilder(uri);

    UriParser.normalisePath(sb);
    String normalizedUri = sb.toString();
    String scheme = normalizedUri.substring(0, normalizedUri.indexOf(':'));

    UriParser.normalisePath(sb);

    String absPath = "/";
    FileType fileType = FileType.IMAGINARY;
    String[] s = normalizedUri.split("/");

    if (s.length > 1) {
      if (scheme.equals("azure")) {

        String container = s[1];
        for (int i = 1; i < s.length; i++) {
          absPath += s[i];

          if (s.length > 1 && i != s.length - 1) {
            absPath += "/";
          }
        }
        fileType = getFileType(uri);

      } else if (scheme.equals("azfs")) {
        fileType = getFileType(uri);
        String path =
            normalizedUri.substring(normalizedUri.indexOf('/', 1), normalizedUri.length());
        AzureConfig azureConfig = AzureConfigSingleton.getConfig();
        absPath = stripFirstFolder(path);
      }
    }
    return new AzureFileName(scheme, absPath, fileType);
  }

  private FileType getFileType(String uri) {
    if (uri.endsWith("/")) {
      return FileType.FOLDER;
    } else {
      return FileType.FILE;
    }
  }

  public static String stripFirstFolder(String path) {
    // Normalize the path to ensure correct separators
    String normalizedPath = FilenameUtils.normalizeNoEndSeparator(path, true);

    // Remove the leading '/'
    String withoutLeadingSlash =
        normalizedPath.startsWith("/") ? normalizedPath.substring(1) : normalizedPath;

    // Find the index of the first '/'
    int index = withoutLeadingSlash.indexOf("/");

    // If '/' is found, return the substring after the first folder
    if (index != -1) {
      return "/" + withoutLeadingSlash.substring(index + 1);
    } else {
      // If there's no '/', return an empty string or the original path
      return "";
    }
  }
}
