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

package org.apache.hop.pipeline.transforms.types;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.HopFileTypePlugin;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.capabilities.FileTypeCapabilities;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.base.BaseExplorerFileType;

@HopFileTypePlugin(
    id = "HtmlExplorerFileType",
    name = "HTML File Type",
    description = "HTML file handling in the explorer perspective",
    image = "ui/images/html.svg")
public class HtmlExplorerFileType extends BaseExplorerFileType<HtmlExplorerFileTypeHandler> {

  public HtmlExplorerFileType() {
    super(
        "HTML File",
        ".html",
        new String[] {"*.html", "*.htm"},
        new String[] {"HTML files"},
        FileTypeCapabilities.getCapabilities(
            IHopFileType.CAPABILITY_SAVE,
            IHopFileType.CAPABILITY_SAVE_AS,
            IHopFileType.CAPABILITY_CLOSE,
            IHopFileType.CAPABILITY_FILE_HISTORY,
            IHopFileType.CAPABILITY_COPY,
            IHopFileType.CAPABILITY_SELECT));
  }

  @Override
  public HtmlExplorerFileTypeHandler createFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile file) {
    return new HtmlExplorerFileTypeHandler(hopGui, perspective, file);
  }

  @Override
  public IHopFileTypeHandler newFile(HopGui hopGui, IVariables parentVariableSpace)
      throws HopException {
    return new EmptyHopFileTypeHandler();
  }

  @Override
  public HtmlExplorerFileTypeHandler openFile(HopGui hopGui, String filename, IVariables variables)
      throws HopException {
    String resolvedFilename = variables.resolve(filename);
    if (resolvedFilename.toLowerCase().startsWith("http://")
        || resolvedFilename.toLowerCase().startsWith("https://")) {
      // Create handler without VFS checks for URLs
      //
      ExplorerFile explorerFile = new ExplorerFile();
      String name = extractTitleFromUrl(resolvedFilename);
      explorerFile.setName(name);
      explorerFile.setFilename(resolvedFilename);
      explorerFile.setFileType(this);

      ExplorerPerspective perspective = ExplorerPerspective.getInstance();
      HtmlExplorerFileTypeHandler handler =
          createFileTypeHandler(hopGui, perspective, explorerFile);
      perspective.addFile(handler);
      return handler;
    }
    return super.openFile(hopGui, filename, variables);
  }

  /**
   * Extract a meaningful title from a URL for use as a tab name.
   *
   * @param url The URL to extract a title from
   * @return A short, meaningful title (max 30 characters)
   */
  private String extractTitleFromUrl(String url) {
    try {
      // Remove protocol and query parameters
      String path = url;
      if (path.contains("?")) {
        path = path.substring(0, path.indexOf("?"));
      }
      if (path.contains("#")) {
        path = path.substring(0, path.indexOf("#"));
      }

      // Extract the last meaningful part of the path
      // e.g., "https://hop.apache.org/manual/latest/pipelines/transforms/data-grid.html"
      // becomes "Data Grid"
      String[] parts = path.split("/");
      String lastPart = "";
      for (int i = parts.length - 1; i >= 0; i--) {
        if (!parts[i].isEmpty() && !parts[i].equals("manual") && !parts[i].equals("latest")) {
          lastPart = parts[i];
          break;
        }
      }

      // Remove file extension and decode
      if (lastPart.endsWith(".html") || lastPart.endsWith(".htm")) {
        lastPart = lastPart.substring(0, lastPart.lastIndexOf("."));
      }

      // Convert kebab-case, snake_case, or camelCase to Title Case
      // e.g., "data-grid" -> "Data Grid", "data_grid" -> "Data Grid"
      String title = lastPart.replaceAll("[-_]", " ");
      title = title.replaceAll("([a-z])([A-Z])", "$1 $2"); // camelCase

      // Capitalize words
      String[] words = title.split("\\s+");
      StringBuilder result = new StringBuilder();
      for (String word : words) {
        if (word.length() > 0) {
          if (result.length() > 0) {
            result.append(" ");
          }
          result.append(word.substring(0, 1).toUpperCase());
          if (word.length() > 1) {
            result.append(word.substring(1).toLowerCase());
          }
        }
      }
      title = result.toString();

      // If we got a meaningful title, use it (limit to 30 chars)
      if (!title.isEmpty() && title.length() <= 30) {
        return title;
      }

      // Fallback: show domain + last part (truncated to 30 chars)
      if (title.length() > 30) {
        title = title.substring(0, 27) + "...";
      }

      // If still no good title, use smart truncation of the full URL
      if (title.isEmpty() || title.length() < 5) {
        // Show domain and last path segment
        int domainEnd = path.indexOf("/", 8); // After "https://"
        if (domainEnd > 0 && domainEnd < path.length() - 1) {
          String domain = path.substring(0, domainEnd);
          String pathPart = path.substring(domainEnd);
          if (pathPart.length() > 20) {
            pathPart = "..." + pathPart.substring(pathPart.length() - 17);
          }
          title = domain + pathPart;
        } else {
          title = path;
        }
        if (title.length() > 30) {
          title = title.substring(0, 27) + "...";
        }
      }

      return title;
    } catch (Exception e) {
      // Fallback to simple truncation if anything goes wrong
      if (url.length() > 30) {
        return url.substring(0, 27) + "...";
      }
      return url;
    }
  }
}
