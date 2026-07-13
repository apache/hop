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

package org.apache.hop.ui.hopgui.search;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchableCallback;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;

/** An {@link ISearchable} for a text-based explorer file (SQL, JSON, plain text, …). */
public class HopGuiTextFileSearchable implements ISearchable<TextFileContent> {

  private final String location;
  private final String type;
  private final TextFileContent content;

  public HopGuiTextFileSearchable(String location, String type, TextFileContent content) {
    this.location = location;
    this.type = type;
    this.content = content;
  }

  @Override
  public String getLocation() {
    return location;
  }

  @Override
  public String getName() {
    String filename = content.getFilename();
    if (filename == null || filename.isEmpty()) {
      return "";
    }
    try {
      FileObject fileObject = HopVfs.getFileObject(filename);
      FileName fileName = fileObject.getName();
      return fileName.getBaseName();
    } catch (Exception e) {
      int slash = Math.max(filename.lastIndexOf('/'), filename.lastIndexOf('\\'));
      return slash >= 0 ? filename.substring(slash + 1) : filename;
    }
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public String getFilename() {
    return content.getFilename();
  }

  @Override
  public TextFileContent getSearchableObject() {
    return content;
  }

  @Override
  public ISearchableCallback getSearchCallback() {
    return (searchable, searchResult) -> {
      String filename = content.getFilename();
      if (filename == null || filename.isEmpty()) {
        return;
      }
      try {
        ExplorerPerspective perspective = HopGui.getExplorerPerspective();
        IHopFileTypeHandler fileTypeHandler = perspective.findFileTypeHandlerByFilename(filename);
        if (fileTypeHandler != null) {
          perspective.setActiveFileTypeHandler(fileTypeHandler);
        } else {
          HopGui.getInstance().fileDelegate.fileOpen(filename);
        }
        perspective.activate();
      } catch (Exception e) {
        throw new HopException("Error opening searchable text file: " + filename, e);
      }
    };
  }
}
