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

package org.apache.hop.ui.hopgui.perspective.explorer.file.types.text;

import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.base.BaseExplorerFileType;
import org.apache.hop.ui.hopgui.search.HopGuiTextFileSearchable;
import org.apache.hop.ui.hopgui.search.TextFileContent;

public abstract class BaseTextExplorerFileType<T extends BaseTextExplorerFileTypeHandler>
    extends BaseExplorerFileType<T> {

  public BaseTextExplorerFileType() {}

  public BaseTextExplorerFileType(
      String name,
      String defaultFileExtension,
      String[] filterExtensions,
      String[] filterNames,
      Properties capabilities) {
    super(name, defaultFileExtension, filterExtensions, filterNames, capabilities);
    if (capabilities != null) {
      capabilities.setProperty(IHopFileType.CAPABILITY_SEARCH, "true");
    }
  }

  public abstract T createFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile file);

  @Override
  public abstract IHopFileTypeHandler newFile(HopGui hopGui, IVariables parentVariableSpace)
      throws HopException;

  @Override
  public ISearchable createSearchable(
      String filename,
      String locationDescription,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    try {
      FileObject fileObject = HopVfs.getFileObject(filename, variables);
      if (!fileObject.exists()) {
        throw new HopException("File '" + filename + "' doesn't exist");
      }
      String text;
      try (InputStream inputStream = HopVfs.getInputStream(fileObject)) {
        StringWriter writer = new StringWriter();
        IOUtils.copy(inputStream, writer, StandardCharsets.UTF_8);
        text = writer.toString();
      }
      TextFileContent content = new TextFileContent(filename, text);
      return new HopGuiTextFileSearchable(locationDescription, getName(), content);
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Error loading text file for search: " + filename, e);
    }
  }
}
