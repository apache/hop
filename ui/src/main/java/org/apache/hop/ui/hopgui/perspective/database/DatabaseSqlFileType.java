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

package org.apache.hop.ui.hopgui.perspective.database;

import java.util.List;
import java.util.Properties;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.HopFileTypeBase;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.file.capabilities.FileTypeCapabilities;

/**
 * File type of SQL editor tabs in the database perspective. {@link #isHandledBy} is always false so
 * Explorer keeps the default handler for {@code .sql} files.
 */
public class DatabaseSqlFileType extends HopFileTypeBase {

  private static final Properties CAPABILITIES =
      FileTypeCapabilities.getCapabilities(
          IHopFileType.CAPABILITY_SAVE,
          IHopFileType.CAPABILITY_SAVE_AS,
          IHopFileType.CAPABILITY_CLOSE,
          IHopFileType.CAPABILITY_FILE_HISTORY,
          IHopFileType.CAPABILITY_COPY,
          IHopFileType.CAPABILITY_CUT,
          IHopFileType.CAPABILITY_PASTE,
          IHopFileType.CAPABILITY_SELECT,
          IHopFileType.CAPABILITY_SEARCH);

  @Override
  public String getName() {
    return "Database SQL";
  }

  @Override
  public String getDefaultFileExtension() {
    return ".sql";
  }

  @Override
  public String[] getFilterExtensions() {
    return new String[] {"*.sql"};
  }

  @Override
  public String[] getFilterNames() {
    return new String[] {"SQL files"};
  }

  @Override
  public Properties getCapabilities() {
    return CAPABILITIES;
  }

  @Override
  public boolean hasCapability(String capability) {
    Object available = CAPABILITIES.get(capability);
    return available != null && "true".equalsIgnoreCase(available.toString());
  }

  @Override
  public IHopFileTypeHandler openFile(HopGui hopGui, String filename, IVariables variables)
      throws HopException {
    return new EmptyHopFileTypeHandler();
  }

  @Override
  public IHopFileTypeHandler newFile(HopGui hopGui, IVariables variables) throws HopException {
    return new EmptyHopFileTypeHandler();
  }

  @Override
  public boolean isHandledBy(String filename, boolean checkContent) {
    return false;
  }

  @Override
  public boolean supportsFile(IHasFilename metaObject) {
    return false;
  }

  @Override
  public List<IGuiContextHandler> getContextHandlers() {
    return List.of();
  }

  @Override
  public String getFileTypeImage() {
    return "ui/images/file.svg";
  }
}
