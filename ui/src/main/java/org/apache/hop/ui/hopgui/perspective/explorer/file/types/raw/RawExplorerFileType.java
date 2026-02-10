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

package org.apache.hop.ui.hopgui.perspective.explorer.file.types.raw;

import java.util.Properties;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.capabilities.FileTypeCapabilities;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.text.BaseTextExplorerFileType;

/**
 * File type for viewing any file as raw text in the explorer. When content looks like text, save is
 * enabled; when binary (null byte in sample), view is read-only and save is disabled. Never claims
 * a file by extension ({@link #isHandledBy} returns false), so it is only used when explicitly
 * opening as raw (e.g. via "Open as text" context menu).
 */
public class RawExplorerFileType extends BaseTextExplorerFileType<RawExplorerFileTypeHandler> {

  private static final Properties CAPABILITIES =
      FileTypeCapabilities.getCapabilities(
          IHopFileType.CAPABILITY_SAVE,
          IHopFileType.CAPABILITY_SAVE_AS,
          IHopFileType.CAPABILITY_CLOSE,
          IHopFileType.CAPABILITY_FILE_HISTORY,
          IHopFileType.CAPABILITY_COPY,
          IHopFileType.CAPABILITY_SELECT);

  public RawExplorerFileType() {
    super("Raw File", "", new String[0], new String[0], CAPABILITIES);
  }

  @Override
  public boolean isHandledBy(String filename, boolean checkContent) throws HopException {
    return false;
  }

  @Override
  public String getFileTypeImage() {
    return "ui/images/file.svg";
  }

  @Override
  public RawExplorerFileTypeHandler createFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile file) {
    return new RawExplorerFileTypeHandler(hopGui, perspective, file);
  }

  @Override
  public IHopFileTypeHandler newFile(HopGui hopGui, IVariables parentVariableSpace)
      throws HopException {
    return new EmptyHopFileTypeHandler();
  }
}
