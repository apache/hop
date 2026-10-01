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

package org.apache.hop.ui.hopgui.perspective.explorer.file;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
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
    id = "ParquetFileType",
    name = "Parquet File Type",
    description = "Apache Parquet file handling in the explorer perspective",
    image = "parquet.svg")
public class ParquetFileType extends BaseExplorerFileType<ParquetExplorerFileTypeHandler> {

  public ParquetFileType() {
    super(
        "Parquet file",
        ".parquet",
        new String[] {"*.parquet;*.parq"},
        new String[] {"Parquet files"},
        FileTypeCapabilities.getCapabilities(
            IHopFileType.CAPABILITY_CLOSE, IHopFileType.CAPABILITY_FILE_HISTORY));
  }

  /**
   * Opens a local file without the large-file confirmation. The preview seeks to the footer and
   * reads at most 1000 rows. Any other VFS scheme keeps {@link BaseExplorerFileType#openFile}'s
   * confirmation: a seek there re-reads the file from the start.
   */
  @Override
  public ParquetExplorerFileTypeHandler openFile(
      HopGui hopGui, String filename, IVariables variables) throws HopException {
    try {
      FileObject fileObject = HopVfs.getFileObject(filename, variables);
      if (keepsLargeFileConfirmation(fileObject)) {
        fileObject.close();
        return super.openFile(hopGui, filename, variables);
      }
      String name = fileObject.getName().getBaseName();
      filename = HopVfs.getFilename(fileObject);

      ExplorerFile explorerFile = new ExplorerFile();
      explorerFile.setName(Const.NVL(name, ""));
      explorerFile.setFilename(filename);
      explorerFile.setFileType(this);

      ExplorerPerspective perspective = ExplorerPerspective.getInstance();
      ParquetExplorerFileTypeHandler fileTypeHandler =
          createFileTypeHandler(hopGui, perspective, explorerFile);
      perspective.addFile(fileTypeHandler);
      return fileTypeHandler;
    } catch (Exception e) {
      throw new HopException(
          "Error opening file '" + filename + "' in a new tab in the Explorer perspective", e);
    }
  }

  /**
   * Remote and non-file locations re-stream the object on every seek, so a large open still asks. A
   * plain {@code file} location can seek and skips the confirmation.
   */
  static boolean keepsLargeFileConfirmation(FileObject fileObject) {
    return fileObject.getName() == null || !"file".equals(fileObject.getName().getScheme());
  }

  @Override
  public ParquetExplorerFileTypeHandler createFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile file) {
    return new ParquetExplorerFileTypeHandler(hopGui, perspective, file);
  }

  @Override
  public IHopFileTypeHandler newFile(HopGui hopGui, IVariables parentVariableSpace)
      throws HopException {
    return new EmptyHopFileTypeHandler();
  }
}
