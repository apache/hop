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

package org.apache.hop.pipeline.transforms.sasinput.types;

import com.epam.parso.Column;
import com.epam.parso.ColumnFormat;
import com.epam.parso.impl.SasFileReaderImpl;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.transforms.sasinput.SasUtil;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.IExplorerFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.base.BaseExplorerFileTypeHandler;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Text;

import java.io.InputStream;
import java.util.List;

/** How do we handle an SVG file in file explorer perspective? */
public class SasExplorerFileTypeHandler extends BaseExplorerFileTypeHandler
    implements IExplorerFileTypeHandler {

  public SasExplorerFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile explorerFile) {
    super(hopGui, perspective, explorerFile);
  }

  @Override
  public void renderFile(Composite composite) {
    // Render the file by simply showing the filename ...
    // TODO: create a TableView based in the file content & load it up...
    //
    //
    Text wText = new Text(composite, SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL);
    PropsUi.getInstance().setLook(wText, Props.WIDGET_STYLE_FIXED);
    wText.setEditable(false);
    FormData fdText = new FormData();
    fdText.left = new FormAttachment(0, 0);
    fdText.right = new FormAttachment(100, 0);
    fdText.top = new FormAttachment(0, 0);
    fdText.bottom = new FormAttachment(100, 0);
    wText.setLayoutData(fdText);

    String message = explorerFile.getFilename() + Const.CR;
    message += Const.CR;

    try {
      try (InputStream inputStream = HopVfs.getInputStream(explorerFile.getFilename())) {
        SasFileReaderImpl sasFileReader = new SasFileReaderImpl(inputStream);

        List<Column> columns = sasFileReader.getColumns();
        for (int c = 0; c < columns.size(); c++) {
          Column column = columns.get(c);
          ColumnFormat format = column.getFormat();

          int length = format.getWidth() == 0 ? -1 : format.getWidth();
          int precision = format.getPrecision() == 0 ? -1 : format.getWidth();

          message += "Column " + (c + 1) + Const.CR;
          message += "   Name      : " + Const.NVL(column.getName(), "") + Const.CR;
          message += "   Type      : " + SasUtil.getHopDataTypeDesc(column.getType()) + Const.CR;
          message += "   Length    : " + (length < 0 ? "" : Integer.toString(length)) + Const.CR;
          message +=
              "   Precision : " + (precision < 0 ? "" : Integer.toString(precision)) + Const.CR;
        }
      } catch (Exception e) {
        throw new HopException("Error reading from file: " + explorerFile.getFilename());
      }
    } catch (Exception e) {
      message += Const.CR + Const.getSimpleStackTrace(e);
    }
    wText.setText(message);
  }
}
