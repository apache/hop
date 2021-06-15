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
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.transforms.sasinput.SasUtil;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.HopFileTypePlugin;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.IExplorerFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.base.BaseExplorerFileType;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Text;

import java.io.InputStream;
import java.util.List;
import java.util.Properties;

@HopFileTypePlugin(
    id = "SasExplorerFileType",
    name = "SAS File Type",
    description = "SAS file handling in the explorer perspective",
    image = "SAS.svg")
public class SasExplorerFileType extends BaseExplorerFileType<SasExplorerFileTypeHandler>
    implements IExplorerFileType<SasExplorerFileTypeHandler> {

  public SasExplorerFileType() {
    super(  "SAS file", ".sas7bdat", new String[] {"*.sas7bdat"}, new String[] {"SAS 7 BDAT files"}, new Properties() );
  }

  @Override
  public IHopFileTypeHandler newFile(HopGui hopGui, IVariables parentVariableSpace)
      throws HopException {
    return new EmptyHopFileTypeHandler();
  }

  @Override public SasExplorerFileTypeHandler createFileTypeHandler( HopGui hopGui, ExplorerPerspective perspective, ExplorerFile file ) {
    return new SasExplorerFileTypeHandler(hopGui, perspective, file);
  }
}
