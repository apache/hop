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

package org.apache.hop.ui.hopgui.perspective.explorer.file.types.log;

import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.IExplorerFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.text.BaseTextExplorerFileTypeHandler;
import org.eclipse.swt.widgets.Text;

/**
 * How do we handle a log file in file explorer perspective?
 *
 * <p>
 *
 * <p>TODO: add bottom section to show status, size, cursor position...
 *
 * <p>TODO: add a checkbox so that we can implement a "tail -f" log viewer
 *
 * <p>
 */
public class LogExplorerFileTypeHandler extends BaseTextExplorerFileTypeHandler
    implements IExplorerFileTypeHandler {

  private Text wText;

  public LogExplorerFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile explorerFile) {
    super(hopGui, perspective, explorerFile);
  }
}
