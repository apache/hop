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

import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

/**
 * Services the database workbench needs from whatever hosts it (perspective now; a floating dialog
 * or dock tab later). Keep Hop Gui specifics here so {@link DatabaseWorkbench} stays a plain {@link
 * org.eclipse.swt.widgets.Composite}.
 */
public interface IDatabaseWorkbenchHost {

  HopGui getHopGui();

  Shell getShell();

  Display getDisplay();

  IVariables getVariables();

  IHopMetadataProvider getMetadataProvider();

  ILoggingObject getLoggingObject();

  /** Bring this workbench to the front (activate the perspective, show the dialog, …). */
  void activate();

  /** Run {@code runnable} on the UI thread if the host is still alive. */
  void asyncExec(Runnable runnable);

  /** Refresh main-menu file capabilities for the active tab. */
  void updateGui(IHopFileTypeHandler handler);
}
