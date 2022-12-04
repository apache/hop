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
 *
 */

package org.apache.hop.neo4j.execution.path.base;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.shared.BaseExecutionViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Text;

@GuiPlugin
public class NeoExecutionViewerCypherTab extends NeoExecutionViewerTabBase {
  private Text wCypher;

  /** The constructor is called every time a new tab is created in the pipeline execution viewer */
  public NeoExecutionViewerCypherTab(BaseExecutionViewer viewer) {
    super(viewer);
  }

  public void addNeo4jCypherTab(CTabFolder tabFolder) {
    Image neo4jImage =
        GuiResource.getInstance().getImage("neo4j_cypher.svg", classLoader, iconSize, iconSize);
    CTabItem cypherTab = new CTabItem(tabFolder, SWT.NONE);
    cypherTab.setFont(GuiResource.getInstance().getFontDefault());
    cypherTab.setText(BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.Cypher.Tab"));
    cypherTab.setImage(neo4jImage);
    wCypher = new Text(tabFolder, SWT.MULTI | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    PropsUi.setLook(wCypher);
    wCypher.setFont(GuiResource.getInstance().getFontFixed());
    cypherTab.setControl(wCypher);

    // If the tab becomes visible, refresh
    //
    tabFolder.addListener(
        SWT.Selection,
        e -> {
          if (cypherTab == tabFolder.getSelection()) {
            // Cypher tab selected!
            refresh();
          }
        });
  }

  private void refresh() {
    String cypher;
    if (StringUtils.isEmpty(viewer.getExecution().getParentId())) {
      cypher =
          "This execution does not have a parent and is as such a root execution. "
              + Const.CR
              + "You can look it up like this:"
              + Const.CR
              + Const.CR;
    } else {
      cypher =
          "This is the Cypher used to get the list of paths to the root execution:"
              + Const.CR
              + Const.CR;
    }
    String rootCypher = getPathToRootCypher();
    String activeId = getActiveLogChannelId();
    cypher += rootCypher.replace("$executionId", '"' + activeId + '"');

    ExecutionState state = viewer.getExecutionState();
    if (state != null) {
      cypher += Const.CR + Const.CR;
      cypher +=
          "Execute this Cypher to get the paths to the lowest level failed execution nodes, if there are any:"
              + Const.CR
              + Const.CR;
      cypher += getPathToFailedCypher();
    }
    cypher = cypher.replace("$executionId", "\"" + getActiveLogChannelId() + "\"");
    wCypher.setText(cypher);
  }
}
