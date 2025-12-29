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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.neo4j.execution.path.PathResult;
import org.apache.hop.neo4j.logging.util.LoggingCore;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.TreeMemory;
import org.apache.hop.ui.core.widget.TreeUtil;
import org.apache.hop.ui.hopgui.shared.BaseExecutionViewer;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeColumn;
import org.eclipse.swt.widgets.TreeItem;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;

@GuiPlugin
public class NeoExecutionViewerLineageTab extends NeoExecutionViewerTabBase {
  private Tree wTree;

  /** The constructor is called every time a new tab is created in the pipeline execution viewer */
  public NeoExecutionViewerLineageTab(BaseExecutionViewer viewer) {
    super(viewer);
  }

  public void addNeoExecutionPathTab(CTabFolder tabFolder) {
    Image lineageImage =
        GuiResource.getInstance().getImage("lineage.svg", classLoader, iconSize, iconSize);

    CTabItem lineageTab = new CTabItem(tabFolder, SWT.NONE);
    lineageTab.setFont(GuiResource.getInstance().getFontDefault());
    lineageTab.setImage(lineageImage);
    lineageTab.setText(BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.Lineage.Tab"));

    Composite tabComposite = new Composite(tabFolder, SWT.NONE);
    lineageTab.setControl(tabComposite);
    tabComposite.setLayout(new FormLayout());

    Button wGo = new Button(tabComposite, SWT.PUSH);
    wGo.setText(BaseMessages.getString("System.Button.Open"));
    PropsUi.setLook(wGo);
    wGo.addListener(SWT.Selection, e -> openItem(wTree));
    BaseTransformDialog.positionBottomButtons(
        tabComposite, new Button[] {wGo}, PropsUi.getMargin(), null);

    wTree = new Tree(tabComposite, SWT.SINGLE | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(wTree);
    FormData fdTree = new FormData();
    fdTree.left = new FormAttachment(0, 0);
    fdTree.top = new FormAttachment(0, 0);
    fdTree.bottom = new FormAttachment(wGo, -PropsUi.getMargin());
    fdTree.right = new FormAttachment(100, 0);
    wTree.setLayoutData(fdTree);
    wTree.addListener(SWT.DefaultSelection, e -> openItem(wTree));
    wTree.setHeaderVisible(true);
    {
      TreeColumn column = new TreeColumn(wTree, SWT.LEFT);
      column.setText("#");
      column.setWidth((int) (50 * props.getZoomFactor()));
    }
    {
      TreeColumn column = new TreeColumn(wTree, SWT.LEFT);
      column.setText("id");
      column.setWidth((int) (250 * props.getZoomFactor()));
    }
    {
      TreeColumn column = new TreeColumn(wTree, SWT.LEFT);
      column.setText("Name");
      column.setWidth((int) (300 * props.getZoomFactor()));
    }
    {
      TreeColumn column = new TreeColumn(wTree, SWT.LEFT);
      column.setText("Type");
      column.setWidth((int) (100 * props.getZoomFactor()));
    }
    {
      TreeColumn column = new TreeColumn(wTree, SWT.LEFT);
      column.setText("Failed?");
      column.setWidth((int) (50 * props.getZoomFactor()));
    }
    {
      TreeColumn column = new TreeColumn(wTree, SWT.LEFT);
      column.setText("date");
      column.setWidth((int) (200 * props.getZoomFactor()));
    }

    // If the tab becomes visible, refresh
    //
    tabFolder.addListener(
        SWT.Selection,
        e -> {
          if (lineageTab == tabFolder.getSelection()) {
            // Cypher tab selected!
            refresh();
          }
        });
  }

  private void refresh() {
    // A list of the shortest path, maximum 10.
    List<List<PathResult>> lineagePaths = getLineageToRoot(getActiveLogChannelId());
    // Remove the elements in the tree
    //
    for (TreeItem treeItem : wTree.getItems()) {
      treeItem.dispose();
    }

    // Populate the tree...
    //
    String treeName = "Execution Lineage";

    for (int p = lineagePaths.size() - 1; p >= 0; p--) {
      List<PathResult> shortestPath = lineagePaths.get(p);

      TreeItem pathItem = new TreeItem(wTree, SWT.NONE);
      pathItem.setText(0, Integer.toString(p + 1));

      for (int e = 0; e < shortestPath.size(); e++) {
        PathResult exec = shortestPath.get(e);
        TreeItem execItem = new TreeItem(pathItem, SWT.NONE);
        int x = 0;
        execItem.setText(x++, Integer.toString(e + 1));
        execItem.setText(x++, Const.NVL(exec.getId(), ""));
        execItem.setText(x++, Const.NVL(exec.getName(), ""));
        execItem.setText(x++, Const.NVL(exec.getType(), ""));
        execItem.setText(x++, exec.getFailed() == null ? "" : exec.getFailed().toString());
        execItem.setText(x++, formatDate(exec.getRegistrationDate()));
        execItem.setExpanded(true);
      }
      if (p == lineagePaths.size() - 1) {
        TreeMemory.getInstance().storeExpanded(treeName, pathItem, true);
      }
    }

    TreeMemory.setExpandedFromMemory(wTree, treeName);

    if (wTree.getItemCount() > 0) {
      TreeItem firstItem = wTree.getItem(0);
      wTree.setSelection(firstItem);
    }

    TreeUtil.setOptimalWidthOnColumns(wTree);
  }

  private String formatDate(Date registrationDate) {
    if (registrationDate == null) {
      return "";
    }
    return new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(registrationDate);
  }

  private List<List<PathResult>> getLineageToRoot(String executionId) {
    // List of the shortest paths to the executionId
    //
    List<List<PathResult>> shortestPaths = new ArrayList<>();

    // Remove the elements in the tree
    //
    for (TreeItem treeItem : wTree.getItems()) {
      treeItem.dispose();
    }

    if (viewer.getExecution().getParentId() == null) {
      // There is no lineage since this is the parent
      return Collections.emptyList();
    }

    // Now get the lineage to the parent (if any)
    //
    Map<String, Object> pathParams = new HashMap<>();
    pathParams.put("executionId", executionId);
    String pathCypher = getPathToRootCypher();

    getSession()
        .executeRead(
            tx -> {
              Result pathResult = tx.run(pathCypher, pathParams);

              while (pathResult.hasNext()) {
                Record pathRecord = pathResult.next();
                Value pathValue = pathRecord.get(0);
                Path path = pathValue.asPath();
                List<PathResult> shortestPath = new ArrayList<>();
                for (Node node : path.nodes()) {
                  PathResult nodeResult = new PathResult();
                  nodeResult.setId(LoggingCore.getStringValue(node, "id"));
                  nodeResult.setName(LoggingCore.getStringValue(node, "name"));
                  nodeResult.setType(LoggingCore.getStringValue(node, "executionType"));
                  nodeResult.setFailed(LoggingCore.getBooleanValue(node, "failed"));
                  nodeResult.setRegistrationDate(
                      LoggingCore.getDateValue(node, "registrationDate"));
                  nodeResult.setCopy(LoggingCore.getStringValue(node, "copyNr"));

                  shortestPath.add(0, nodeResult);
                }
                shortestPaths.add(shortestPath);
              }
              //
              return null;
            });

    return shortestPaths;
  }
}
