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
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.neo4j.execution.NeoExecutionInfoLocation;
import org.apache.hop.neo4j.perspective.HopNeo4jPerspective;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.perspective.execution.ExecutionPerspective;
import org.apache.hop.ui.hopgui.shared.BaseExecutionViewer;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.neo4j.driver.Session;

public abstract class NeoExecutionViewerTabBase {
  public static final Class<?> PKG = HopNeo4jPerspective.class;

  protected final BaseExecutionViewer viewer;
  protected final PropsUi props;
  protected final ClassLoader classLoader;
  protected final int iconSize;

  public NeoExecutionViewerTabBase(BaseExecutionViewer viewer) {
    this.viewer = viewer;
    this.props = PropsUi.getInstance();
    this.classLoader = this.getClass().getClassLoader();
    this.iconSize = ConstUi.SMALL_ICON_SIZE;
  }

  protected static ExecutionInfoLocation getExecutionInfoLocation(BaseExecutionViewer viewer) {
    String locationName = viewer.getLocationName();
    ExecutionPerspective perspective = viewer.getPerspective();
    return perspective.getLocationMap().get(locationName);
  }

  protected String getActiveLogChannelId() {
    return viewer.getActiveId();
  }

  protected Session getSession() {
    ExecutionInfoLocation location = getExecutionInfoLocation(viewer);
    return ((NeoExecutionInfoLocation) location.getExecutionInfoLocation()).getSession();
  }

  protected String getPathToRootCypher() {
    // Do we have a parent?  If not we can just return the cypher to the current execution node
    //
    if (StringUtils.isEmpty(viewer.getExecution().getParentId())) {
      return "MATCH(e:Execution {id: $executionId }) " + Const.CR + "RETURN e " + Const.CR;
    } else {
      return "MATCH(top:Execution), (child:Execution {id: $executionId }), p=shortestPath((top)-[:EXECUTES*]-(child)) "
          + Const.CR
          + "WHERE top.parentId IS NULL "
          + Const.CR
          + "RETURN p "
          + Const.CR
          + "ORDER BY size(RELATIONSHIPS(p)) DESC "
          + Const.CR
          + "LIMIT 10 "
          + Const.CR;
    }
  }

  protected String getPathToFailedCypher() {

    return "MATCH(top:Execution {id: $executionId }), (child:Execution), p=shortestPath((top)-[:EXECUTES*]-(child)) "
        + Const.CR
        + "WHERE child.failed "
        + Const.CR
        + "AND   size((child)-[:EXECUTES]->())=0 "
        + Const.CR
        + "RETURN p "
        + Const.CR
        + "ORDER BY size(RELATIONSHIPS(p)) "
        + Const.CR
        + "LIMIT 10 "
        + Const.CR;
  }

  public void openItem(Tree tree) {
    if (tree.getSelectionCount() <= 0) {
      return;
    }
    TreeItem treeItem = tree.getSelection()[0];

    String childId = null;
    String id = treeItem.getText(1);
    String name = treeItem.getText(2);
    String type = treeItem.getText(3);

    ExecutionInfoLocation location = getExecutionInfoLocation(viewer);
    IExecutionInfoLocation iLocation = location.getExecutionInfoLocation();

    try {

      if (ExecutionType.Transform.name().equals(type)) {
        // Find the parent of this execution
        Execution transformExecution = iLocation.getExecution(id);
        childId = id;
        id = transformExecution.getParentId();
      } else if (ExecutionType.Action.name().equals(type)) {
        // Find the parent of this execution
        Execution actionExecution = iLocation.getExecution(id);
        childId = id;
        id = actionExecution.getParentId();
      }
      // Get the state
      //
      Execution execution = iLocation.getExecution(id);
      ExecutionState executionState = iLocation.getExecutionState(id);

      // Open the execution in a new viewer
      //
      viewer.getPerspective().createExecutionViewer(location.getName(), execution, executionState);
    } catch (Exception e) {
      new ErrorDialog(viewer.getShell(), "Error", "Error opening lineage item", e);
    }
  }

  /**
   * Gets viewer
   *
   * @return value of viewer
   */
  public BaseExecutionViewer getViewer() {
    return viewer;
  }

  /**
   * Gets props
   *
   * @return value of props
   */
  public PropsUi getProps() {
    return props;
  }

  /**
   * Gets classLoader
   *
   * @return value of classLoader
   */
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  /**
   * Gets iconSize
   *
   * @return value of iconSize
   */
  public int getIconSize() {
    return iconSize;
  }
}
