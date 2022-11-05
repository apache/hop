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

import org.apache.hop.core.Const;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.neo4j.execution.NeoExecutionInfoLocation;
import org.apache.hop.neo4j.perspective.HopNeo4jPerspective;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.perspective.execution.ExecutionPerspective;
import org.apache.hop.ui.hopgui.shared.BaseExecutionViewer;
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

  protected String getPathToRootCypher(String executionId) {
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
