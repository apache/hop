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

package org.apache.hop.ui.hopgui.perspective.explorer.file;

import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;

public interface IExplorerFileType<T extends IExplorerFileTypeHandler> extends IHopFileType {

  /**
   * Create a new file type handler for the given Hop GUI instance, perspective and file
   *
   * @param hopGui The Hop GUI
   * @param perspective The perspective
   * @param file The file
   * @return
   */
  T createFileTypeHandler(HopGui hopGui, ExplorerPerspective perspective, ExplorerFile file);
}
