/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.ui.hopgui.delegates;

import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.GuiContextUtil;

public class HopGuiContextDelegate {
  private HopGui hopGui;

  public HopGuiContextDelegate( HopGui hopGui ) {
    this.hopGui = hopGui;
  }

  /**
   * Create a new file, ask which type of file or object you want created
   */
  public void fileNew() {
    // Click : shell location + 50, 50
    //
    int x = 50 + hopGui.getShell().getLocation().x;
    int y = 50 + hopGui.getShell().getLocation().y;

    GuiContextUtil.handleActionSelection( hopGui.getShell(), "Select the action...", new Point( x, y ), hopGui, GuiActionType.Create );
  }

  /**
   * Edit a metastore object...
   */
  public void fileMetaStoreEdit() {

    GuiContextUtil.handleActionSelection( hopGui.getShell(), "Select the element type to edit...", new Point( 0, 0 ), hopGui, GuiActionType.Modify );
  }

  /**
   * Delete a metastore object...
   */
  public void fileMetaStoreDelete() {

    GuiContextUtil.handleActionSelection( hopGui.getShell(), "Select the element type to delete...", new Point( 0, 0 ), hopGui, GuiActionType.Delete );
  }

  /**
   * Gets hopGui
   *
   * @return value of hopGui
   */
  public HopGui getHopGui() {
    return hopGui;
  }

  /**
   * @param hopGui The hopGui to set
   */
  public void setHopGui( HopGui hopGui ) {
    this.hopGui = hopGui;
  }

}
