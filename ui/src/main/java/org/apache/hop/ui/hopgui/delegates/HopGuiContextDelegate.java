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
import org.apache.hop.core.gui.plugin.GuiActionType;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.GuiContextUtil;

public class HopGuiContextDelegate {
  private HopGui hopUi;

  public HopGuiContextDelegate( HopGui hopGui ) {
    this.hopUi = hopGui;
  }

  /**
   * Create a new file, ask which type of file or object you want created
   */
  public void fileNew() {
    // Click : shell location + 50, 50
    //
    int x = 50 + hopUi.getShell().getLocation().x;
    int y = 50 + hopUi.getShell().getLocation().y;

    GuiContextUtil.handleActionSelection( hopUi.getShell(), "Select the action...", new Point( x, y ), hopUi, GuiActionType.Create );
  }

  /**
   * Edit a metastore object...
   */
  public void fileMetaStoreEdit() {

    GuiContextUtil.handleActionSelection( hopUi.getShell(), "Select the element type to edit...", new Point( 0, 0 ), hopUi, GuiActionType.Modify );
  }

  /**
   * Delete a metastore object...
   */
  public void fileMetaStoreDelete() {

    GuiContextUtil.handleActionSelection( hopUi.getShell(), "Select the element type to delete...", new Point( 0, 0 ), hopUi, GuiActionType.Delete );
  }

  /**
   * Gets hopGui
   *
   * @return value of hopGui
   */
  public HopGui getHopUi() {
    return hopUi;
  }

  /**
   * @param hopUi The hopGui to set
   */
  public void setHopUi( HopGui hopUi ) {
    this.hopUi = hopUi;
  }

}
