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

import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.widgets.FileDialog;

import java.util.concurrent.atomic.AtomicBoolean;

public class HopGuiFileOpenExtension {
  public AtomicBoolean doIt;
  public FileDialog fileDialog;
  public HopGui hopGui;

  public HopGuiFileOpenExtension( AtomicBoolean doIt, FileDialog fileDialog, HopGui hopGui ) {
    this.doIt = doIt;
    this.fileDialog = fileDialog;
    this.hopGui = hopGui;
  }

  /**
   * Gets doIt
   *
   * @return value of doIt
   */
  public AtomicBoolean getDoIt() {
    return doIt;
  }

  /**
   * @param doIt The doIt to set
   */
  public void setDoIt( AtomicBoolean doIt ) {
    this.doIt = doIt;
  }

  /**
   * Gets fileDialog
   *
   * @return value of fileDialog
   */
  public FileDialog getFileDialog() {
    return fileDialog;
  }

  /**
   * @param fileDialog The fileDialog to set
   */
  public void setFileDialog( FileDialog fileDialog ) {
    this.fileDialog = fileDialog;
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
