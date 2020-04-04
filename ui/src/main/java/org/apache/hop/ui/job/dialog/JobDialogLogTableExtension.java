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

package org.apache.hop.ui.job.dialog;

import org.apache.hop.core.logging.ILogTable;
import org.apache.hop.job.JobMeta;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Shell;

public class JobDialogLogTableExtension {
  public enum Direction {
    SHOW, RETRIEVE,
  }

  public Direction direction;
  public Shell shell;
  public ILogTable logTable;
  public Composite wLogOptionsComposite;
  public JobMeta jobMeta;
  public ModifyListener lsMod;
  public JobDialog jobDialog;

  public JobDialogLogTableExtension( Direction direction, Shell shell, JobMeta jobMeta,
                                     ILogTable logTable, Composite wLogOptionsComposite, ModifyListener lsMod, JobDialog jobDialog ) {
    super();
    this.direction = direction;
    this.shell = shell;
    this.jobMeta = jobMeta;
    this.logTable = logTable;
    this.wLogOptionsComposite = wLogOptionsComposite;
    this.lsMod = lsMod;
    this.jobDialog = jobDialog;
  }
}
