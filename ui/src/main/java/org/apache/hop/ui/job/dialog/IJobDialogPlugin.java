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
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Shell;

public interface IJobDialogPlugin {
  void addTab( JobMeta jobMeta, Shell shell, CTabFolder tabFolder );

  void getData( JobMeta jobMeta );

  void ok( JobMeta jobMeta );

  void showLogTableOptions( JobMeta jobMeta, ILogTable logTable, Composite wLogOptionsComposite );
}
