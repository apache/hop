/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.core.widget.warning;

import org.eclipse.swt.widgets.Control;

public interface IWarning {
  /**
   * Verifies if a warning situation has occurred.
   *
   * @param text    The content of the text box to verify
   * @param widget  the text widget itself
   * @param subject the parent subject that is being edited.
   * @return the warning message
   */
  IWarningMessage getWarningSituation( String text, Control widget, Object subject );
}
