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

package org.apache.hop.ui.hopgui.partition.processor;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.ui.hopgui.file.pipeline.IPartitionSchemaSelection;
import org.apache.hop.ui.hopgui.partition.PartitionSettings;
import org.eclipse.swt.widgets.Shell;

/**
 * @author Evgeniy_Lyakhov@epam.com
 */
public interface IMethodProcessor {
  void schemaSelection( PartitionSettings settings, Shell shell, IPartitionSchemaSelection schemaSelection ) throws HopException;
}
