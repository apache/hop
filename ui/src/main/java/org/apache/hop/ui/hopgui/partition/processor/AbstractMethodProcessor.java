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
 */

package org.apache.hop.ui.hopgui.partition.processor;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.hopgui.partition.PartitionSettings;
import org.eclipse.swt.widgets.Shell;

/**
 * @author Evgeniy_Lyakhov@epam.com
 */
public abstract class AbstractMethodProcessor implements IMethodProcessor {

  public String askForSchema( String[] schemaNames, Shell shell, int defaultSelectedSchemaIndex ) {
    EnterSelectionDialog askSchema =
      new EnterSelectionDialog(
        shell, schemaNames, "Select a partition schema", "Select the partition schema to use:" );
    return askSchema.open( defaultSelectedSchemaIndex );

  }

  public void processForKnownSchema( String schemaName, PartitionSettings settings ) throws HopPluginException {
    if ( schemaName != null ) {
      int idx = Const.indexOfString( schemaName, settings.getSchemaNames() );
      settings.updateSchema( settings.getSchemas().get( idx ) );
    } else {
      settings.rollback( settings.getBefore() );
    }
  }


}
