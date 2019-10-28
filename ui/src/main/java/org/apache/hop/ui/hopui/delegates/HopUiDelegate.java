/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.hopui.delegates;

import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.ui.hopui.HopUi;

public abstract class HopUiDelegate {
  public static final LoggingObjectInterface loggingObject = new SimpleLoggingObject(
    "Spoon (delegate)", LoggingObjectType.SPOON, null );

  protected HopUi hopUi;
  protected LogChannelInterface log;

  protected HopUiDelegate( HopUi hopUi ) {
    this.hopUi = hopUi;
    this.log = hopUi.getLog();
  }

  protected static int getMaxTabLength() {
    return Const.toInt( System.getProperty( Const.HOP_MAX_TAB_LENGTH ), 17 );
  }
}
