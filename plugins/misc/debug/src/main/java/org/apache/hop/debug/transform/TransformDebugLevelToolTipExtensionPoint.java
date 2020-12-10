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

package org.apache.hop.debug.transform;

import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.debug.util.BeePainter;
import org.apache.hop.ui.hopgui.file.shared.HopGuiTooltipExtension;

@ExtensionPoint(
  id = "TransformDebugLevelToolTipExtensionPoint",
  description = "Show a tooltip when hovering over the bee",
  extensionPointId = "HopGuiPipelineGraphAreaHover"
)
public class TransformDebugLevelToolTipExtensionPoint extends BeePainter implements IExtensionPoint<HopGuiTooltipExtension> {

  @Override public void callExtensionPoint( ILogChannel log, IVariables variables, HopGuiTooltipExtension ext ) {

    AreaOwner areaOwner = ext.areaOwner;
    try {
      if ( areaOwner.getOwner() instanceof TransformDebugLevel ) {
        TransformDebugLevel debugLevel = (TransformDebugLevel) areaOwner.getOwner();
        ext.tip.append( "Custom transform debug level: " + debugLevel.toString() );
      }
    } catch ( Exception e ) {
      // Ignore error, not that important
      // logChannelInterface.logError( "Unable to handle specific debug level", e );
    }
  }


}
