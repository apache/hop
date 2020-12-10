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

package org.apache.hop.testing.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelinePainterExtension;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.PipelineUnitTestTweak;
import org.apache.hop.testing.gui.TestingGuiPlugin;

@ExtensionPoint(
  id = "DrawTweakOnTransformExtensionPoint",
  description = "Draws a marker on top of a transform if is tweaked",
  extensionPointId = "PipelinePainterTransform"
)
public class DrawTweakOnTransformExtensionPoint implements IExtensionPoint<PipelinePainterExtension> {

  @Override
  public void callExtensionPoint( ILogChannel log, IVariables variables, PipelinePainterExtension ext ) throws HopException {
    TransformMeta transformMeta = ext.transformMeta;
    PipelineUnitTest unitTest = TestingGuiPlugin.getCurrentUnitTest( ext.pipelineMeta );
    if ( unitTest == null ) {
      return;
    }
    PipelineUnitTestTweak tweak = unitTest.findTweak( transformMeta.getName() );
    if ( tweak == null || tweak.getTweak() == null ) {
      return;
    }

    try {
      switch ( tweak.getTweak() ) {
        case NONE:
          break;
        case REMOVE_TRANSFORM:
          drawRemovedTweak( ext, transformMeta );
          break;
        case BYPASS_TRANSFORM:
          drawBypassedTweak( ext, transformMeta );
          break;
        default:
          break;
      }
    } catch ( Exception e ) {
      // Ignore
    }
  }

  private void drawRemovedTweak( PipelinePainterExtension ext, TransformMeta transformMeta ) {
    // Now we're here, mark the transform as removed: a cross over the transform icon
    //
    IGc gc = ext.gc;
    int iconSize = ext.iconSize;
    int x = ext.x1 - 5;
    int y = ext.y1 - 5;

    gc.setLineWidth( transformMeta.isSelected() ? 4 : 3 );
    gc.setForeground( IGc.EColor.CRYSTAL );
    gc.setBackground( IGc.EColor.LIGHTGRAY );
    gc.setFont( IGc.EFont.GRAPH );

    gc.drawLine( x, y, x + iconSize / 2, y + iconSize / 2 );
    gc.drawLine( x + iconSize / 2, y, x, y + iconSize / 2 );
  }

  protected void drawBypassedTweak( PipelinePainterExtension ext, TransformMeta transformMeta ) {
    // put an arrow over the transform to indicate bypass
    //
    IGc gc = ext.gc;
    int iconSize = ext.iconSize;
    int x = ext.x1 - 5;
    int y = ext.y1 - 5;

    int aW = iconSize / 2;
    int aH = 3 * iconSize / 8;

    gc.setForeground( IGc.EColor.CRYSTAL );
    gc.setBackground( IGc.EColor.CRYSTAL );

    //                 C\
    //                 | \
    //    A------------B  \
    //    |                D
    //    G------------F  /
    //                 | /
    //                 E/  
    //
    int[] arrow = new int[] {
      x, y + aH / 3, // A
      x + 5 * aW / 8, y + aH / 3, // B
      x + 5 * aW / 8, y,  // C
      x + aW, y + aH / 2, // D
      x + 5 * aW / 8, y + aH,  // E
      x + 5 * aW / 8, y + 2 * aH / 3, // F
      x, y + 2 * aH / 3, // G
    };
    gc.fillPolygon( arrow );
  }
}
