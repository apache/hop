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

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.IGc;

import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePainterExtension;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.PipelineUnitTestSetLocation;
import org.apache.hop.testing.gui.TestingGuiPlugin;
import org.apache.hop.testing.util.DataSetConst;

import java.util.List;

@ExtensionPoint(
  id = "DrawInputDataSetOnTransformExtensionPoint",
  description = "Draws a marker on top of a transform if it has an input data set defined for it",
  extensionPointId = "PipelinePainterTransform"
)
public class DrawInputDataSetOnTransformExtensionPoint implements IExtensionPoint<PipelinePainterExtension> {

  @Override
  public void callExtensionPoint( ILogChannel log, IVariables variables, PipelinePainterExtension ext ) throws HopException {
    TransformMeta transformMeta = ext.transformMeta;
    PipelineMeta pipelineMeta = ext.pipelineMeta;

    PipelineUnitTest unitTest = TestingGuiPlugin.getInstance().getActiveTests().get( pipelineMeta );
    if ( unitTest != null ) {
      drawInputDataSetMarker( ext, transformMeta, unitTest, ext.areaOwners );
    }
  }

  private void drawInputDataSetMarker( PipelinePainterExtension ext, TransformMeta transformMeta, PipelineUnitTest unitTest, List<AreaOwner> areaOwners ) {
    // Now we're here, draw a marker and indicate the name of the data set name
    //
    PipelineUnitTestSetLocation location = unitTest.findInputLocation( transformMeta.getName() );
    if ( location == null ) {
      return;
    }
    String dataSetName = Const.NVL( location.getDataSetName(), "" );

    IGc gc = ext.gc;
    int iconSize = ext.iconSize;
    int x = ext.x1;
    int y = ext.y1;

    gc.setLineWidth( transformMeta.isSelected() ? 2 : 1 );
    gc.setForeground( IGc.EColor.CRYSTAL );
    gc.setBackground( IGc.EColor.LIGHTGRAY );
    gc.setFont( IGc.EFont.GRAPH );
    Point textExtent = gc.textExtent( dataSetName );
    textExtent.x += 6; // add a tiny bit of a margin
    textExtent.y += 6;

    // Draw it to the left as an arrow
    //
    int arrowSize = textExtent.y;
    Point point = new Point( x - textExtent.x - arrowSize - 2, y + ( iconSize - textExtent.y ) / 2 );

    int[] arrow = new int[] {
      point.x, point.y,
      point.x + textExtent.x, point.y,
      point.x + textExtent.x + arrowSize, point.y + textExtent.y / 2,
      point.x + textExtent.x, point.y + textExtent.y,
      point.x, point.y + textExtent.y };

    gc.fillPolygon( arrow );
    gc.drawPolygon( arrow );
    gc.drawText( dataSetName, point.x + 3, point.y + 3 );

    // Leave a trace of what we drew, for memory reasons, just the name of the data set here.
    //
    areaOwners.add( new AreaOwner( AreaOwner.AreaType.CUSTOM, point.x, point.y, textExtent.x, textExtent.y, new Point( 0, 0 ), DataSetConst.AREA_DRAWN_INPUT_DATA_SET, transformMeta.getName() ) );
  }
}
