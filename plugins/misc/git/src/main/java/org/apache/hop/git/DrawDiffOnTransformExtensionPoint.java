/*
 * Hop : The Hop Orchestration Platform
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.git;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.gui.BasePainter;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePainter;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;

import static org.apache.hop.git.PdiDiff.ADDED;
import static org.apache.hop.git.PdiDiff.ATTR_GIT;
import static org.apache.hop.git.PdiDiff.ATTR_STATUS;
import static org.apache.hop.git.PdiDiff.CHANGED;
import static org.apache.hop.git.PdiDiff.REMOVED;

@ExtensionPoint(
    id = "DrawDiffOnTransExtensionPoint",
    description = "Draws a marker on top of a step if it has some change",
    extensionPointId = "PipelinePainterEnd" )
public class DrawDiffOnTransformExtensionPoint implements IExtensionPoint {

  @Override
  public void callExtensionPoint( ILogChannel log, Object object ) throws HopException {
    if ( !( object instanceof PipelinePainter ) ) {
      return;
    }
    PipelinePainter painter = (PipelinePainter) object;
    Point offset = painter.getOffset();
    IGc gc = painter.getGc();
    PipelineMeta transMeta = painter.getPipelineMeta();
    transMeta.getTransforms().stream().filter( step -> step.getAttribute( ATTR_GIT, ATTR_STATUS ) != null )
      .forEach( step -> {
        if ( transMeta.getPipelineVersion() == null ? false : transMeta.getPipelineVersion().startsWith( "git" ) ) {
          String status = step.getAttribute( ATTR_GIT, ATTR_STATUS );
          Point n = step.getLocation();
          String location = "images/git/";
          if ( status.equals( REMOVED ) ) {
            location += "removed.svg";
          } else if ( status.equals( CHANGED ) ) {
            location += "changed.svg";
          } else if ( status.equals( ADDED ) ) {
            location += "added.svg";
          } else { // Unchanged
            return;
          }
          int iconsize = ConstUi.ICON_SIZE;
          try {
            iconsize = PropsUi.getInstance().getIconSize();
          } catch ( Exception e ) {
            // Exception when accessed from Hop Server
          }
          gc.drawImage( location, getClass().getClassLoader(), ( n.x + iconsize + offset.x ) - ( BasePainter.MINI_ICON_SIZE / 2 ), n.y + offset.y - ( BasePainter.MINI_ICON_SIZE / 2 ) );
        } else {
          step.getAttributesMap().remove( ATTR_GIT );
        }
      } );
  }
}
