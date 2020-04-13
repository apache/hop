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

package org.apache.hop.ui.hopgui.perspective.dataorch;

import org.apache.hop.core.gui.IGuiPosition;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.GuiKeyboardShortcut;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.ScrollBar;
import org.eclipse.swt.widgets.Shell;

import java.util.List;
import java.util.UUID;

/**
 * The beginnings of a common graph object, used by JobGraph and HopGuiPipelineGraph to share common behaviors.
 */
public abstract class HopGuiAbstractGraph extends Composite {

  protected HopGui hopUi;

  protected Composite parentComposite;

  protected CTabItem parentTabItem;

  protected Point offset, iconoffset, noteoffset;

  protected ScrollBar vert, hori;

  protected Canvas canvas;

  protected float magnification = 1.0f;

  private boolean changedState;
  private Font defaultFont;

  protected final String id;

  public HopGuiAbstractGraph( HopGui hopUi, Composite parent, int style, CTabItem parentTabItem ) {
    super( parent, style );
    this.parentComposite = parent;

    this.hopUi = hopUi;
    this.parentTabItem = parentTabItem;
    defaultFont = parentTabItem.getFont();
    changedState = false;
    this.id = UUID.randomUUID().toString();
  }

  protected Shell hopShell() {
    return hopUi.getShell();
  }

  protected Display hopDisplay() {
    return hopUi.getDisplay();
  }

  protected abstract Point getOffset();

  protected Point getOffset( Point thumb, Point area ) {
    Point p = new Point( 0, 0 );
    Point sel = new Point( hori.getSelection(), vert.getSelection() );

    if ( thumb.x == 0 || thumb.y == 0 ) {
      return p;
    }
    float cm = calculateCorrectedMagnification();
    p.x = Math.round( -sel.x * area.x / thumb.x / cm );
    p.y = Math.round( -sel.y * area.y / thumb.y / cm );

    return p;
  }

  protected float calculateCorrectedMagnification() {
    return (float) ( magnification * PropsUi.getInstance().getZoomFactor() );
  }

  protected Point magnifyPoint( Point p ) {
    float cm = calculateCorrectedMagnification();
    return new Point( Math.round( p.x * cm ), Math.round( p.y * cm ) );
  }

  protected Point getThumb( Point area, Point pipelineMax ) {
    Point resizedMax = magnifyPoint( pipelineMax );

    Point thumb = new Point( 0, 0 );
    if ( resizedMax.x <= area.x ) {
      thumb.x = 100;
    } else {
      thumb.x = 100 * area.x / resizedMax.x;
    }

    if ( resizedMax.y <= area.y ) {
      thumb.y = 100;
    } else {
      thumb.y = 100 * area.y / resizedMax.y;
    }

    return thumb;
  }

  public int sign( int n ) {
    return n < 0 ? -1 : ( n > 0 ? 1 : 1 );
  }

  protected Point getArea() {
    org.eclipse.swt.graphics.Rectangle rect = canvas.getClientArea();
    Point area = new Point( rect.width, rect.height );

    return area;
  }

  protected <T extends IGuiPosition> void doRightClickSelection( T clicked, List<T> selection ) {
    if ( selection.contains( clicked ) ) {
      return;
    }
    if ( !selection.isEmpty() ) {
      for ( IGuiPosition selected : selection ) {
        selected.setSelected( false );
      }
      selection.clear();
    }
    clicked.setSelected( true );
    selection.add( clicked );
    redraw();
  }

  public abstract boolean hasChanged();

  public void redraw() {
    if ( isDisposed() || canvas == null || canvas.isDisposed() || parentTabItem.isDisposed() ) {
      return;
    }

    if ( hasChanged() != changedState ) {
      changedState = hasChanged();
      if ( hasChanged() ) {
        parentTabItem.setFont( GuiResource.getInstance().getFontBold() );
      } else {
        parentTabItem.setFont( defaultFont );
      }
    }
    canvas.redraw();
  }


  @GuiKeyboardShortcut( control = true, key = '=' )
  public void zoomIn() {
    magnification += .1f;
    redraw();
  }

  @GuiKeyboardShortcut( control = true, key = '-' )
  public void zoomOut() {
    if ( magnification > 0.15f ) {
      magnification -= .1f;
    }
    redraw();
  }

  @GuiKeyboardShortcut( control = true, key = '0' )
  public void zoom100Percent() {
    magnification = 1.0f;
    redraw();
  }

  public Point screen2real( int x, int y ) {
    offset = getOffset();
    float correctedMagnification = calculateCorrectedMagnification();
    Point real;
    if ( offset != null ) {
      real =
        new Point( Math.round( ( x / correctedMagnification - offset.x ) ), Math.round( ( y / correctedMagnification - offset.y ) ) );
    } else {
      real = new Point( x, y );
    }

    return real;
  }

  public Point real2screen( int x, int y ) {
    offset = getOffset();
    Point screen = new Point( x + offset.x, y + offset.y );

    return screen;
  }

  public boolean forceFocus() {
    return canvas.forceFocus();
  }

  public void dispose() {
    parentTabItem.dispose();
  }

  /**
   * Gets parentTabItem
   *
   * @return value of parentTabItem
   */
  public CTabItem getParentTabItem() {
    return parentTabItem;
  }

  /**
   * @param parentTabItem The parentTabItem to set
   */
  public void setParentTabItem( CTabItem parentTabItem ) {
    this.parentTabItem = parentTabItem;
  }

  /**
   * Gets parentComposite
   *
   * @return value of parentComposite
   */
  public Composite getParentComposite() {
    return parentComposite;
  }

  /**
   * @param parentComposite The parentComposite to set
   */
  public void setParentComposite( Composite parentComposite ) {
    this.parentComposite = parentComposite;
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public String getId() {
    return id;
  }
}
