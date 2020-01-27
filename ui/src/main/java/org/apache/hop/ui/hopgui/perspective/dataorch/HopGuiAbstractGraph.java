/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.ui.hopgui.perspective.dataorch;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.GUIPositionInterface;
import org.apache.hop.core.gui.Point;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.hopui.ChangedWarningDialog;
import org.apache.hop.ui.hopui.ChangedWarningInterface;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.ScrollBar;

import java.util.List;

/**
 * The beginnings of a common graph object, used by JobGraph and HopGuiTransGraph to share common behaviors.
 *
 */
public abstract class HopGuiAbstractGraph extends Composite {

  protected CTabItem parentTabItem;

  protected Point offset, iconoffset, noteoffset;

  protected ScrollBar vert, hori;

  protected Canvas canvas;

  protected float magnification = 1.0f;

  private boolean changedState;
  private Font defaultFont;

  public HopGuiAbstractGraph( Composite parent, int style, CTabItem parentTabItem ) {
    super( parent, style );
    this.parentTabItem = parentTabItem;
    defaultFont = parentTabItem.getFont();
    changedState = false;
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
    return (float) ( magnification * PropsUI.getInstance().getZoomFactor() );
  }

  protected Point magnifyPoint( Point p ) {
    float cm = calculateCorrectedMagnification();
    return new Point( Math.round( p.x * cm ), Math.round( p.y * cm ) );
  }

  protected Point getThumb( Point area, Point transMax ) {
    Point resizedMax = magnifyPoint( transMax );

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

  protected <T extends GUIPositionInterface> void doRightClickSelection( T clicked, List<T> selection ) {
    if ( selection.contains( clicked ) ) {
      return;
    }
    if ( !selection.isEmpty() ) {
      for ( GUIPositionInterface selected : selection ) {
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
    if ( isDisposed() || canvas.isDisposed() || parentTabItem.isDisposed() ) {
      return;
    }

    if (hasChanged()!=changedState) {
      changedState = hasChanged();
      if ( hasChanged() ) {
        parentTabItem.setFont( GUIResource.getInstance().getFontBold() );
      } else {
        parentTabItem.setFont( defaultFont );
      }
    }

    canvas.redraw();
  }

  public void zoomIn() {
    magnification += .1f;
    redraw();
  }

  public void zoomOut() {
    magnification -= .1f;
    redraw();
  }

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

  /**
   * Gets the ChangedWarning for the given TabItemInterface class. This should be overridden by a given TabItemInterface
   * class to support the changed warning dialog.
   *
   * @return ChangedWarningInterface The class that provides the dialog and return value
   */
  public ChangedWarningInterface getChangedWarning() {
    return ChangedWarningDialog.getInstance();
  }

  /**
   * Show the ChangedWarning and return the users selection
   *
   * @return int Value of SWT.YES, SWT.NO, SWT.CANCEL
   */
  public int showChangedWarning( String fileName ) throws HopException {
    ChangedWarningInterface changedWarning = getChangedWarning();

    if ( changedWarning != null ) {
      try {
        return changedWarning.show( fileName );
      } catch ( Exception e ) {
        throw new HopException( e );
      }
    }

    return 0;
  }

  public int showChangedWarning() throws HopException {
    return showChangedWarning( null );
  }

  public void dispose() {
    super.dispose();
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

}
