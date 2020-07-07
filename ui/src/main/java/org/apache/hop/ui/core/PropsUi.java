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

package org.apache.hop.ui.core;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.IGuiPosition;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.history.AuditManager;
import org.apache.hop.history.AuditState;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.RGB;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

/**
 * We use Props to store all kinds of user interactive information such as the selected colors, fonts, positions of
 * windows, etc.
 *
 * @author Matt
 * @since 15-12-2003
 */
public class PropsUi extends Props {

  private static String OS = System.getProperty( "os.name" ).toLowerCase();

  private static final String NO = "N";

  private static final String YES = "Y";

  private static double nativeZoomFactor = 1.0;

  private static final String STRING_SHOW_COPY_OR_DISTRIBUTE_WARNING = "ShowCopyOrDistributeWarning";

  private static final String SHOW_TOOL_TIPS = "ShowToolTips";

  private static final String SHOW_HELP_TOOL_TIPS = "ShowHelpToolTips";

  private static final String CANVAS_GRID_SIZE = "CanvasGridSize";

  private static final String LEGACY_PERSPECTIVE_MODE = "LegacyPerspectiveMode";

  private static final String DISABLE_BROWSER_ENVIRONMENT_CHECK = "DisableBrowserEnvironmentCheck";

  private static final String USE_DOUBLE_CLICK_ON_CANVAS = "UseDoubleClickOnCanvas";

  private static PropsUi instance;

  public static PropsUi getInstance() {
    if ( instance == null ) {
      instance = new PropsUi();
    }
    return instance;
  }

  private PropsUi() {
    super();
    setDefault();
  }

  public void setDefault() {

    super.setDefault();
    Display display = HopGui.getInstance().getDisplay();

    if ( display != null ) {
      FontData fontData = getFixedFont();
      setProperty( STRING_FONT_FIXED_NAME, fontData.getName() );
      setProperty( STRING_FONT_FIXED_SIZE, "" + fontData.getHeight() );
      setProperty( STRING_FONT_FIXED_STYLE, "" + fontData.getStyle() );

      fontData = getDefaultFont();
      setProperty( STRING_FONT_DEFAULT_NAME, fontData.getName() );
      setProperty( STRING_FONT_DEFAULT_SIZE, "" + fontData.getHeight() );
      setProperty( STRING_FONT_DEFAULT_STYLE, "" + fontData.getStyle() );

      fontData = getDefaultFont();
      setProperty( STRING_FONT_GRAPH_NAME, fontData.getName() );
      setProperty( STRING_FONT_GRAPH_SIZE, "" + fontData.getHeight() );
      setProperty( STRING_FONT_GRAPH_STYLE, "" + fontData.getStyle() );

      fontData = getDefaultFont();
      setProperty( STRING_FONT_GRID_NAME, fontData.getName() );
      setProperty( STRING_FONT_GRID_SIZE, "" + fontData.getHeight() );
      setProperty( STRING_FONT_GRID_STYLE, "" + fontData.getStyle() );

      fontData = getDefaultFont();
      setProperty( STRING_FONT_NOTE_NAME, fontData.getName() );
      setProperty( STRING_FONT_NOTE_SIZE, "" + fontData.getHeight() );
      setProperty( STRING_FONT_NOTE_STYLE, "" + fontData.getStyle() );

      RGB color = getBackgroundRGB();
      setProperty( STRING_BACKGROUND_COLOR_R, "" + color.red );
      setProperty( STRING_BACKGROUND_COLOR_G, "" + color.green );
      setProperty( STRING_BACKGROUND_COLOR_B, "" + color.blue );

      color = getGraphColorRGB();
      setProperty( STRING_GRAPH_COLOR_R, "" + color.red );
      setProperty( STRING_GRAPH_COLOR_G, "" + color.green );
      setProperty( STRING_GRAPH_COLOR_B, "" + color.blue );

      setProperty( STRING_ICON_SIZE, "" + getIconSize() );
      setProperty( STRING_LINE_WIDTH, "" + getLineWidth() );
      setProperty( STRING_MAX_UNDO, "" + getMaxUndo() );
    }
  }

  public void setFixedFont( FontData fd ) {
    setProperty( STRING_FONT_FIXED_NAME, fd.getName() );
    setProperty( STRING_FONT_FIXED_SIZE, "" + fd.getHeight() );
    setProperty( STRING_FONT_FIXED_STYLE, "" + fd.getStyle() );
  }

  public FontData getFixedFont() {
    FontData def = getDefaultFontData();

    String name = getProperty( STRING_FONT_FIXED_NAME, "Monospaced" );
    int size = Const.toInt( getProperty( STRING_FONT_FIXED_SIZE ), def.getHeight() );
    int style = Const.toInt( getProperty( STRING_FONT_FIXED_STYLE ), def.getStyle() );

    return new FontData( name, size, style );
  }

  public void setDefaultFont( FontData fd ) {
    if ( fd != null ) {
      setProperty( STRING_FONT_DEFAULT_NAME, fd.getName() );
      setProperty( STRING_FONT_DEFAULT_SIZE, "" + fd.getHeight() );
      setProperty( STRING_FONT_DEFAULT_STYLE, "" + fd.getStyle() );
    }
  }

  public FontData getDefaultFont() {
    FontData def = getDefaultFontData();

    if ( isOSLookShown() ) {
      return def;
    }

    String name = getProperty( STRING_FONT_DEFAULT_NAME, def.getName() );
    int size = Const.toInt( getProperty( STRING_FONT_DEFAULT_SIZE ), def.getHeight() );
    int style = Const.toInt( getProperty( STRING_FONT_DEFAULT_STYLE ), def.getStyle() );

    return new FontData( name, size, style );
  }

  public void setGraphFont( FontData fd ) {
    setProperty( STRING_FONT_GRAPH_NAME, fd.getName() );
    setProperty( STRING_FONT_GRAPH_SIZE, "" + fd.getHeight() );
    setProperty( STRING_FONT_GRAPH_STYLE, "" + fd.getStyle() );
  }

  public FontData getGraphFont() {
    FontData def = getDefaultFontData();

    String name = getProperty( STRING_FONT_GRAPH_NAME, def.getName() );

    int size = Const.toInt( getProperty( STRING_FONT_GRAPH_SIZE ), def.getHeight() );

    // Correct the size with the native zoom factor...
    //
    int correctedSize = (int) Math.round( size / PropsUi.getNativeZoomFactor() );

    int style = Const.toInt( getProperty( STRING_FONT_GRAPH_STYLE ), def.getStyle() );

    return new FontData( name, correctedSize, style );
  }

  public void setGridFont( FontData fd ) {
    setProperty( STRING_FONT_GRID_NAME, fd.getName() );
    setProperty( STRING_FONT_GRID_SIZE, "" + fd.getHeight() );
    setProperty( STRING_FONT_GRID_STYLE, "" + fd.getStyle() );
  }

  public FontData getGridFont() {
    FontData def = getDefaultFontData();

    String name = getProperty( STRING_FONT_GRID_NAME, def.getName() );
    String ssize = getProperty( STRING_FONT_GRID_SIZE );
    String sstyle = getProperty( STRING_FONT_GRID_STYLE );

    int size = Const.toInt( ssize, def.getHeight() );
    int style = Const.toInt( sstyle, def.getStyle() );

    return new FontData( name, size, style );
  }

  public void setNoteFont( FontData fd ) {
    setProperty( STRING_FONT_NOTE_NAME, fd.getName() );
    setProperty( STRING_FONT_NOTE_SIZE, "" + fd.getHeight() );
    setProperty( STRING_FONT_NOTE_STYLE, "" + fd.getStyle() );
  }

  public FontData getNoteFont() {
    FontData def = getDefaultFontData();

    String name = getProperty( STRING_FONT_NOTE_NAME, def.getName() );
    String ssize = getProperty( STRING_FONT_NOTE_SIZE );
    String sstyle = getProperty( STRING_FONT_NOTE_STYLE );

    int size = Const.toInt( ssize, def.getHeight() );
    int style = Const.toInt( sstyle, def.getStyle() );

    return new FontData( name, size, style );
  }

  public void setBackgroundRGB( RGB c ) {
    setProperty( STRING_BACKGROUND_COLOR_R, c != null ? "" + c.red : "" );
    setProperty( STRING_BACKGROUND_COLOR_G, c != null ? "" + c.green : "" );
    setProperty( STRING_BACKGROUND_COLOR_B, c != null ? "" + c.blue : "" );
  }

  public RGB getBackgroundRGB() {
    int r = Const.toInt( getProperty( STRING_BACKGROUND_COLOR_R ), ConstUi.COLOR_BACKGROUND_RED ); // Defaut:
    int g = Const.toInt( getProperty( STRING_BACKGROUND_COLOR_G ), ConstUi.COLOR_BACKGROUND_GREEN );
    int b = Const.toInt( getProperty( STRING_BACKGROUND_COLOR_B ), ConstUi.COLOR_BACKGROUND_BLUE );

    return new RGB( r, g, b );
  }

  public void setGraphColorRGB( RGB c ) {
    setProperty( STRING_GRAPH_COLOR_R, "" + c.red );
    setProperty( STRING_GRAPH_COLOR_G, "" + c.green );
    setProperty( STRING_GRAPH_COLOR_B, "" + c.blue );
  }

  public RGB getGraphColorRGB() {
    int r = Const.toInt( getProperty( STRING_GRAPH_COLOR_R ), ConstUi.COLOR_GRAPH_RED ); // default White
    int g = Const.toInt( getProperty( STRING_GRAPH_COLOR_G ), ConstUi.COLOR_GRAPH_GREEN );
    int b = Const.toInt( getProperty( STRING_GRAPH_COLOR_B ), ConstUi.COLOR_GRAPH_BLUE );

    return new RGB( r, g, b );
  }

  public void setTabColorRGB( RGB c ) {
    setProperty( STRING_TAB_COLOR_R, "" + c.red );
    setProperty( STRING_TAB_COLOR_G, "" + c.green );
    setProperty( STRING_TAB_COLOR_B, "" + c.blue );
  }

  public RGB getTabColorRGB() {
    int r = Const.toInt( getProperty( STRING_TAB_COLOR_R ), ConstUi.COLOR_TAB_RED ); // default White
    int g = Const.toInt( getProperty( STRING_TAB_COLOR_G ), ConstUi.COLOR_TAB_GREEN );
    int b = Const.toInt( getProperty( STRING_TAB_COLOR_B ), ConstUi.COLOR_TAB_BLUE );

    return new RGB( r, g, b );
  }

  public void setIconSize( int size ) {
    setProperty( STRING_ICON_SIZE, "" + size );
  }

  public int getIconSize() {
    return Const.toInt( getProperty( STRING_ICON_SIZE ), ConstUi.ICON_SIZE );
  }

  public void setZoomFactor( double factor ) {
    setProperty( STRING_ZOOM_FACTOR, Double.toString( factor ) );
  }

  public double getZoomFactor() {
    String zoomFactorString = getProperty( STRING_ZOOM_FACTOR );
    if ( StringUtils.isNotEmpty( zoomFactorString ) ) {
      return Const.toDouble( zoomFactorString, nativeZoomFactor );
    } else {
      return nativeZoomFactor;
    }
  }

  /**
   * Get the margin compensated for the zoom factor
   *
   * @return
   */
  public int getMargin() {
    return (int) Math.round( getZoomFactor() * Const.MARGIN );
  }

  public void setLineWidth( int width ) {
    setProperty( STRING_LINE_WIDTH, "" + width );
  }

  public int getLineWidth() {
    return Const.toInt( getProperty( STRING_LINE_WIDTH ), ConstUi.LINE_WIDTH );
  }

  public void setLastPipeline( String pipeline ) {
    setProperty( STRING_LAST_PREVIEW_PIPELINE, pipeline );
  }

  public String getLastPipeline() {
    return getProperty( STRING_LAST_PREVIEW_PIPELINE, "" );
  }

  public void setLastPreview( String[] lastpreview, int[] transformsize ) {
    setProperty( STRING_LAST_PREVIEW_TRANSFORM, "" + lastpreview.length );

    for ( int i = 0; i < lastpreview.length; i++ ) {
      setProperty( STRING_LAST_PREVIEW_TRANSFORM + ( i + 1 ), lastpreview[ i ] );
      setProperty( STRING_LAST_PREVIEW_SIZE + ( i + 1 ), "" + transformsize[ i ] );
    }
  }

  public String[] getLastPreview() {
    String snr = getProperty( STRING_LAST_PREVIEW_TRANSFORM );
    int nr = Const.toInt( snr, 0 );
    String[] lp = new String[ nr ];
    for ( int i = 0; i < nr; i++ ) {
      lp[ i ] = getProperty( STRING_LAST_PREVIEW_TRANSFORM + ( i + 1 ), "" );
    }
    return lp;
  }

  public int[] getLastPreviewSize() {
    String snr = getProperty( STRING_LAST_PREVIEW_TRANSFORM );
    int nr = Const.toInt( snr, 0 );
    int[] si = new int[ nr ];
    for ( int i = 0; i < nr; i++ ) {
      si[ i ] = Const.toInt( getProperty( STRING_LAST_PREVIEW_SIZE + ( i + 1 ), "" ), 0 );
    }
    return si;
  }

  public FontData getDefaultFontData() {
    return HopGui.getInstance().getDisplay().getSystemFont().getFontData()[ 0 ];
  }

  public void setMaxUndo( int max ) {
    setProperty( STRING_MAX_UNDO, "" + max );
  }

  public int getMaxUndo() {
    return Const.toInt( getProperty( STRING_MAX_UNDO ), Const.MAX_UNDO );
  }

  public void setMiddlePct( int pct ) {
    setProperty( STRING_MIDDLE_PCT, "" + pct );
  }

  public int getMiddlePct() {
    return Const.toInt( getProperty( STRING_MIDDLE_PCT ), Const.MIDDLE_PCT );
  }

  public void setScreen( WindowProperty windowProperty ) {
    AuditManager.storeState( LogChannel.UI, HopNamespace.getNamespace(), "shells", windowProperty.getName(), windowProperty.getStateProperties() );
  }

  public WindowProperty getScreen( String windowName ) {
    if ( windowName == null ) {
      return null;
    }
    AuditState auditState = AuditManager.retrieveState(LogChannel.UI, HopNamespace.getNamespace(), "shells", windowName);
    if (auditState==null) {
      return null;
    }
    return new WindowProperty(windowName, auditState.getStateMap());
  }


  public void setOpenLastFile( boolean open ) {
    setProperty( STRING_OPEN_LAST_FILE, open ? YES : NO );
  }

  public boolean openLastFile() {
    String open = getProperty( STRING_OPEN_LAST_FILE );
    return !NO.equalsIgnoreCase( open );
  }

  public void setAutoSave( boolean autosave ) {
    setProperty( STRING_AUTO_SAVE, autosave ? YES : NO );
  }

  public boolean getAutoSave() {
    String autosave = getProperty( STRING_AUTO_SAVE );
    return YES.equalsIgnoreCase( autosave ); // Default = OFF
  }

  public void setSaveConfirmation( boolean saveconf ) {
    setProperty( STRING_SAVE_CONF, saveconf ? YES : NO );
  }

  public boolean getSaveConfirmation() {
    String saveconf = getProperty( STRING_SAVE_CONF );
    return YES.equalsIgnoreCase( saveconf ); // Default = OFF
  }

  public void setAutoSplit( boolean autosplit ) {
    setProperty( STRING_AUTO_SPLIT, autosplit ? YES : NO );
  }

  public boolean getAutoSplit() {
    String autosplit = getProperty( STRING_AUTO_SPLIT );
    return YES.equalsIgnoreCase( autosplit ); // Default = OFF
  }

  public void setAutoCollapseCoreObjectsTree( boolean autoCollapse ) {
    setProperty( STRING_AUTO_COLLAPSE_CORE_TREE, autoCollapse ? YES : NO );
  }

  public boolean getAutoCollapseCoreObjectsTree() {
    String autoCollapse = getProperty( STRING_AUTO_COLLAPSE_CORE_TREE );
    return YES.equalsIgnoreCase( autoCollapse ); // Default = OFF
  }

  public void setExitWarningShown( boolean show ) {
    setProperty( STRING_SHOW_EXIT_WARNING, show ? YES : NO );
  }

  public boolean isShowCanvasGridEnabled() {
    String showCanvas = getProperty( STRING_SHOW_CANVAS_GRID, NO );
    return YES.equalsIgnoreCase( showCanvas ); // Default: don't show canvas grid
  }

  public void setShowCanvasGridEnabled( boolean anti ) {
    setProperty( STRING_SHOW_CANVAS_GRID, anti ? YES : NO );
  }

  public boolean showExitWarning() {
    String show = getProperty( STRING_SHOW_EXIT_WARNING, YES );
    return YES.equalsIgnoreCase( show ); // Default: show repositories dialog at startup
  }

  public boolean isOSLookShown() {
    String show = getProperty( STRING_SHOW_OS_LOOK, NO );
    return YES.equalsIgnoreCase( show ); // Default: don't show gray dialog boxes, show Hop look.
  }

  public void setOSLookShown( boolean show ) {
    setProperty( STRING_SHOW_OS_LOOK, show ? YES : NO );
  }

  public static void setGCFont( GC gc, Device device, FontData fontData ) {
    if ( Const.getSystemOs().startsWith( "Windows" ) ) {
      Font font = new Font( device, fontData );
      gc.setFont( font );
      font.dispose();
    } else {
      gc.setFont( device.getSystemFont() );
    }
  }

  public void setLook( Control widget ) {
    if (widget instanceof Combo ) {
      return; // Just keep the default
    }

    setLook( widget, WIDGET_STYLE_DEFAULT );

    if ( widget instanceof Composite ) {
      for ( Control child : ( (Composite) widget ).getChildren() ) {
        setLook( child );
      }
    }
  }

  public void setLook( final Control control, int style ) {
    if ( this.isOSLookShown() && style != WIDGET_STYLE_FIXED ) {
      return;
    }

    final GuiResource gui = GuiResource.getInstance();
    Font font = null;
    Color background = null;

    switch ( style ) {
      case WIDGET_STYLE_DEFAULT:
        background = gui.getColorBackground();
        if ( control instanceof Group && OS.indexOf( "mac" ) > -1 ) {
          control.addPaintListener( paintEvent -> {
            paintEvent.gc.setBackground( gui.getColorBackground() );
            paintEvent.gc.fillRectangle( 2, 0, control.getBounds().width - 8, control.getBounds().height - 20 );
          } );
        }
        font = null; // GuiResource.getInstance().getFontDefault();
        break;
      case WIDGET_STYLE_FIXED:
        if ( !this.isOSLookShown() ) {
          background = gui.getColorBackground();
        }
        font = gui.getFontFixed();
        break;
      case WIDGET_STYLE_TABLE:
        background = gui.getColorBackground();
        font = null; // gui.getFontGrid();
        break;
      case WIDGET_STYLE_NOTEPAD:
        background = gui.getColorBackground();
        font = gui.getFontNote();
        break;
      case WIDGET_STYLE_GRAPH:
        background = gui.getColorBackground();
        font = gui.getFontGraph();
        break;
      case WIDGET_STYLE_TOOLBAR:
        background = GuiResource.getInstance().getColorDemoGray();
        break;
      case WIDGET_STYLE_TAB:
        background = GuiResource.getInstance().getColorWhite();
        CTabFolder tabFolder = (CTabFolder) control;
        tabFolder.setSimple( false );
        tabFolder.setBorderVisible( true );
        // need to make a copy of the tab selection background color to get around PDI-13940
        Color c = GuiResource.getInstance().getColorTab();
        Color tabColor = new Color( c.getDevice(), c.getRed(), c.getGreen(), c.getBlue() );
        tabFolder.setSelectionBackground( tabColor );
        break;
      default:
        background = gui.getColorBackground();
        font = null; // gui.getFontDefault();
        break;
    }

    if ( font != null && !font.isDisposed() ) {
      control.setFont( font );
    }

    if ( background != null && !background.isDisposed() ) {
      if ( control instanceof Button ) {
        Button b = (Button) control;
        if ( ( b.getStyle() & SWT.PUSH ) != 0 ) {
          return;
        }
      }
      control.setBackground( background );
    }
  }

  public static void setTableItemLook( TableItem item, Display disp ) {
    if ( !Const.getSystemOs().startsWith( "Windows" ) ) {
      return;
    }

    Color background = GuiResource.getInstance().getColorBackground();
    if ( background != null ) {
      item.setBackground( background );
    }
  }

  /**
   * @return Returns the display.
   */
  public static Display getDisplay() {
    return HopGui.getInstance().getDisplay();
  }

  public void setDefaultPreviewSize( int size ) {
    setProperty( STRING_DEFAULT_PREVIEW_SIZE, "" + size );
  }

  public int getDefaultPreviewSize() {
    return Const.toInt( getProperty( STRING_DEFAULT_PREVIEW_SIZE ), 1000 );
  }

  public boolean showCopyOrDistributeWarning() {
    String show = getProperty( STRING_SHOW_COPY_OR_DISTRIBUTE_WARNING, YES );
    return YES.equalsIgnoreCase( show );
  }

  public void setShowCopyOrDistributeWarning( boolean show ) {
    setProperty( STRING_SHOW_COPY_OR_DISTRIBUTE_WARNING, show ? YES : NO );
  }

  public void setDialogSize( Shell shell, String styleProperty ) {
    String prop = getProperty( styleProperty );
    if ( Utils.isEmpty( prop ) ) {
      return;
    }

    String[] xy = prop.split( "," );
    if ( xy.length != 2 ) {
      return;
    }

    shell.setSize( Integer.parseInt( xy[ 0 ] ), Integer.parseInt( xy[ 1 ] ) );
  }

  public boolean useDoubleClick() {
    return YES.equalsIgnoreCase( getProperty( USE_DOUBLE_CLICK_ON_CANVAS, YES) );
  }

  public void setUseDoubleClickOnCanvas( boolean use ) {
    setProperty( USE_DOUBLE_CLICK_ON_CANVAS, use ? YES : NO );
  }


  public boolean showToolTips() {
    return YES.equalsIgnoreCase( getProperty( SHOW_TOOL_TIPS, YES ) );
  }

  public void setShowToolTips( boolean show ) {
    setProperty( SHOW_TOOL_TIPS, show ? YES : NO );
  }

  public boolean isShowingHelpToolTips() {
    return YES.equalsIgnoreCase( getProperty( SHOW_HELP_TOOL_TIPS, YES ) );
  }

  public void setShowingHelpToolTips( boolean show ) {
    setProperty( SHOW_HELP_TOOL_TIPS, show ? YES : NO );
  }

  public int getCanvasGridSize() {
    return Const.toInt( getProperty( CANVAS_GRID_SIZE, "16" ), 16 );
  }

  public void setCanvasGridSize( int gridSize ) {
    setProperty( CANVAS_GRID_SIZE, Integer.toString( gridSize ) );
  }

  /**
   * Gets the supported version of the requested software.
   *
   * @param property the key for the software version
   * @return an integer that represents the supported version for the software.
   */
  public int getSupportedVersion( String property ) {
    return Integer.parseInt( getProperty( property ) );
  }

  /**
   * Ask if the browsing environment checks are disabled.
   *
   * @return 'true' if disabled 'false' otherwise.
   */
  public boolean isBrowserEnvironmentCheckDisabled() {
    return "Y".equalsIgnoreCase( getProperty( DISABLE_BROWSER_ENVIRONMENT_CHECK, "N" ) );
  }

  public boolean isLegacyPerspectiveMode() {
    return "Y".equalsIgnoreCase( getProperty( LEGACY_PERSPECTIVE_MODE, "N" ) );
  }

  public static void setLocation( IGuiPosition guiElement, int x, int y ) {
    if ( x < 0 ) {
      x = 0;
    }
    if ( y < 0 ) {
      y = 0;
    }
    guiElement.setLocation( calculateGridPosition( new Point( x, y ) ) );
  }

  public static Point calculateGridPosition( Point p ) {
    int gridSize = PropsUi.getInstance().getCanvasGridSize();
    if ( gridSize > 1 ) {
      // Snap to grid...
      //
      return new Point( gridSize * Math.round( p.x / gridSize ), gridSize * Math.round( p.y / gridSize ) );
    } else {
      // Normal draw
      //
      return p;
    }
  }

  public boolean isIndicateSlowPipelineTransformsEnabled() {
    String indicate = getProperty( STRING_INDICATE_SLOW_PIPELINE_TRANSFORMS, "Y" );
    return YES.equalsIgnoreCase( indicate );
  }

  public void setIndicateSlowPipelineTransformsEnabled( boolean indicate ) {
    setProperty( STRING_INDICATE_SLOW_PIPELINE_TRANSFORMS, indicate ? YES : NO );
  }

  /**
   * Gets nativeZoomFactor
   *
   * @return value of nativeZoomFactor
   */
  public static double getNativeZoomFactor() {
    return nativeZoomFactor;
  }

  /**
   * @param nativeZoomFactor The nativeZoomFactor to set
   */
  public static void setNativeZoomFactor( double nativeZoomFactor ) {
    PropsUi.nativeZoomFactor = nativeZoomFactor;
  }


}
