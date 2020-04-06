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

package org.apache.hop.ui.core.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.IGUIOption;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.i18n.GlobalMessages;
import org.apache.hop.i18n.LanguageChoice;
import org.apache.hop.ui.core.ConstUI;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.database.dialog.DatabaseDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.PaintListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.RGB;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.ColorDialog;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FontDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * Allows you to set the configurable options for the Hop environment
 *
 * @author Matt
 * @since 15-12-2003
 */
public class EnterOptionsDialog extends Dialog {
  private static Class<?> PKG = DatabaseDialog.class; // for i18n purposes, needed by Translator!!

  private Display display;

  private CTabFolder wTabFolder;

  private FontData fixedFontData, graphFontData, noteFontData;

  private Font fixedFont, graphFont, noteFont;

  private RGB backgroundRGB, graphColorRGB, tabColorRGB;

  private Color background, graphColor, tabColor;

  private Canvas wFFont;

  private Canvas wGFont;

  private Canvas wNFont;

  private Canvas wBGColor;

  private Canvas wGrColor;

  private Canvas wTabColor;

  private Text wIconsize;

  private Text wLineWidth;

  private Text wShadowSize;

  private Text wDefaultPreview;

  private Text wMaxNrLogLines;

  private Text wMaxLogLineTimeout;

  private Text wMaxNrHistLines;

  private Text wMiddlePct;

  private Text wGridSize;

  private Button wOriginalLook;

  private Button wShowCanvasGrid;

  private Button wUseCache;

  private Button wOpenLast;

  private Button wAutoSave;

  private Button wAutoSplit;

  private Button wCopyDistrib;

  private Button wExitWarning;

  private Combo wDefaultLocale;

  private Shell shell;

  private PropsUI props;

  private int middle;

  private int margin;

  private Button tooltipBtn;

  private Button helptipBtn;

  private Button closeAllFilesBtn;

  private Button autoCollapseBtn;

  //private Button wIndicateSlowTransforms;

  /**
   * @deprecated Use CT without <i>props</i> parameter instead
   */
  @Deprecated
  public EnterOptionsDialog( Shell parent, PropsUI props ) {
    super( parent, SWT.NONE );
    this.props = props;
  }

  public EnterOptionsDialog( Shell parent ) {
    super( parent, SWT.NONE );
    props = PropsUI.getInstance();
  }

  public Props open() {
    Shell parent = getParent();
    display = parent.getDisplay();

    getData();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.SHEET | SWT.RESIZE );
    props.setLook( shell );
    shell.setImage( GUIResource.getInstance().getImageLogoSmall() );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.Title" ) );

    middle = props.getMiddlePct();
    margin = props.getMargin();

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );

    addGeneralTab();
    addLookTab();

    // Some buttons
    Button wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    Button wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin, null );

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( 0, 0 );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOK, -margin );
    wTabFolder.setLayoutData( fdTabFolder );

    // ///////////////////////////////////////////////////////////
    // / END OF TABS
    // ///////////////////////////////////////////////////////////

    // Add listeners
    Listener lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    Listener lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wCancel.addListener( SWT.Selection, lsCancel );

    SelectionAdapter lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wIconsize.addSelectionListener( lsDef );
    wLineWidth.addSelectionListener( lsDef );
    wShadowSize.addSelectionListener( lsDef );
    wMiddlePct.addSelectionListener( lsDef );
    wDefaultPreview.addSelectionListener( lsDef );
    wMaxNrLogLines.addSelectionListener( lsDef );
    wMaxLogLineTimeout.addSelectionListener( lsDef );
    wMaxNrHistLines.addSelectionListener( lsDef );
    wGridSize.addSelectionListener( lsDef );

    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    wTabFolder.setSelection( 0 );

    BaseTransformDialog.setSize( shell );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return props;
  }

  private void addLookTab() {
    int h = 40;

    // ////////////////////////
    // START OF LOOK TAB///
    // /
    CTabItem wLookTab = new CTabItem( wTabFolder, SWT.NONE );
    wLookTab.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.LookAndFeel.Label" ) );

    ScrolledComposite sLookComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    sLookComp.setLayout( new FillLayout() );

    Composite wLookComp = new Composite( sLookComp, SWT.NONE );
    props.setLook( wLookComp );

    FormLayout lookLayout = new FormLayout();
    lookLayout.marginWidth = 3;
    lookLayout.marginHeight = 3;
    wLookComp.setLayout( lookLayout );

    // Fixed font
    int nr = 0;
    Label wlFFont = new Label( wLookComp, SWT.RIGHT );
    wlFFont.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.FixedWidthFont.Label" ) );
    props.setLook( wlFFont );
    FormData fdlFFont = new FormData();
    fdlFFont.left = new FormAttachment( 0, 0 );
    fdlFFont.right = new FormAttachment( middle, -margin );
    fdlFFont.top = new FormAttachment( 0, nr * h + margin + 10 );
    wlFFont.setLayoutData( fdlFFont );

    Button wdFFont = new Button( wLookComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wdFFont );
    FormData fddFFont = layoutResetOptionButton( wdFFont );
    fddFFont.right = new FormAttachment( 100, 0 );
    fddFFont.top = new FormAttachment( 0, nr * h + margin );
    fddFFont.bottom = new FormAttachment( 0, ( nr + 1 ) * h + margin );

    wdFFont.setLayoutData( fddFFont );
    wdFFont.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        fixedFontData = new FontData( PropsUI.getInstance().getFixedFont().getName(),
          PropsUI.getInstance().getFixedFont().getHeight(), PropsUI.getInstance().getFixedFont().getStyle() );
        fixedFont.dispose();
        fixedFont = new Font( display, fixedFontData );
        wFFont.redraw();
      }
    } );

    Button wbFFont = new Button( wLookComp, SWT.PUSH );
    props.setLook( wbFFont );

    FormData fdbFFont = layoutEditOptionButton( wbFFont );
    fdbFFont.right = new FormAttachment( wdFFont, -margin );
    fdbFFont.top = new FormAttachment( 0, nr * h + margin );
    fdbFFont.bottom = new FormAttachment( 0, ( nr + 1 ) * h + margin );
    wbFFont.setLayoutData( fdbFFont );
    wbFFont.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        FontDialog fd = new FontDialog( shell );
        fd.setFontList( new FontData[] { fixedFontData } );
        FontData newfd = fd.open();
        if ( newfd != null ) {
          fixedFontData = newfd;
          fixedFont.dispose();
          fixedFont = new Font( display, fixedFontData );
          wFFont.redraw();
        }
      }
    } );

    wFFont = new Canvas( wLookComp, SWT.BORDER );
    props.setLook( wFFont );
    FormData fdFFont = new FormData();
    fdFFont.left = new FormAttachment( middle, 0 );
    fdFFont.right = new FormAttachment( wbFFont, -margin );
    fdFFont.top = new FormAttachment( 0, margin );
    fdFFont.bottom = new FormAttachment( 0, h );
    wFFont.setLayoutData( fdFFont );
    wFFont.addPaintListener( new PaintListener() {
      public void paintControl( PaintEvent pe ) {
        pe.gc.setFont( fixedFont );
        Rectangle max = wFFont.getBounds();
        String name = fixedFontData.getName() + " - " + fixedFontData.getHeight();
        Point size = pe.gc.textExtent( name );

        pe.gc.drawText( name, ( max.width - size.x ) / 2, ( max.height - size.y ) / 2, true );
      }
    } );

    // Graph font
    nr++;
    Label wlGFont = new Label( wLookComp, SWT.RIGHT );
    wlGFont.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.GraphFont.Label" ) );
    props.setLook( wlGFont );
    FormData fdlGFont = new FormData();
    fdlGFont.left = new FormAttachment( 0, 0 );
    fdlGFont.right = new FormAttachment( middle, -margin );
    fdlGFont.top = new FormAttachment( 0, nr * h + margin + 10 );
    wlGFont.setLayoutData( fdlGFont );

    Button wdGFont = new Button( wLookComp, SWT.PUSH );
    props.setLook( wdGFont );

    FormData fddGFont = layoutResetOptionButton( wdGFont );
    fddGFont.right = new FormAttachment( 100, 0 );
    fddGFont.top = new FormAttachment( 0, nr * h + margin );
    fddGFont.bottom = new FormAttachment( 0, ( nr + 1 ) * h + margin );
    wdGFont.setLayoutData( fddGFont );
    wdGFont.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        graphFont.dispose();

        graphFontData = props.getDefaultFontData();
        graphFont = new Font( display, graphFontData );
        wGFont.redraw();
      }
    } );

    Button wbGFont = new Button( wLookComp, SWT.PUSH );
    props.setLook( wbGFont );

    FormData fdbGFont = layoutEditOptionButton( wbGFont );
    fdbGFont.right = new FormAttachment( wdGFont, -margin );
    fdbGFont.top = new FormAttachment( 0, nr * h + margin );
    fdbGFont.bottom = new FormAttachment( 0, ( nr + 1 ) * h + margin );
    wbGFont.setLayoutData( fdbGFont );
    wbGFont.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        FontDialog fd = new FontDialog( shell );
        fd.setFontList( new FontData[] { graphFontData } );
        FontData newfd = fd.open();
        if ( newfd != null ) {
          graphFontData = newfd;
          graphFont.dispose();
          graphFont = new Font( display, graphFontData );
          wGFont.redraw();
        }
      }
    } );

    wGFont = new Canvas( wLookComp, SWT.BORDER );
    props.setLook( wGFont );
    FormData fdGFont = new FormData();
    fdGFont.left = new FormAttachment( middle, 0 );
    fdGFont.right = new FormAttachment( wbGFont, -margin );
    fdGFont.top = new FormAttachment( 0, nr * h + margin );
    fdGFont.bottom = new FormAttachment( 0, ( nr + 1 ) * h + margin );
    wGFont.setLayoutData( fdGFont );
    wGFont.addPaintListener( pe -> {
      pe.gc.setFont( graphFont );
      Rectangle max = wGFont.getBounds();
      String name = graphFontData.getName() + " - " + graphFontData.getHeight();
      Point size = pe.gc.textExtent( name );

      pe.gc.drawText( name, ( max.width - size.x ) / 2, ( max.height - size.y ) / 2, true );
    } );

    // Note font
    nr++;
    Label wlNFont = new Label( wLookComp, SWT.RIGHT );
    wlNFont.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.NoteFont.Label" ) );
    props.setLook( wlNFont );
    FormData fdlNFont = new FormData();
    fdlNFont.left = new FormAttachment( 0, 0 );
    fdlNFont.right = new FormAttachment( middle, -margin );
    fdlNFont.top = new FormAttachment( 0, nr * h + margin + 10 );
    wlNFont.setLayoutData( fdlNFont );

    Button wdNFont = new Button( wLookComp, SWT.PUSH );
    props.setLook( wdNFont );

    FormData fddNFont = layoutResetOptionButton( wdNFont );
    fddNFont.right = new FormAttachment( 100, 0 );
    fddNFont.top = new FormAttachment( 0, nr * h + margin );
    fddNFont.bottom = new FormAttachment( 0, ( nr + 1 ) * h + margin );
    wdNFont.setLayoutData( fddNFont );
    wdNFont.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        noteFontData = props.getDefaultFontData();
        noteFont.dispose();
        noteFont = new Font( display, noteFontData );
        wNFont.redraw();
      }
    } );

    Button wbNFont = new Button( wLookComp, SWT.PUSH );
    props.setLook( wbNFont );

    FormData fdbNFont = layoutEditOptionButton( wbNFont );
    fdbNFont.right = new FormAttachment( wdNFont, -margin );
    fdbNFont.top = new FormAttachment( 0, nr * h + margin );
    fdbNFont.bottom = new FormAttachment( 0, ( nr + 1 ) * h + margin );
    wbNFont.setLayoutData( fdbNFont );
    wbNFont.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        FontDialog fd = new FontDialog( shell );
        fd.setFontList( new FontData[] { noteFontData } );
        FontData newfd = fd.open();
        if ( newfd != null ) {
          noteFontData = newfd;
          noteFont.dispose();
          noteFont = new Font( display, noteFontData );
          wNFont.redraw();
        }
      }
    } );

    wNFont = new Canvas( wLookComp, SWT.BORDER );
    props.setLook( wNFont );
    FormData fdNFont = new FormData();
    fdNFont.left = new FormAttachment( middle, 0 );
    fdNFont.right = new FormAttachment( wbNFont, -margin );
    fdNFont.top = new FormAttachment( 0, nr * h + margin );
    fdNFont.bottom = new FormAttachment( 0, ( nr + 1 ) * h + margin );
    wNFont.setLayoutData( fdNFont );
    wNFont.addPaintListener( new PaintListener() {
      public void paintControl( PaintEvent pe ) {
        pe.gc.setFont( noteFont );
        Rectangle max = wNFont.getBounds();
        String name = noteFontData.getName() + " - " + noteFontData.getHeight();
        Point size = pe.gc.textExtent( name );

        pe.gc.drawText( name, ( max.width - size.x ) / 2, ( max.height - size.y ) / 2, true );
      }
    } );

    // Background color
    nr++;
    Label wlBGColor = new Label( wLookComp, SWT.RIGHT );
    wlBGColor.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.BackgroundColor.Label" ) );
    props.setLook( wlBGColor );
    FormData fdlBGColor = new FormData();
    fdlBGColor.left = new FormAttachment( 0, 0 );
    fdlBGColor.right = new FormAttachment( middle, -margin );
    fdlBGColor.top = new FormAttachment( 0, nr * h + margin + 10 );
    wlBGColor.setLayoutData( fdlBGColor );

    Button wdBGcolor = new Button( wLookComp, SWT.PUSH );
    props.setLook( wdBGcolor );

    FormData fddBGColor = layoutResetOptionButton( wdBGcolor );
    fddBGColor.right = new FormAttachment( 100, 0 ); // to the right of the
    // dialog
    fddBGColor.top = new FormAttachment( 0, nr * h + margin );
    fddBGColor.bottom = new FormAttachment( 0, ( nr + 1 ) * h + margin );
    wdBGcolor.setLayoutData( fddBGColor );
    wdBGcolor.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        background.dispose();

        backgroundRGB =
          new RGB( ConstUI.COLOR_BACKGROUND_RED, ConstUI.COLOR_BACKGROUND_GREEN, ConstUI.COLOR_BACKGROUND_BLUE );
        background = new Color( display, backgroundRGB );
        wBGColor.setBackground( background );
        wBGColor.redraw();
      }
    } );

    Button wbBGColor = new Button( wLookComp, SWT.PUSH );
    props.setLook( wbBGColor );

    FormData fdbBGColor = layoutEditOptionButton( wbBGColor );
    fdbBGColor.right = new FormAttachment( wdBGcolor, -margin ); // to the
    // left of
    // the
    // "default"
    // button
    fdbBGColor.top = new FormAttachment( 0, nr * h + margin );
    fdbBGColor.bottom = new FormAttachment( 0, ( nr + 1 ) * h + margin );
    wbBGColor.setLayoutData( fdbBGColor );
    wbBGColor.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        ColorDialog cd = new ColorDialog( shell );
        cd.setRGB( props.getBackgroundRGB() );
        RGB newbg = cd.open();
        if ( newbg != null ) {
          backgroundRGB = newbg;
          background.dispose();
          background = new Color( display, backgroundRGB );
          wBGColor.setBackground( background );
          wBGColor.redraw();
        }
      }
    } );

    wBGColor = new Canvas( wLookComp, SWT.BORDER );
    props.setLook( wBGColor );
    wBGColor.setBackground( background );
    FormData fdBGColor = new FormData();
    fdBGColor.left = new FormAttachment( middle, 0 );
    fdBGColor.right = new FormAttachment( wbBGColor, -margin );
    fdBGColor.top = new FormAttachment( 0, nr * h + margin );
    fdBGColor.bottom = new FormAttachment( 0, ( nr + 1 ) * h + margin );
    wBGColor.setLayoutData( fdBGColor );

    // Graph background color
    nr++;
    Label wlGrColor = new Label( wLookComp, SWT.RIGHT );
    wlGrColor.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.BackgroundColorGraph.Label" ) );
    props.setLook( wlGrColor );
    FormData fdlGrColor = new FormData();
    fdlGrColor.left = new FormAttachment( 0, 0 );
    fdlGrColor.right = new FormAttachment( middle, -margin );
    fdlGrColor.top = new FormAttachment( 0, nr * h + margin + 10 );
    wlGrColor.setLayoutData( fdlGrColor );

    Button wdGrColor = new Button( wLookComp, SWT.PUSH );
    props.setLook( wdGrColor );

    FormData fddGrColor = layoutResetOptionButton( wdGrColor );
    fddGrColor.right = new FormAttachment( 100, 0 );
    fddGrColor.top = new FormAttachment( 0, nr * h + margin );
    fddGrColor.bottom = new FormAttachment( 0, ( nr + 1 ) * h + margin );
    wdGrColor.setLayoutData( fddGrColor );
    wdGrColor.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        graphColor.dispose();

        graphColorRGB = new RGB( ConstUI.COLOR_GRAPH_RED, ConstUI.COLOR_GRAPH_GREEN, ConstUI.COLOR_GRAPH_BLUE );
        graphColor = new Color( display, graphColorRGB );
        wGrColor.setBackground( graphColor );
        wGrColor.redraw();
      }
    } );

    Button wbGrColor = new Button( wLookComp, SWT.PUSH );
    props.setLook( wbGrColor );

    FormData fdbGrColor = layoutEditOptionButton( wbGrColor );
    fdbGrColor.right = new FormAttachment( wdGrColor, -margin );
    fdbGrColor.top = new FormAttachment( 0, nr * h + margin );
    fdbGrColor.bottom = new FormAttachment( 0, ( nr + 1 ) * h + margin );
    wbGrColor.setLayoutData( fdbGrColor );
    wbGrColor.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        ColorDialog cd = new ColorDialog( shell );
        cd.setRGB( props.getGraphColorRGB() );
        RGB newbg = cd.open();
        if ( newbg != null ) {
          graphColorRGB = newbg;
          graphColor.dispose();
          graphColor = new Color( display, graphColorRGB );
          wGrColor.setBackground( graphColor );
          wGrColor.redraw();
        }
      }
    } );

    wGrColor = new Canvas( wLookComp, SWT.BORDER );
    props.setLook( wGrColor );
    wGrColor.setBackground( graphColor );
    FormData fdGrColor = new FormData();
    fdGrColor.left = new FormAttachment( middle, 0 );
    fdGrColor.right = new FormAttachment( wbGrColor, -margin );
    fdGrColor.top = new FormAttachment( 0, nr * h + margin );
    fdGrColor.bottom = new FormAttachment( 0, ( nr + 1 ) * h + margin );
    wGrColor.setLayoutData( fdGrColor );

    // Tab selected color
    nr++;
    Label wlTabColor = new Label( wLookComp, SWT.RIGHT );
    wlTabColor.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.TabColor.Label" ) );
    props.setLook( wlTabColor );
    FormData fdlTabColor = new FormData();
    fdlTabColor.left = new FormAttachment( 0, 0 );
    fdlTabColor.right = new FormAttachment( middle, -margin );
    fdlTabColor.top = new FormAttachment( 0, nr * h + margin + 10 );
    wlTabColor.setLayoutData( fdlTabColor );

    Button wdTabColor = new Button( wLookComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wdTabColor );

    FormData fddTabColor = layoutResetOptionButton( wdTabColor );
    fddTabColor.right = new FormAttachment( 100, 0 );
    fddTabColor.top = new FormAttachment( 0, nr * h + margin );
    fddTabColor.bottom = new FormAttachment( 0, ( nr + 1 ) * h + margin );
    wdTabColor.setLayoutData( fddTabColor );
    wdTabColor.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        tabColor.dispose();

        tabColorRGB = new RGB( ConstUI.COLOR_TAB_RED, ConstUI.COLOR_TAB_GREEN, ConstUI.COLOR_TAB_BLUE );
        tabColor = new Color( display, tabColorRGB );
        wTabColor.setBackground( tabColor );
        wTabColor.redraw();
      }
    } );

    Button wbTabColor = new Button( wLookComp, SWT.PUSH );
    props.setLook( wbTabColor );

    FormData fdbTabColor = layoutEditOptionButton( wbTabColor );
    fdbTabColor.right = new FormAttachment( wdTabColor, -margin );
    fdbTabColor.top = new FormAttachment( 0, nr * h + margin );
    fdbTabColor.bottom = new FormAttachment( 0, ( nr + 1 ) * h + margin );
    wbTabColor.setLayoutData( fdbTabColor );
    wbTabColor.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        ColorDialog cd = new ColorDialog( shell );
        cd.setRGB( props.getTabColorRGB() );
        RGB newbg = cd.open();
        if ( newbg != null ) {
          tabColorRGB = newbg;
          tabColor.dispose();
          tabColor = new Color( display, tabColorRGB );
          wTabColor.setBackground( tabColor );
          wTabColor.redraw();
        }
      }
    } );

    wTabColor = new Canvas( wLookComp, SWT.BORDER );
    props.setLook( wTabColor );
    wTabColor.setBackground( tabColor );
    FormData fdTabColor = new FormData();
    fdTabColor.left = new FormAttachment( middle, 0 );
    fdTabColor.right = new FormAttachment( wbTabColor, -margin );
    fdTabColor.top = new FormAttachment( 0, nr * h + margin );
    fdTabColor.bottom = new FormAttachment( 0, ( nr + 1 ) * h + margin );
    wTabColor.setLayoutData( fdTabColor );

    // Iconsize line
    Label wlIconsize = new Label( wLookComp, SWT.RIGHT );
    wlIconsize.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.IconSize.Label" ) );
    props.setLook( wlIconsize );
    FormData fdlIconsize = new FormData();
    fdlIconsize.left = new FormAttachment( 0, 0 );
    fdlIconsize.right = new FormAttachment( middle, -margin );
    fdlIconsize.top = new FormAttachment( wTabColor, margin );
    wlIconsize.setLayoutData( fdlIconsize );
    wIconsize = new Text( wLookComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wIconsize.setText( Integer.toString( props.getIconSize() ) );
    props.setLook( wIconsize );
    FormData fdIconsize = new FormData();
    fdIconsize.left = new FormAttachment( middle, 0 );
    fdIconsize.right = new FormAttachment( 100, -margin );
    fdIconsize.top = new FormAttachment( wTabColor, margin );
    wIconsize.setLayoutData( fdIconsize );

    // LineWidth line
    Label wlLineWidth = new Label( wLookComp, SWT.RIGHT );
    wlLineWidth.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.LineWidth.Label" ) );
    props.setLook( wlLineWidth );
    FormData fdlLineWidth = new FormData();
    fdlLineWidth.left = new FormAttachment( 0, 0 );
    fdlLineWidth.right = new FormAttachment( middle, -margin );
    fdlLineWidth.top = new FormAttachment( wIconsize, margin );
    wlLineWidth.setLayoutData( fdlLineWidth );
    wLineWidth = new Text( wLookComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wLineWidth.setText( Integer.toString( props.getLineWidth() ) );
    props.setLook( wLineWidth );
    FormData fdLineWidth = new FormData();
    fdLineWidth.left = new FormAttachment( middle, 0 );
    fdLineWidth.right = new FormAttachment( 100, -margin );
    fdLineWidth.top = new FormAttachment( wIconsize, margin );
    wLineWidth.setLayoutData( fdLineWidth );

    // ShadowSize line
    Label wlShadowSize = new Label( wLookComp, SWT.RIGHT );
    wlShadowSize.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.ShadowSize.Label" ) );
    props.setLook( wlShadowSize );
    FormData fdlShadowSize = new FormData();
    fdlShadowSize.left = new FormAttachment( 0, 0 );
    fdlShadowSize.right = new FormAttachment( middle, -margin );
    fdlShadowSize.top = new FormAttachment( wLineWidth, margin );
    wlShadowSize.setLayoutData( fdlShadowSize );
    wShadowSize = new Text( wLookComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wShadowSize.setText( Integer.toString( props.getShadowSize() ) );
    props.setLook( wShadowSize );
    FormData fdShadowSize = new FormData();
    fdShadowSize.left = new FormAttachment( middle, 0 );
    fdShadowSize.right = new FormAttachment( 100, -margin );
    fdShadowSize.top = new FormAttachment( wLineWidth, margin );
    wShadowSize.setLayoutData( fdShadowSize );

    // MiddlePct line
    Label wlMiddlePct = new Label( wLookComp, SWT.RIGHT );
    wlMiddlePct.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.DialogMiddlePercentage.Label" ) );
    props.setLook( wlMiddlePct );
    FormData fdlMiddlePct = new FormData();
    fdlMiddlePct.left = new FormAttachment( 0, 0 );
    fdlMiddlePct.right = new FormAttachment( middle, -margin );
    fdlMiddlePct.top = new FormAttachment( wShadowSize, margin );
    wlMiddlePct.setLayoutData( fdlMiddlePct );
    wMiddlePct = new Text( wLookComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wMiddlePct.setText( Integer.toString( props.getMiddlePct() ) );
    props.setLook( wMiddlePct );
    FormData fdMiddlePct = new FormData();
    fdMiddlePct.left = new FormAttachment( middle, 0 );
    fdMiddlePct.right = new FormAttachment( 100, -margin );
    fdMiddlePct.top = new FormAttachment( wShadowSize, margin );
    wMiddlePct.setLayoutData( fdMiddlePct );

    // GridSize line
    Label wlGridSize = new Label( wLookComp, SWT.RIGHT );
    wlGridSize.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.GridSize.Label" ) );
    wlGridSize.setToolTipText( BaseMessages.getString( PKG, "EnterOptionsDialog.GridSize.ToolTip" ) );
    props.setLook( wlGridSize );
    FormData fdlGridSize = new FormData();
    fdlGridSize.left = new FormAttachment( 0, 0 );
    fdlGridSize.right = new FormAttachment( middle, -margin );
    fdlGridSize.top = new FormAttachment( wMiddlePct, margin );
    wlGridSize.setLayoutData( fdlGridSize );
    wGridSize = new Text( wLookComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wGridSize.setText( Integer.toString( props.getCanvasGridSize() ) );
    wGridSize.setToolTipText( BaseMessages.getString( PKG, "EnterOptionsDialog.GridSize.ToolTip" ) );
    props.setLook( wGridSize );
    FormData fdGridSize = new FormData();
    fdGridSize.left = new FormAttachment( middle, 0 );
    fdGridSize.right = new FormAttachment( 100, -margin );
    fdGridSize.top = new FormAttachment( wMiddlePct, margin );
    wGridSize.setLayoutData( fdGridSize );

    // Show Canvas Grid
    Label wlShowCanvasGrid = new Label( wLookComp, SWT.RIGHT );
    wlShowCanvasGrid.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.ShowCanvasGrid.Label" ) );
    wlShowCanvasGrid.setToolTipText( BaseMessages.getString( PKG, "EnterOptionsDialog.ShowCanvasGrid.ToolTip" ) );
    props.setLook( wlShowCanvasGrid );
    FormData fdlShowCanvasGrid = new FormData();
    fdlShowCanvasGrid.left = new FormAttachment( 0, 0 );
    fdlShowCanvasGrid.right = new FormAttachment( middle, -margin );
    fdlShowCanvasGrid.top = new FormAttachment( wGridSize, margin );
    wlShowCanvasGrid.setLayoutData( fdlShowCanvasGrid );
    wShowCanvasGrid = new Button( wLookComp, SWT.CHECK );
    props.setLook( wShowCanvasGrid );
    wShowCanvasGrid.setSelection( props.isShowCanvasGridEnabled() );
    FormData fdShowCanvasGrid = new FormData();
    fdShowCanvasGrid.left = new FormAttachment( middle, 0 );
    fdShowCanvasGrid.right = new FormAttachment( 100, -margin );
    fdShowCanvasGrid.top = new FormAttachment( wlShowCanvasGrid, 0, SWT.CENTER );
    wShowCanvasGrid.setLayoutData( fdShowCanvasGrid );


    // Show original look
    Label wlOriginalLook = new Label( wLookComp, SWT.RIGHT );
    wlOriginalLook.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.UseOSLook.Label" ) );
    props.setLook( wlOriginalLook );
    FormData fdlOriginalLook = new FormData();
    fdlOriginalLook.left = new FormAttachment( 0, 0 );
    fdlOriginalLook.top = new FormAttachment( wShowCanvasGrid, margin );
    fdlOriginalLook.right = new FormAttachment( middle, -margin );
    wlOriginalLook.setLayoutData( fdlOriginalLook );
    wOriginalLook = new Button( wLookComp, SWT.CHECK );
    props.setLook( wOriginalLook );
    wOriginalLook.setSelection( props.isOSLookShown() );
    FormData fdOriginalLook = new FormData();
    fdOriginalLook.left = new FormAttachment( middle, 0 );
    fdOriginalLook.top = new FormAttachment( wlOriginalLook, 0, SWT.CENTER );
    fdOriginalLook.right = new FormAttachment( 100, 0 );
    wOriginalLook.setLayoutData( fdOriginalLook );

    // DefaultLocale line
    Label wlDefaultLocale = new Label( wLookComp, SWT.RIGHT );
    wlDefaultLocale.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.DefaultLocale.Label" ) );
    props.setLook( wlDefaultLocale );
    FormData fdlDefaultLocale = new FormData();
    fdlDefaultLocale.left = new FormAttachment( 0, 0 );
    fdlDefaultLocale.right = new FormAttachment( middle, -margin );
    fdlDefaultLocale.top = new FormAttachment( wOriginalLook, margin );
    wlDefaultLocale.setLayoutData( fdlDefaultLocale );
    wDefaultLocale = new Combo( wLookComp, SWT.SINGLE | SWT.READ_ONLY | SWT.LEFT | SWT.BORDER );
    wDefaultLocale.setItems( GlobalMessages.localeDescr );
    props.setLook( wDefaultLocale );
    FormData fdDefaultLocale = new FormData();
    fdDefaultLocale.left = new FormAttachment( middle, 0 );
    fdDefaultLocale.right = new FormAttachment( 100, -margin );
    fdDefaultLocale.top = new FormAttachment( wlDefaultLocale, 0, SWT.CENTER );
    wDefaultLocale.setLayoutData( fdDefaultLocale );
    // language selections...
    int idxDefault =
      Const.indexOfString(
        LanguageChoice.getInstance().getDefaultLocale().toString(), GlobalMessages.localeCodes );
    if ( idxDefault >= 0 ) {
      wDefaultLocale.select( idxDefault );
    }

    FormData fdLookComp = new FormData();
    fdLookComp.left = new FormAttachment( 0, 0 );
    fdLookComp.right = new FormAttachment( 100, 0 );
    fdLookComp.top = new FormAttachment( 0, 0 );
    fdLookComp.bottom = new FormAttachment( 100, 100 );
    wLookComp.setLayoutData( fdLookComp );

    wLookComp.pack();

    Rectangle bounds = wLookComp.getBounds();
    sLookComp.setContent( wLookComp );
    sLookComp.setExpandHorizontal( true );
    sLookComp.setExpandVertical( true );
    sLookComp.setMinWidth( bounds.width );
    sLookComp.setMinHeight( bounds.height );

    wLookTab.setControl( sLookComp );

    // ///////////////////////////////////////////////////////////
    // / END OF LOOK TAB
    // ///////////////////////////////////////////////////////////
  }

  private void addGeneralTab() {
    // ////////////////////////
    // START OF GENERAL TAB///
    // /
    CTabItem wGeneralTab = new CTabItem( wTabFolder, SWT.NONE );
    wGeneralTab.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.General.Label" ) );

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;

    ScrolledComposite sGeneralComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    sGeneralComp.setLayout( new FillLayout() );

    Composite wGeneralComp = new Composite( sGeneralComp, SWT.NONE );
    props.setLook( wGeneralComp );
    wGeneralComp.setLayout( generalLayout );

    // Default preview size
    Label wlDefaultPreview = new Label( wGeneralComp, SWT.RIGHT );
    wlDefaultPreview.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.DefaultPreviewSize.Label" ) );
    props.setLook( wlDefaultPreview );
    FormData fdlDefaultPreview = new FormData();
    fdlDefaultPreview.left = new FormAttachment( 0, 0 );
    fdlDefaultPreview.right = new FormAttachment( middle, -margin );
    fdlDefaultPreview.top = new FormAttachment( 0, margin );
    wlDefaultPreview.setLayoutData( fdlDefaultPreview );
    wDefaultPreview = new Text( wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wDefaultPreview.setText( Integer.toString( props.getDefaultPreviewSize() ) );
    props.setLook( wDefaultPreview );
    FormData fdDefaultPreview = new FormData();
    fdDefaultPreview.left = new FormAttachment( middle, 0 );
    fdDefaultPreview.right = new FormAttachment( 100, -margin );
    fdDefaultPreview.top = new FormAttachment( 0, margin );
    wDefaultPreview.setLayoutData( fdDefaultPreview );

    // Max Nr of log lines
    Label wlMaxNrLogLines = new Label( wGeneralComp, SWT.RIGHT );
    wlMaxNrLogLines.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.MaxNrLogLinesSize.Label" ) );
    props.setLook( wlMaxNrLogLines );
    FormData fdlMaxNrLogLines = new FormData();
    fdlMaxNrLogLines.left = new FormAttachment( 0, 0 );
    fdlMaxNrLogLines.right = new FormAttachment( middle, -margin );
    fdlMaxNrLogLines.top = new FormAttachment( wDefaultPreview, margin );
    wlMaxNrLogLines.setLayoutData( fdlMaxNrLogLines );
    wMaxNrLogLines = new Text( wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wMaxNrLogLines.setText( Integer.toString( props.getMaxNrLinesInLog() ) );
    props.setLook( wMaxNrLogLines );
    FormData fdMaxNrLogLines = new FormData();
    fdMaxNrLogLines.left = new FormAttachment( middle, 0 );
    fdMaxNrLogLines.right = new FormAttachment( 100, -margin );
    fdMaxNrLogLines.top = new FormAttachment( wlMaxNrLogLines, 0, SWT.CENTER );
    wMaxNrLogLines.setLayoutData( fdMaxNrLogLines );

    // Max log line timeout (minutes)
    Label wlMaxLogLineTimeout = new Label( wGeneralComp, SWT.RIGHT );
    wlMaxLogLineTimeout.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.MaxLogLineTimeout.Label" ) );
    props.setLook( wlMaxLogLineTimeout );
    FormData fdlMaxLogLineTimeout = new FormData();
    fdlMaxLogLineTimeout.left = new FormAttachment( 0, 0 );
    fdlMaxLogLineTimeout.right = new FormAttachment( middle, -margin );
    fdlMaxLogLineTimeout.top = new FormAttachment( wMaxNrLogLines, margin );
    wlMaxLogLineTimeout.setLayoutData( fdlMaxLogLineTimeout );
    wMaxLogLineTimeout = new Text( wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wMaxLogLineTimeout.setText( Integer.toString( props.getMaxLogLineTimeoutMinutes() ) );
    props.setLook( wMaxLogLineTimeout );
    FormData fdMaxLogLineTimeout = new FormData();
    fdMaxLogLineTimeout.left = new FormAttachment( middle, 0 );
    fdMaxLogLineTimeout.right = new FormAttachment( 100, -margin );
    fdMaxLogLineTimeout.top = new FormAttachment( wlMaxLogLineTimeout, 0, SWT.CENTER );
    wMaxLogLineTimeout.setLayoutData( fdMaxLogLineTimeout );

    // Max Nr of history lines
    Label wlMaxNrHistLines = new Label( wGeneralComp, SWT.RIGHT );
    wlMaxNrHistLines.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.MaxNrHistLinesSize.Label" ) );
    props.setLook( wlMaxNrHistLines );
    FormData fdlMaxNrHistLines = new FormData();
    fdlMaxNrHistLines.left = new FormAttachment( 0, 0 );
    fdlMaxNrHistLines.right = new FormAttachment( middle, -margin );
    fdlMaxNrHistLines.top = new FormAttachment( wMaxLogLineTimeout, margin );
    wlMaxNrHistLines.setLayoutData( fdlMaxNrHistLines );
    wMaxNrHistLines = new Text( wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wMaxNrHistLines.setText( Integer.toString( props.getMaxNrLinesInHistory() ) );
    props.setLook( wMaxNrHistLines );
    FormData fdMaxNrHistLines = new FormData();
    fdMaxNrHistLines.left = new FormAttachment( middle, 0 );
    fdMaxNrHistLines.right = new FormAttachment( 100, -margin );
    fdMaxNrHistLines.top = new FormAttachment( wlMaxNrHistLines, 0, SWT.CENTER );
    wMaxNrHistLines.setLayoutData( fdMaxNrHistLines );

    // Use DB Cache?
    Label wlUseCache = new Label( wGeneralComp, SWT.RIGHT );
    wlUseCache.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.UseDatabaseCache.Label" ) );
    props.setLook( wlUseCache );
    FormData fdlUseCache = new FormData();
    fdlUseCache.left = new FormAttachment( 0, 0 );
    fdlUseCache.top = new FormAttachment( wMaxNrHistLines, margin );
    fdlUseCache.right = new FormAttachment( middle, -margin );
    wlUseCache.setLayoutData( fdlUseCache );
    wUseCache = new Button( wGeneralComp, SWT.CHECK );
    props.setLook( wUseCache );
    wUseCache.setSelection( props.useDBCache() );
    FormData fdUseCache = new FormData();
    fdUseCache.left = new FormAttachment( middle, 0 );
    fdUseCache.top = new FormAttachment( wlUseCache, 0, SWT.CENTER );
    fdUseCache.right = new FormAttachment( 100, 0 );
    wUseCache.setLayoutData( fdUseCache );

    // Auto load last file at startup?
    Label wlOpenLast = new Label( wGeneralComp, SWT.RIGHT );
    wlOpenLast.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.OpenLastFileStartup.Label" ) );
    props.setLook( wlOpenLast );
    FormData fdlOpenLast = new FormData();
    fdlOpenLast.left = new FormAttachment( 0, 0 );
    fdlOpenLast.top = new FormAttachment( wUseCache, margin );
    fdlOpenLast.right = new FormAttachment( middle, -margin );
    wlOpenLast.setLayoutData( fdlOpenLast );
    wOpenLast = new Button( wGeneralComp, SWT.CHECK );
    props.setLook( wOpenLast );
    wOpenLast.setSelection( props.openLastFile() );
    FormData fdOpenLast = new FormData();
    fdOpenLast.left = new FormAttachment( middle, 0 );
    fdOpenLast.top = new FormAttachment( wlOpenLast, 0, SWT.CENTER );
    fdOpenLast.right = new FormAttachment( 100, 0 );
    wOpenLast.setLayoutData( fdOpenLast );

    // Auto save changed files?
    Label wlAutoSave = new Label( wGeneralComp, SWT.RIGHT );
    wlAutoSave.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.AutoSave.Label" ) );
    props.setLook( wlAutoSave );
    FormData fdlAutoSave = new FormData();
    fdlAutoSave.left = new FormAttachment( 0, 0 );
    fdlAutoSave.top = new FormAttachment( wOpenLast, margin );
    fdlAutoSave.right = new FormAttachment( middle, -margin );
    wlAutoSave.setLayoutData( fdlAutoSave );
    wAutoSave = new Button( wGeneralComp, SWT.CHECK );
    props.setLook( wAutoSave );
    wAutoSave.setSelection( props.getAutoSave() );
    FormData fdAutoSave = new FormData();
    fdAutoSave.left = new FormAttachment( middle, 0 );
    fdAutoSave.top = new FormAttachment( wlAutoSave, 0, SWT.CENTER);
    fdAutoSave.right = new FormAttachment( 100, 0 );
    wAutoSave.setLayoutData( fdAutoSave );

    // Automatically split hops?
    Label wlAutoSplit = new Label( wGeneralComp, SWT.RIGHT );
    wlAutoSplit.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.AutoSplitHops.Label" ) );
    props.setLook( wlAutoSplit );
    FormData fdlAutoSplit = new FormData();
    fdlAutoSplit.left = new FormAttachment( 0, 0 );
    fdlAutoSplit.top = new FormAttachment( wAutoSave, margin );
    fdlAutoSplit.right = new FormAttachment( middle, -margin );
    wlAutoSplit.setLayoutData( fdlAutoSplit );
    wAutoSplit = new Button( wGeneralComp, SWT.CHECK );
    props.setLook( wAutoSplit );
    wAutoSplit.setToolTipText( BaseMessages.getString( PKG, "EnterOptionsDialog.AutoSplitHops.Tooltip" ) );
    wAutoSplit.setSelection( props.getAutoSplit() );
    FormData fdAutoSplit = new FormData();
    fdAutoSplit.left = new FormAttachment( middle, 0 );
    fdAutoSplit.top = new FormAttachment( wlAutoSplit, 0, SWT.CENTER);
    fdAutoSplit.right = new FormAttachment( 100, 0 );
    wAutoSplit.setLayoutData( fdAutoSplit );

    // Show warning for copy / distribute...
    Label wlCopyDistrib = new Label( wGeneralComp, SWT.RIGHT );
    wlCopyDistrib.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.CopyOrDistributeDialog.Label" ) );
    props.setLook( wlCopyDistrib );
    FormData fdlCopyDistrib = new FormData();
    fdlCopyDistrib.left = new FormAttachment( 0, 0 );
    fdlCopyDistrib.top = new FormAttachment( wAutoSplit, margin );
    fdlCopyDistrib.right = new FormAttachment( middle, -margin );
    wlCopyDistrib.setLayoutData( fdlCopyDistrib );
    wCopyDistrib = new Button( wGeneralComp, SWT.CHECK );
    props.setLook( wCopyDistrib );
    wCopyDistrib
      .setToolTipText( BaseMessages.getString( PKG, "EnterOptionsDialog.CopyOrDistributeDialog.Tooltip" ) );
    wCopyDistrib.setSelection( props.showCopyOrDistributeWarning() );
    FormData fdCopyDistrib = new FormData();
    fdCopyDistrib.left = new FormAttachment( middle, 0 );
    fdCopyDistrib.top = new FormAttachment( wlCopyDistrib, 0, SWT.CENTER );
    fdCopyDistrib.right = new FormAttachment( 100, 0 );
    wCopyDistrib.setLayoutData( fdCopyDistrib );

    // Show exit warning?
    Label wlExitWarning = new Label( wGeneralComp, SWT.RIGHT );
    wlExitWarning.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.AskOnExit.Label" ) );
    props.setLook( wlExitWarning );
    FormData fdlExitWarning = new FormData();
    fdlExitWarning.left = new FormAttachment( 0, 0 );
    fdlExitWarning.top = new FormAttachment( wCopyDistrib, margin );
    fdlExitWarning.right = new FormAttachment( middle, -margin );
    wlExitWarning.setLayoutData( fdlExitWarning );
    wExitWarning = new Button( wGeneralComp, SWT.CHECK );
    props.setLook( wExitWarning );
    wExitWarning.setSelection( props.showExitWarning() );
    FormData fdExitWarning = new FormData();
    fdExitWarning.left = new FormAttachment( middle, 0 );
    fdExitWarning.top = new FormAttachment( wlExitWarning, 0, SWT.CENTER );
    fdExitWarning.right = new FormAttachment( 100, 0 );
    wExitWarning.setLayoutData( fdExitWarning );

    // Clear custom parameters. (from transform)
    Label wlClearCustom = new Label( wGeneralComp, SWT.RIGHT );
    wlClearCustom.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.ClearCustomParameters.Label" ) );
    props.setLook( wlClearCustom );
    FormData fdlClearCustom = new FormData();
    fdlClearCustom.left = new FormAttachment( 0, 0 );
    fdlClearCustom.top = new FormAttachment( wExitWarning, margin + 10 );
    fdlClearCustom.right = new FormAttachment( middle, -margin );
    wlClearCustom.setLayoutData( fdlClearCustom );

    Button wClearCustom = new Button( wGeneralComp, SWT.PUSH );
    props.setLook( wClearCustom );
    FormData fdClearCustom = layoutResetOptionButton( wClearCustom );
    fdClearCustom.width = fdClearCustom.width + 6;
    fdClearCustom.height = fdClearCustom.height + 18;
    fdClearCustom.left = new FormAttachment( middle, 0 );
    fdClearCustom.top = new FormAttachment( wExitWarning, margin );
    wClearCustom.setLayoutData( fdClearCustom );
    wClearCustom.setToolTipText( BaseMessages.getString( PKG, "EnterOptionsDialog.ClearCustomParameters.Tooltip" ) );
    wClearCustom.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        MessageBox mb = new MessageBox( shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION );
        mb.setMessage( BaseMessages.getString( PKG, "EnterOptionsDialog.ClearCustomParameters.Question" ) );
        mb.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.ClearCustomParameters.Title" ) );
        int id = mb.open();
        if ( id == SWT.YES ) {
          props.clearCustomParameters();
          props.saveProps();
          MessageBox ok = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
          ok.setMessage( BaseMessages.getString( PKG, "EnterOptionsDialog.ClearCustomParameters.Confirmation" ) );
          ok.open();
        }
      }
    } );

    // Auto-collapse core objects tree branches?
    Label autoCollapseLbl = new Label( wGeneralComp, SWT.RIGHT );
    autoCollapseLbl.setText( BaseMessages.getString(
      PKG, "EnterOptionsDialog.EnableAutoCollapseCoreObjectTree.Label" ) );
    props.setLook( autoCollapseLbl );
    FormData fdautoCollapse = new FormData();
    fdautoCollapse.left = new FormAttachment( 0, 0 );
    fdautoCollapse.top = new FormAttachment( wClearCustom, 2*margin  );
    fdautoCollapse.right = new FormAttachment( middle, -margin );
    autoCollapseLbl.setLayoutData( fdautoCollapse );
    autoCollapseBtn = new Button( wGeneralComp, SWT.CHECK );
    props.setLook( autoCollapseBtn );
    autoCollapseBtn.setSelection( props.getAutoCollapseCoreObjectsTree() );
    FormData helpautoCollapse = new FormData();
    helpautoCollapse.left = new FormAttachment( middle, 0 );
    helpautoCollapse.top = new FormAttachment( autoCollapseLbl, 0, SWT.CENTER );
    helpautoCollapse.right = new FormAttachment( 100, 0 );
    autoCollapseBtn.setLayoutData( helpautoCollapse );

    // Tooltips
    Label tooltipLbl = new Label( wGeneralComp, SWT.RIGHT );
    tooltipLbl.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.ToolTipsEnabled.Label" ) );
    props.setLook( tooltipLbl );
    FormData fdlToolTipData = new FormData();
    fdlToolTipData.left = new FormAttachment( 0, 0 );
    fdlToolTipData.top = new FormAttachment( autoCollapseBtn, margin );
    fdlToolTipData.right = new FormAttachment( middle, -margin );
    tooltipLbl.setLayoutData( fdlToolTipData );
    tooltipBtn = new Button( wGeneralComp, SWT.CHECK );
    props.setLook( tooltipBtn );
    tooltipBtn.setSelection( props.showToolTips() );
    FormData toolTipBtnData = new FormData();
    toolTipBtnData.left = new FormAttachment( middle, 0 );
    toolTipBtnData.top = new FormAttachment( tooltipLbl, 0, SWT.CENTER );
    toolTipBtnData.right = new FormAttachment( 100, 0 );
    tooltipBtn.setLayoutData( toolTipBtnData );

    // Help tool tips
    Label helptipLbl = new Label( wGeneralComp, SWT.RIGHT );
    helptipLbl.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.HelpToolTipsEnabled.Label" ) );
    props.setLook( helptipLbl );
    FormData fdlHelpTipData = new FormData();
    fdlHelpTipData.left = new FormAttachment( 0, 0 );
    fdlHelpTipData.top = new FormAttachment( tooltipLbl, margin );
    fdlHelpTipData.right = new FormAttachment( middle, -margin );
    helptipLbl.setLayoutData( fdlHelpTipData );
    helptipBtn = new Button( wGeneralComp, SWT.CHECK );
    props.setLook( helptipBtn );
    helptipBtn.setSelection( props.isShowingHelpToolTips() );
    FormData helpTipBtnData = new FormData();
    helpTipBtnData.left = new FormAttachment( middle, 0 );
    helpTipBtnData.top = new FormAttachment( helptipLbl, 0, SWT.CENTER );
    helpTipBtnData.right = new FormAttachment( 100, 0 );
    helptipBtn.setLayoutData( helpTipBtnData );

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.top = new FormAttachment( 0, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 100 );
    wGeneralComp.setLayoutData( fdGeneralComp );

    wGeneralComp.pack();

    Rectangle bounds = wGeneralComp.getBounds();

    sGeneralComp.setContent( wGeneralComp );
    sGeneralComp.setExpandHorizontal( true );
    sGeneralComp.setExpandVertical( true );
    sGeneralComp.setMinWidth( bounds.width );
    sGeneralComp.setMinHeight( bounds.height );

    wGeneralTab.setControl( sGeneralComp );

    // editables
    Label refLabel = new Label( wGeneralComp, SWT.RIGHT );
    refLabel = tooltipLbl;
    Button lastbtn = closeAllFilesBtn;
    for ( final IGUIOption<Object> e : PropsUI.getInstance().getRegisteredEditableComponents() ) {
      if ( e.getLabelText() == null ) {
        continue;
      }
      Label wlMaxNrLogLines1 = new Label( wGeneralComp, SWT.RIGHT );
      wlMaxNrLogLines1.setText( e.getLabelText() );
      props.setLook( wlMaxNrLogLines1 );
      FormData fdlMaxNrLogLinesTemp = new FormData();
      fdlMaxNrLogLinesTemp.left = new FormAttachment( 0, 0 );
      fdlMaxNrLogLinesTemp.right = new FormAttachment( middle, -margin );
      fdlMaxNrLogLinesTemp.top = new FormAttachment( refLabel, margin );
      wlMaxNrLogLines1.setLayoutData( fdlMaxNrLogLinesTemp );
      switch ( e.getType() ) {
        case TEXT_FIELD: // TODO: IMPLEMENT!
          break;
        case CHECK_BOX:
          final Button btn = new Button( wGeneralComp, SWT.CHECK );
          props.setLook( btn );
          btn.setSelection( new Boolean( e.getLastValue().toString() ).booleanValue() );
          btn.setText( e.getLabelText() );
          FormData btnData = new FormData();
          btnData.left = new FormAttachment( middle, 0 );
          btnData.top = new FormAttachment( lastbtn, margin );
          btnData.right = new FormAttachment( 100, 0 );
          btn.setLayoutData( btnData );

          btn.addSelectionListener( new SelectionListener() {

            public void widgetDefaultSelected( SelectionEvent arg0 ) {
            }

            public void widgetSelected( SelectionEvent ev ) {
              e.setValue( btn.getSelection() );

            }
          } );

          lastbtn = btn;
          break;
        default:
          break;
      }

    }

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

  }


  /**
   * Setting the layout of a <i>Reset</i> option button. Either a button image is set - if existing - or a text.
   *
   * @param button The button
   */
  private FormData layoutResetOptionButton( Button button ) {
    FormData fd = new FormData();
    Image editButton = GUIResource.getInstance().getResetOptionButton();
    if ( editButton != null ) {
      button.setImage( editButton );
      button.setBackground( GUIResource.getInstance().getColorWhite() );
      fd.width = editButton.getBounds().width + 20;
      fd.height = editButton.getBounds().height;
    } else {
      button.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.Button.Reset" ) );
    }

    button.setToolTipText( BaseMessages.getString( PKG, "EnterOptionsDialog.Button.Reset.Tooltip" ) );
    return fd;
  }

  /**
   * Setting the layout of an <i>Edit</i> option button. Either a button image is set - if existing - or a text.
   *
   * @param button The button
   */
  private FormData layoutEditOptionButton( Button button ) {
    FormData fd = new FormData();
    Image editButton = GUIResource.getInstance().getEditOptionButton();
    if ( editButton != null ) {
      button.setImage( editButton );
      button.setBackground( GUIResource.getInstance().getColorWhite() );
      fd.width = editButton.getBounds().width + 20;
      fd.height = editButton.getBounds().height;
    } else {
      button.setText( BaseMessages.getString( PKG, "EnterOptionsDialog.Button.Edit" ) );
    }

    button.setToolTipText( BaseMessages.getString( PKG, "EnterOptionsDialog.Button.Edit.Tooltip" ) );
    return fd;
  }

  public void dispose() {
    fixedFont.dispose();
    graphFont.dispose();
    noteFont.dispose();

    background.dispose();
    graphColor.dispose();
    tabColor.dispose();

    shell.dispose();
  }

  public void getData() {
    fixedFontData = props.getFixedFont();
    fixedFont = new Font( display, fixedFontData );

    // Magnify to compensate for the same reduction elsewhere.
    //
    graphFontData = props.getGraphFont();
    graphFontData.height*=PropsUI.getNativeZoomFactor();
    graphFont = new Font( display, graphFontData );

    noteFontData = props.getNoteFont();
    noteFont = new Font( display, noteFontData );

    backgroundRGB = props.getBackgroundRGB();
    if ( backgroundRGB == null ) {
      backgroundRGB = display.getSystemColor( SWT.COLOR_WIDGET_BACKGROUND ).getRGB();
    }
    background = new Color( display, backgroundRGB );

    graphColorRGB = props.getGraphColorRGB();
    graphColor = new Color( display, graphColorRGB );

    tabColorRGB = props.getTabColorRGB();
    tabColor = new Color( display, tabColorRGB );
  }

  private void cancel() {
    props.setScreen( new WindowProperty( shell ) );
    props = null;
    dispose();
  }

  private void ok() {
    props.setFixedFont( fixedFontData );
    props.setGraphFont( graphFontData );
    props.setNoteFont( noteFontData );
    props.setBackgroundRGB( backgroundRGB );
    props.setGraphColorRGB( graphColorRGB );
    props.setTabColorRGB( tabColorRGB );
    props.setIconSize( Const.toInt( wIconsize.getText(), props.getIconSize() ) );
    props.setLineWidth( Const.toInt( wLineWidth.getText(), props.getLineWidth() ) );
    props.setShadowSize( Const.toInt( wShadowSize.getText(), props.getShadowSize() ) );
    props.setMiddlePct( Const.toInt( wMiddlePct.getText(), props.getMiddlePct() ) );
    props.setCanvasGridSize( Const.toInt( wGridSize.getText(), 1 ) );

    props.setDefaultPreviewSize( Const.toInt( wDefaultPreview.getText(), props.getDefaultPreviewSize() ) );

    props.setMaxNrLinesInLog( Const.toInt( wMaxNrLogLines.getText(), Const.MAX_NR_LOG_LINES ) );
    props.setMaxLogLineTimeoutMinutes( Const.toInt(
      wMaxLogLineTimeout.getText(), Const.MAX_LOG_LINE_TIMEOUT_MINUTES ) );
    props.setMaxNrLinesInHistory( Const.toInt( wMaxNrHistLines.getText(), Const.MAX_NR_HISTORY_LINES ) );

    props.setUseDBCache( wUseCache.getSelection() );
    props.setOpenLastFile( wOpenLast.getSelection() );
    props.setAutoSave( wAutoSave.getSelection() );
    props.setAutoSplit( wAutoSplit.getSelection() );
    props.setShowCopyOrDistributeWarning( wCopyDistrib.getSelection() );
    props.setShowCanvasGridEnabled( wShowCanvasGrid.getSelection() );
    props.setExitWarningShown( wExitWarning.getSelection() );
    props.setOSLookShown( wOriginalLook.getSelection() );
    props.setShowToolTips( tooltipBtn.getSelection() );
    // props.setIndicateSlowPipelineTransformsEnabled( wIndicateSlowTransforms.getSelection() );
    props.setAutoCollapseCoreObjectsTree( autoCollapseBtn.getSelection() );
    props.setShowingHelpToolTips( helptipBtn.getSelection() );

    int defaultLocaleIndex = wDefaultLocale.getSelectionIndex();
    if ( defaultLocaleIndex < 0 || defaultLocaleIndex >= GlobalMessages.localeCodes.length ) {
      // Code hardening, when the combo-box ever gets in a strange state,
      // use the first language as default (should be English)
      defaultLocaleIndex = 0;
    }

    String defaultLocale = GlobalMessages.localeCodes[ defaultLocaleIndex ];
    LanguageChoice.getInstance().setDefaultLocale( EnvUtil.createLocale( defaultLocale ) );

    LanguageChoice.getInstance().saveSettings();

    props.saveProps();

    dispose();
  }
}
