package org.apache.hop.ui.core.dialog;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.SwtUniversalImage;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.GuiAction;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.KeyListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.MouseListener;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.PaintListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.events.ShellListener;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ContextDialog implements PaintListener, ModifyListener, FocusListener, KeyListener, MouseListener, ShellListener {
  private Shell parent;
  private String message;
  private Point location;
  private Shell shell;
  private List<GuiAction> actions;

  private PropsUI props;
  private GuiAction selectedAction;
  private Map<String, Image> imageMap;
  private Set<String> filteredActions;
  private Map<String, Rectangle> selectionMap;

  private Text wSearch;
  private Label wlTooltip;
  private Canvas wCanvas;

  private int iconSize;
  private int maxNameWidth;
  private int maxNameHeight;

  public ContextDialog( Shell parent, String message, Point location, List<GuiAction> actions ) {
    this.parent = parent;
    this.message = message;
    this.location = location;
    this.actions = actions;

    props = PropsUI.getInstance();
    imageMap = new HashMap<>();
    filteredActions = new HashSet<>();
    selectionMap = new HashMap<>();

    if ( actions.isEmpty() ) {
      selectedAction = null;
    } else {
      // Just grab the first by default
      selectedAction = actions.get( 0 );
    }

    iconSize = (int) Math.round( 2 * props.getZoomFactor() * props.getIconSize() );
  }

  public GuiAction open() {

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE );
    shell.setText( message );
    shell.setImage( GUIResource.getInstance().getImageHop() );
    props.setLook( shell );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );

    shell.addFocusListener( this );
    shell.addShellListener( this );

    // Load all the images...
    // Filter all actions by default
    //
    maxNameWidth = iconSize;
    maxNameHeight = 0;
    Image img = new Image( parent.getDisplay(), 100, 100 );
    GC gc = new GC( img );

    for ( GuiAction action : actions ) {
      ClassLoader classLoader = action.getClassLoader();
      if ( classLoader == null ) {
        classLoader = ClassLoader.getSystemClassLoader();
      }
      SwtUniversalImage universalImage = SwtSvgImageUtil.getUniversalImage( parent.getDisplay(), classLoader, action.getImage() );
      Image image = universalImage.getAsBitmapForSize( parent.getDisplay(), iconSize, iconSize );
      imageMap.put( action.getId(), image );
      filteredActions.add( action.getId() );

      if ( action.getName() != null ) {
        org.eclipse.swt.graphics.Point extent = gc.textExtent( action.getName() );
        if ( extent.x > maxNameWidth ) {
          maxNameWidth = extent.x;
        }
        if ( extent.y > maxNameHeight ) {
          maxNameHeight = extent.y;
        }
      }
    }
    gc.dispose();
    img.dispose();

    // Add a search bar at the top...
    //
    Label wlSearch = new Label( shell, SWT.LEFT );
    wlSearch.setText( "Search: " );
    FormData fdlSearch = new FormData();
    fdlSearch.left = new FormAttachment( 0, 0 );
    fdlSearch.top = new FormAttachment( 0, 0 );
    wlSearch.setLayoutData( fdlSearch );

    wSearch = new Text( shell, SWT.LEFT | SWT.BORDER | SWT.SINGLE );
    FormData fdSearch = new FormData();
    fdSearch.left = new FormAttachment( wlSearch, props.getMargin() );
    fdSearch.right = new FormAttachment( 100, 0 );
    fdSearch.top = new FormAttachment( wlSearch, 0, SWT.CENTER );
    wSearch.setLayoutData( fdSearch );
    wSearch.addModifyListener( this );
    wSearch.addFocusListener( this );
    wSearch.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetDefaultSelected( SelectionEvent e ) {
        // Pressed enter
        //
        dispose();
      }
    } );
    wSearch.addKeyListener( this );

    // Add a description label at the bottom...
    //
    wlTooltip = new Label( shell, SWT.LEFT );
    FormData fdlTooltip = new FormData();
    fdlTooltip.left = new FormAttachment( 0, 0 );
    fdlTooltip.right = new FormAttachment( 100, 0 );
    fdlTooltip.bottom = new FormAttachment( 100, 0 );
    wlTooltip.setLayoutData( fdlTooltip );

    // The rest of the dialog is used to draw the actions...
    //
    wCanvas = new Canvas( shell, SWT.NO_BACKGROUND | SWT.V_SCROLL | SWT.H_SCROLL );
    FormData fdCanvas = new FormData();
    fdCanvas.left = new FormAttachment( 0, 0 );
    fdCanvas.right = new FormAttachment( 100, 0 );
    fdCanvas.top = new FormAttachment( wSearch, props.getMargin() );
    fdCanvas.bottom = new FormAttachment( wlTooltip, -props.getMargin() );
    wCanvas.setLayoutData( fdCanvas );
    wCanvas.addPaintListener( this );
    wCanvas.addMouseListener( this );

    // Set the tooltip
    //
    changeSelectedAction( selectedAction );

    wSearch.setFocus();

    // TODO: Calcualte a more dynamic size based on number of actions, screen size and so on

    Rectangle parentBounds = HopGui.getInstance().getShell().getBounds();
    int width = (int) Math.round( 1000 * props.getZoomFactor() );
    int height = (int) Math.round( 750 * props.getZoomFactor() );
    shell.setSize( width, height );

    // Position the dialog where there was a click to be more intuitive
    //
    if (location!=null) {
      shell.setLocation( location.x, location.y );
    } else {
      shell.setLocation( Math.max( ( parentBounds.width - width ) / 2, 0 ), Math.max( ( parentBounds.height - height ) / 2, 0 ) );
    }

    // Show the dialog now
    //
    shell.open();

    // Wait until the dialog is closed
    //
    while ( !shell.isDisposed() ) {
      if ( !shell.getDisplay().readAndDispatch() ) {
        shell.getDisplay().sleep();
      }
    }

    return selectedAction;
  }

  private void cancel() {
    selectedAction = null;
    dispose();
  }

  private void dispose() {
    // Clean up the images...
    //
    for ( Image image : imageMap.values() ) {
      image.dispose();
    }
    shell.dispose();
  }

  /**
   * This is where all the actions are drawn
   *
   * @param e
   */
  @Override public void paintControl( PaintEvent e ) {
    // Draw all actions
    //
    int margin = props.getMargin();

    int x = 0;
    int y = 0;

    GC gc = e.gc;

    // Fill everything with white...
    //
    gc.setForeground( GUIResource.getInstance().getColorBackground() );
    gc.setBackground( GUIResource.getInstance().getColorBackground() );
    gc.fillRectangle( 0, 0, e.width, e.height );

    // For text and lines...
    //
    gc.setForeground( GUIResource.getInstance().getColorBlack() );
    gc.setLineWidth( 4 );

    selectionMap = new HashMap<>();

    for ( GuiAction action : actions ) {
      if ( filteredActions.contains( action.getId() ) ) {
        Rectangle selectionBox = new Rectangle( x, y, maxNameWidth, iconSize + margin + maxNameHeight );
        selectionMap.put( action.getId(), selectionBox );

        org.eclipse.swt.graphics.Point extent = gc.textExtent( action.getName() );
        Image image = imageMap.get( action.getId() );

        boolean selected = selectedAction != null && action.equals( selectedAction );
        if ( selected ) {
          gc.setBackground( GUIResource.getInstance().getColorLightBlue() );
          gc.fillRectangle( selectionBox );
        } else {
          gc.setBackground( GUIResource.getInstance().getColorBackground() );
        }

        gc.drawImage( image, x + ( maxNameWidth - iconSize ) / 2, y );
        gc.drawText( action.getName(), x + ( maxNameWidth - extent.x ) / 2, y + iconSize + margin );

        x += maxNameWidth + margin;

        if ( x + maxNameWidth > e.width ) {
          x = 0;
          y += iconSize + margin + maxNameHeight + margin;
        }
        if ( y > e.height ) {
          break;
        }
      }
    }
  }

  private void changeSelectedAction( GuiAction action ) {
    this.selectedAction = action;

    if ( action == null ) {
      wlTooltip.setText( "" );
    } else {
      wlTooltip.setText( Const.NVL( action.getTooltip(), "" ) );
    }
  }

  /**
   * If the search text is modified we end up here...
   *
   * @param e
   */
  @Override public void modifyText( ModifyEvent e ) {
    filteredActions = new HashSet<>();
    GuiAction firstAction = null;

    String filterString = wSearch.getText();
    String[] filters = filterString.split( "," );
    for (int i=0;i<filters.length;i++) {
      filters[i] = Const.trim( filters[i] );
    }

    for ( GuiAction action : actions ) {
      if ( StringUtils.isEmpty( filterString ) || action.containsFilterStrings( filters ) ) {
        filteredActions.add( action.getId() );
        if ( firstAction == null ) {
          firstAction = action;
        }
      }
    }
    // change to a new default selection: first in the list
    //
    if ( filteredActions.isEmpty() ) {
      changeSelectedAction( null );
    } else {
      if ( selectedAction == null || !filteredActions.contains( selectedAction.getId() ) ) {
        changeSelectedAction( firstAction );
      }
    }
    wCanvas.redraw();
  }

  @Override public void focusGained( FocusEvent e ) {
  }

  @Override public void focusLost( FocusEvent e ) {
    cancel();
  }

  @Override public void keyPressed( KeyEvent e ) {
    if (filteredActions.isEmpty()) {
      return;
    }
    if (selectedAction==null) {
      selectedAction = findAction(5,5);
    } else {
      Rectangle rectangle = selectionMap.get( selectedAction.getId() );
      GuiAction action = null;
      if ( e.keyCode == SWT.ARROW_DOWN ) {
        action = findAction( rectangle.x + 5, rectangle.y + 5 + rectangle.height + props.getMargin() );
      }
      if ( e.keyCode == SWT.ARROW_UP ) {
        action = findAction( rectangle.x+5, rectangle.y+5 - rectangle.height - props.getMargin() );
      }
      if ( e.keyCode == SWT.ARROW_LEFT ) {
        action = findAction( rectangle.x+5 - rectangle.width - props.getMargin(), rectangle.y+5 );
      }
      if ( e.keyCode == SWT.ARROW_RIGHT ) {
        action = findAction( rectangle.x+5 + rectangle.width + props.getMargin(), rectangle.y+5 );
      }
      if (e.keyCode==SWT.ESC) {
        cancel();
      }
      if (action!=null) {
        selectedAction = action;
      }
    }
    changeSelectedAction( selectedAction );
    wCanvas.redraw();
    wSearch.setSelection( wSearch.getText().length() );
  }

  private GuiAction findAction( int x, int y ) {
    for (String id : selectionMap.keySet()) {
      Rectangle rect = selectionMap.get( id );
      if (rect.contains( x,y )) {
        return findAction(id);
      }
    }
    return null;
  }

  private GuiAction findAction( String id ) {
    for (GuiAction action : actions) {
      if (action.getId().equals( id )) {
        return action;
      }
    }
    return null;
  }

  @Override public void keyReleased( KeyEvent e ) {

  }

  @Override public void mouseDoubleClick( MouseEvent e ) {

  }

  @Override public void mouseDown( MouseEvent e ) {
    // See where the click was...
    //
    GuiAction action = findAction( e.x, e.y );
    if (action!=null) {
      selectedAction = action;
      dispose();
    }
  }

  @Override public void mouseUp( MouseEvent e ) {

  }

  @Override public void shellActivated( ShellEvent e ) {
  }

  /**
   * We hit this when Escape is hit by the user
   * @param e
   */
  @Override public void shellClosed( ShellEvent e ) {
    selectedAction=null;
  }

  @Override public void shellDeactivated( ShellEvent e ) {
  }

  @Override public void shellDeiconified( ShellEvent e ) {
  }

  @Override public void shellIconified( ShellEvent e ) {
  }
}
