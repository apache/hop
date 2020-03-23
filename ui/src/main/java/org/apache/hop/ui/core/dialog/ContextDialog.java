package org.apache.hop.ui.core.dialog;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.SwtUniversalImage;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.GuiAction;
import org.apache.hop.core.gui.plugin.GuiActionType;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.StepPluginType;
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
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.ScrollBar;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
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

  private int cellWidth;
  private int cellHeight;
  private int margin;

  private boolean shiftClicked;
  private boolean ctrlClicked;

  public ContextDialog( Shell parent, String message, Point location, List<GuiAction> actions ) {
    this.parent = parent;
    this.message = message;
    this.location = location;
    this.actions = actions;

    props = PropsUI.getInstance();
    imageMap = new HashMap<>();
    filteredActions = new HashSet<>();
    selectionMap = new HashMap<>();

    shiftClicked = false;
    ctrlClicked = false;

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

    // Calculate the cell width height
    //
    margin = props.getMargin();
    cellWidth = maxNameWidth + margin;
    cellHeight = iconSize + margin + maxNameHeight + margin;

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
    wCanvas = new Canvas( shell, SWT.NO_BACKGROUND | SWT.V_SCROLL );
    FormData fdCanvas = new FormData();
    fdCanvas.left = new FormAttachment( 0, 0 );

    fdCanvas.right = new FormAttachment( 100, 0 );
    fdCanvas.top = new FormAttachment( wSearch, props.getMargin() );
    fdCanvas.bottom = new FormAttachment( wlTooltip, -props.getMargin() );
    wCanvas.setLayoutData( fdCanvas );

    // Set the tooltip
    //
    changeSelectedAction( selectedAction );

    // TODO: Calcualte a more dynamic size based on number of actions, screen size and so on
    //
    int width = (int) Math.round( 1000 * props.getZoomFactor() );
    int height = (int) Math.round( 750 * props.getZoomFactor() );
    shell.setSize( width, height );

    // Position the dialog where there was a click to be more intuitive
    //
    if ( location != null ) {
      shell.setLocation( location.x, location.y );
    } else {
      Rectangle parentBounds = HopGui.getInstance().getShell().getBounds();
      shell.setLocation( Math.max( ( parentBounds.width - width ) / 2, 0 ), Math.max( ( parentBounds.height - height ) / 2, 0 ) );
    }

    // Show the dialog now
    //
    shell.layout();
    shell.open();

    // Add all the listeners
    //
    shell.addFocusListener( this );
    shell.addShellListener( this );
    shell.addListener( SWT.Resize, ( e ) -> changeVerticalBar() );
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
    wCanvas.addPaintListener( this );
    wCanvas.addMouseListener( this );
    wCanvas.getVerticalBar().addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        wCanvas.redraw();
      }
    } );
    changeVerticalBar();

    wSearch.setFocus();
    wCanvas.redraw();

    // Wait until the dialog is closed
    //
    while ( !shell.isDisposed() ) {
      if ( !shell.getDisplay().readAndDispatch() ) {
        shell.getDisplay().sleep();
      }
    }

    return selectedAction;
  }

  private void changeVerticalBar() {
    ScrollBar verticalBar = wCanvas.getVerticalBar();
    verticalBar.setMinimum( 0 );
    verticalBar.setIncrement( 1 );
    int pageRows = wCanvas.getBounds().height / cellHeight;
    verticalBar.setPageIncrement( pageRows );
    verticalBar.setMaximum( calculateNrRows() + pageRows );
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

    // Draw all actions
    //
    int x = 0;
    int y = 0;

    // How many rows and columns do we have?
    // The canvas width, height and the number of selected actions gives us a clue:
    //
    int nrColumns = calculateNrColumns();
    int nrRows = calculateNrRows();
    if ( nrColumns == 0 || nrRows == 0 ) {
      return;
    }

    // So at which row do we start rendering?
    //
    int startRow = calculateStartRow( nrRows );
    int startAction = startRow * nrColumns;

    // Start rendering one row up if possible
    // That way we can easily scroll up
    //
    if ( startRow > 0 ) {
      startRow--;
      y -= cellHeight;
    }

    selectionMap = new HashMap<>();

    List<GuiAction> filtered = new ArrayList<>();
    for ( GuiAction action : actions ) {
      if ( filteredActions.contains( action.getId() ) ) {
        filtered.add( action );
      }
    }

    for ( int i = startAction; i < filtered.size(); i++ ) {
      GuiAction action = filtered.get( i );
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

      x += cellWidth;

      if ( x + maxNameWidth > e.width ) {
        x = 0;
        y += cellHeight;
      }
      if ( y > e.height + 2 * cellHeight ) {
        break;
      }
    }
  }

  private int calculateNrColumns() {
    double columns = (double) ( wCanvas.getBounds().width ) / (double) cellWidth;
    return (int) Math.floor( columns );
  }

  private int calculateNrRows() {
    int nrRows = calculateNrColumns();
    if ( nrRows == 0 ) {
      return 0;
    }
    return (int) Math.ceil( (double) filteredActions.size() / (double) nrRows );
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
    for ( int i = 0; i < filters.length; i++ ) {
      filters[ i ] = Const.trim( filters[ i ] );
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
    if ( filteredActions.isEmpty() ) {
      return;
    }
    if ( selectedAction == null ) {
      selectedAction = findAction( 5, 5 );
    } else {
      Rectangle rectangle = selectionMap.get( selectedAction.getId() );
      if ( rectangle == null ) {
        return;
      }
      GuiAction action = null;
      if ( e.keyCode == SWT.ARROW_DOWN ) {
        action = findAction( rectangle.x + 5, rectangle.y + 5 + rectangle.height + props.getMargin() );
      }
      if ( e.keyCode == SWT.ARROW_UP ) {
        action = findAction( rectangle.x + 5, rectangle.y + 5 - rectangle.height - props.getMargin() );
      }
      if ( e.keyCode == SWT.ARROW_LEFT ) {
        action = findAction( rectangle.x + 5 - rectangle.width - props.getMargin(), rectangle.y + 5 );
      }
      if ( e.keyCode == SWT.ARROW_RIGHT ) {
        action = findAction( rectangle.x + 5 + rectangle.width + props.getMargin(), rectangle.y + 5 );
      }
      if ( e.keyCode == SWT.HOME ) {
        // Position on the first row and column of the screen
        //
        action = findHomeAction();
      }
      if ( e.keyCode == SWT.ESC ) {
        cancel();
      }
      if ( action != null ) {
        selectedAction = action;
        e.doit = false;

        Rectangle r = selectionMap.get( action.getId() );
        Rectangle bounds = wCanvas.getBounds();
        ScrollBar bar = wCanvas.getVerticalBar();
        if ( r.y + r.height > bounds.height ) {
          // We scrolled down and need to scroll the scrollbar
          //
          bar.setSelection( Math.min( bar.getSelection() + bar.getPageIncrement(), bar.getMaximum() ) );
        }
        if ( r.y < 0 ) {
          // We scrolled up and need to scroll the scrollbar up
          //
          bar.setSelection( Math.max( bar.getSelection() - bar.getPageIncrement(), bar.getMinimum() ) );
        }
      }
    }
    changeSelectedAction( selectedAction );
    wCanvas.redraw();
  }

  private int calculateStartRow( int nrRows ) {
    // So at which row do we start rendering?
    //
    ScrollBar bar = wCanvas.getVerticalBar();
    double pctDown = 100.0d * bar.getSelection() / bar.getMaximum();
    int startRow = (int) pctDown * nrRows / 100;
    return startRow;
  }

  private GuiAction findHomeAction() {
    int startRow = calculateStartRow( calculateNrRows() );
    int actionNr = startRow * calculateNrColumns();
    int index = 0;
    for ( GuiAction action : actions ) {
      if ( filteredActions.contains( action.getId() ) ) {
        if ( actionNr == index ) {
          return action;
        }
        index++;
      }
    }
    return null;
  }

  private GuiAction findAction( int x, int y ) {
    for ( String id : selectionMap.keySet() ) {
      Rectangle rect = selectionMap.get( id );
      if ( rect.contains( x, y ) ) {
        return findAction( id );
      }
    }
    return null;
  }

  private GuiAction findAction( String id ) {
    for ( GuiAction action : actions ) {
      if ( action.getId().equals( id ) ) {
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
    if ( action != null ) {
      selectedAction = action;
      shiftClicked = (e.stateMask & SWT.SHIFT)!=0;
      ctrlClicked = (e.stateMask & SWT.CONTROL)!=0 || (Const.isOSX() && (e.stateMask & SWT.COMMAND)!=0);

      dispose();
    }
  }

  @Override public void mouseUp( MouseEvent e ) {

  }

  @Override public void shellActivated( ShellEvent e ) {
  }

  /**
   * We hit this when Escape is hit by the user
   *
   * @param e
   */
  @Override public void shellClosed( ShellEvent e ) {
    selectedAction = null;
  }

  @Override public void shellDeactivated( ShellEvent e ) {
  }

  @Override public void shellDeiconified( ShellEvent e ) {
  }

  @Override public void shellIconified( ShellEvent e ) {
  }

  public static void main( String[] args ) throws Exception {
    Display display = new Display();
    Shell shell = new Shell( display, SWT.MIN | SWT.MAX | SWT.RESIZE );
    // shell.setSize( 500, 500 );
    // shell.open();

    HopClientEnvironment.init();
    PropsUI.init( display );
    HopEnvironment.init();

    List<GuiAction> actions = new ArrayList<>();
    List<PluginInterface> stepPlugins = PluginRegistry.getInstance().getPlugins( StepPluginType.class );
    for ( PluginInterface stepPlugin : stepPlugins ) {
      GuiAction createStepAction =
        new GuiAction( "transgraph-create-step-" + stepPlugin.getIds()[ 0 ], GuiActionType.Create, stepPlugin.getName(), stepPlugin.getDescription(), stepPlugin.getImageFile(),
          (shiftClicked, controlClicked, t) -> System.out.println( "Create step action : " + stepPlugin.getName() + ", shift="+shiftClicked+", control="+controlClicked )
        );
      createStepAction.getKeywords().add( stepPlugin.getCategory() );
      // if (actions.size()<2) {
      actions.add( createStepAction );
      //}
    }
    ContextDialog dialog = new ContextDialog( shell, "Action test", new Point( 50, 50 ), actions );
    GuiAction action = dialog.open();
    if ( action == null ) {
      System.out.println( "There was no selection in dialog" );
    } else {
      System.out.println( "Selected action : " + action );
    }

    // Cleanup
    //
    display.dispose();
  }

  /**
   * Gets shiftClicked
   *
   * @return value of shiftClicked
   */
  public boolean isShiftClicked() {
    return shiftClicked;
  }

  /**
   * @param shiftClicked The shiftClicked to set
   */
  public void setShiftClicked( boolean shiftClicked ) {
    this.shiftClicked = shiftClicked;
  }

  /**
   * Gets ctrlClicked
   *
   * @return value of ctrlClicked
   */
  public boolean isCtrlClicked() {
    return ctrlClicked;
  }

  /**
   * @param ctrlClicked The ctrlClicked to set
   */
  public void setCtrlClicked( boolean ctrlClicked ) {
    this.ctrlClicked = ctrlClicked;
  }
}
