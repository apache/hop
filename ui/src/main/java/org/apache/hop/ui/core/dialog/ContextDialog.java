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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.SwtUniversalImage;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.Rectangle;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.ScrollBar;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class ContextDialog extends Dialog {

  public static final String CATEGORY_OTHER = "Other";
  private Point location;
  private List<GuiAction> actions;
  private PropsUi props;
  private Shell shell;
  private Text wSearch;
  private Label wlTooltip;
  private Canvas wCanvas;

  private int iconSize;

  private int margin;
  private int xMargin;
  private int yMargin;

  private boolean shiftClicked;
  private boolean ctrlClicked;
  private boolean focusLost;

  /**
   * All context items.
   */
  private final List<Item> items = new ArrayList<>();

  /**
   * List of filtered items.
   */
  private final List<Item> filteredItems = new ArrayList<>();

  private Item selectedItem;

  private GuiAction selectedAction;

  private List<AreaOwner> areaOwners = new ArrayList<>();

  private Color highlightColor;

  private int heightOffSet = 0;
  private int totalContentHeight = 0;
  private int previousTotalContentHeight = 0;
  private Font headerFont;
  private Font itemsFont;
  private Item firstShownItem;
  private Item lastShownItem;

  private class CategoryAndOrder {
    String category;
    String order;

    public CategoryAndOrder( String category, String order ) {
      this.category = category;
      this.order = order;
    }

    @Override public boolean equals( Object o ) {
      if ( this == o ) {
        return true;
      }
      if ( o == null || getClass() != o.getClass() ) {
        return false;
      }
      CategoryAndOrder that = (CategoryAndOrder) o;
      return category.equals( that.category );
    }

    @Override public int hashCode() {
      return Objects.hash( category );
    }

    /**
     * Gets category
     *
     * @return value of category
     */
    public String getCategory() {
      return category;
    }

    /**
     * @param category The category to set
     */
    public void setCategory( String category ) {
      this.category = category;
    }

    /**
     * Gets order
     *
     * @return value of order
     */
    public String getOrder() {
      return order;
    }

    /**
     * @param order The order to set
     */
    public void setOrder( String order ) {
      this.order = order;
    }
  }

  private List<CategoryAndOrder> categories;

  private static class Item {
    private GuiAction action;
    private Image image;
    private boolean selected;
    private AreaOwner areaOwner;

    public Item( GuiAction action, Image image ) {
      this.action = action;
      this.image = image;
      this.selected = false;
    }

    public GuiAction getAction() {
      return action;
    }

    public String getText() {
      return action.getShortName();
    }

    public Image getImage() {
      return image;
    }

    /**
     * Gets selected
     *
     * @return value of selected
     */
    public boolean isSelected() {
      return selected;
    }

    /**
     * @param selected The selected to set
     */
    public void setSelected( boolean selected ) {
      this.selected = selected;
    }

    /**
     * Gets areaOwner
     *
     * @return value of areaOwner
     */
    public AreaOwner getAreaOwner() {
      return areaOwner;
    }

    /**
     * @param areaOwner The areaOwner to set
     */
    public void setAreaOwner( AreaOwner areaOwner ) {
      this.areaOwner = areaOwner;
    }

    public void dispose() {
      if ( image != null ) {
        image.dispose();
      }
    }
  }

  public ContextDialog( Shell parent, String title, Point location, List<GuiAction> actions ) {
    super( parent );

    this.setText( title );
    this.location = location;
    this.actions = actions;
    props = PropsUi.getInstance();

    shiftClicked = false;
    ctrlClicked = false;

    // Make the icons a bit smaller to fit more
    //
    iconSize = (int) Math.round( props.getZoomFactor() * props.getIconSize() * 0.75 );
    margin = (int) ( Const.MARGIN * props.getZoomFactor() );
    highlightColor = new Color( parent.getDisplay(), 201, 232, 251 );
  }

  public GuiAction open() {

    shell = new Shell( getParent(), SWT.DIALOG_TRIM | SWT.RESIZE );
    shell.setText( getText() );
    shell.setMinimumSize( new org.eclipse.swt.graphics.Point( 200, 180 ) );
    shell.setImage( GuiResource.getInstance().getImageHop() );
    shell.setLayout( new FormLayout() );

    Display display = shell.getDisplay();

    xMargin = 3 * margin;
    yMargin = 2 * margin;


    // Load the action images
    //
    items.clear();
    for ( GuiAction action : actions ) {
      ClassLoader classLoader = action.getClassLoader();
      if ( classLoader == null ) {
        classLoader = ClassLoader.getSystemClassLoader();
      }
      SwtUniversalImage universalImage = SwtSvgImageUtil.getUniversalImage( display, classLoader, action.getImage() );
      Image image = universalImage.getAsBitmapForSize( display, iconSize, iconSize );
      items.add( new Item( action, image ) );
    }

    // Add a search bar at the top...
    //
    Composite toolBar = new Composite( shell, SWT.NONE );
    toolBar.setLayout( new GridLayout( 2, false ) );
    props.setLook( toolBar );
    FormData fdlToolBar = new FormData();
    fdlToolBar.top = new FormAttachment( 0, 0 );
    fdlToolBar.left = new FormAttachment( 0, 0 );
    fdlToolBar.right = new FormAttachment( 100, 0 );
    toolBar.setLayoutData( fdlToolBar );

    Label wlSearch = new Label( toolBar, SWT.LEFT );
    wlSearch.setText( "Search " );
    props.setLook( wlSearch );

    wSearch = new Text( toolBar, SWT.LEFT | SWT.BORDER | SWT.SINGLE | SWT.SEARCH );
    wSearch.setLayoutData( new GridData( GridData.FILL_BOTH ) );

    // Add a description label at the bottom...
    //
    wlTooltip = new Label( shell, SWT.LEFT );
    FormData fdlTooltip = new FormData();
    fdlTooltip.left = new FormAttachment( 0, Const.FORM_MARGIN );
    fdlTooltip.right = new FormAttachment( 100, -Const.FORM_MARGIN );
    fdlTooltip.top = new FormAttachment( 100, -Const.FORM_MARGIN - (int) ( props.getZoomFactor() * 50 ) );
    fdlTooltip.bottom = new FormAttachment( 100, -Const.FORM_MARGIN );
    wlTooltip.setLayoutData( fdlTooltip );

    // The rest of the dialog is used to draw the actions...
    //
    wCanvas = new Canvas( shell, SWT.NO_BACKGROUND | SWT.V_SCROLL );
    FormData fdCanvas = new FormData();
    fdCanvas.left = new FormAttachment( 0, 0 );
    fdCanvas.right = new FormAttachment( 100, 0 );
    fdCanvas.top = new FormAttachment( toolBar, 0 );
    fdCanvas.bottom = new FormAttachment( wlTooltip, 0 );
    wCanvas.setLayoutData( fdCanvas );

    itemsFont = wCanvas.getFont();

    int fontHeight = wCanvas.getFont().getFontData()[ 0 ].getHeight() + 1;
    headerFont = new Font( getParent().getDisplay(), props.getDefaultFont().getName(), fontHeight, props.getGraphFont().getStyle() | SWT.BOLD | SWT.ITALIC );


    // TODO: Calculate a more dynamic size based on number of actions, screen size
    // and so on
    //
    int width = (int) Math.round( 800 * props.getZoomFactor() );
    int height = (int) Math.round( 600 * props.getZoomFactor() );

    // Position the dialog where there was a click to be more intuitive
    //
    if ( location != null ) {
      shell.setSize( width, height );
      shell.setLocation( location.x, location.y );
    } else {
      BaseTransformDialog.setSize( shell, width, height, false );
    }

    // Add all the listeners
    //
    shell.addListener( SWT.Resize, event -> updateVerticalBar() );
    // shell.addListener( SWT.Deactivate, event -> onFocusLost() );

    wSearch.addModifyListener( event -> onModifySearch() );

    KeyAdapter keyAdapter = new KeyAdapter() {
      @Override
      public void keyPressed( KeyEvent event ) {
        onKeyPressed( event );
      }
    };
    wSearch.addKeyListener( keyAdapter );


    wSearch.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetDefaultSelected( SelectionEvent e ) {
        // Pressed enter
        //
        if ( selectedItem != null ) {
          selectedAction = selectedItem.getAction();
        }
        dispose();
      }
    } );

    wCanvas.addPaintListener( event -> onPaint( event ) );
    wCanvas.addMouseListener( new MouseAdapter() {
      @Override
      public void mouseDown( MouseEvent event ) {
        // See where the click was...
        //
        Item item = findItem( event.x, event.y );
        if ( item != null ) {
          selectedAction = item.getAction();

          shiftClicked = ( event.stateMask & SWT.SHIFT ) != 0;
          ctrlClicked = ( event.stateMask & SWT.CONTROL ) != 0 || ( Const.isOSX() && ( event.stateMask & SWT.COMMAND ) != 0 );

          dispose();
        }
      }
    } );
    wCanvas.addMouseMoveListener( ( MouseEvent event ) -> {
      // Do we mouse over an action?
      //
      Item item = findItem( event.x, event.y );
      if ( item != null ) {
        selectItem( item, false );
      }
    } );
    wCanvas.getVerticalBar().addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        wCanvas.redraw();
      }
    } );
    wCanvas.addKeyListener( keyAdapter );

    // Show the dialog now
    //
    shell.layout();
    shell.open();

    // Filter all actions by default
    //
    this.filter( null );

    // Force focus on the search bar
    //
    wSearch.setFocus();

    // Wait until the dialog is closed
    //
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }

    return selectedAction;
  }

  private void dispose() {

    // Save the shell size and location in case the position isn't a mouse click
    //
    if ( location == null ) {
      props.setScreen( new WindowProperty( shell ) );
    }

    // Close the dialog window
    shell.close();

    // Clean up the images...
    //
    for ( Item item : items ) {
      item.dispose();
    }
    highlightColor.dispose();
    headerFont.dispose();
  }

  private boolean paintInitialized = false;

  /**
   * This is where all the actions are drawn
   *
   * @param event
   */
  private void onPaint( PaintEvent event ) {

    // Do double buffering to prevent flickering on Windows
    //
    boolean needsDoubleBuffering = Const.isWindows() && "GUI".equalsIgnoreCase( Const.getHopPlatformRuntime() );

    Image image = null;
    GC gc = event.gc;

    if ( needsDoubleBuffering ) {
      image = new Image( shell.getDisplay(), event.width, event.height );
      gc = new GC( image );
    }


    boolean useCategories = true; // TODO: replace by checkbox selection value

    if ( !paintInitialized ) {

      // Let's take a look at the list of actions and see if we've got categories to use...
      //
      categories = new ArrayList<>();
      for ( GuiAction action : actions ) {
        if ( StringUtils.isNotEmpty( action.getCategory() ) ) {
          CategoryAndOrder categoryAndOrder = new CategoryAndOrder( action.getCategory(), Const.NVL( action.getCategoryOrder(), "0" ) );
          if ( !categories.contains( categoryAndOrder ) ) {
            categories.add( categoryAndOrder );
          }
        } else {
          // Add an "Other" category
          CategoryAndOrder categoryAndOrder = new CategoryAndOrder( CATEGORY_OTHER, "9999" );
          if ( !categories.contains( categoryAndOrder ) ) {
            categories.add( categoryAndOrder );
          }
        }
      }

      categories.sort( Comparator.comparing( o -> o.order ) );

      useCategories &= !categories.isEmpty();

      paintInitialized = true;
    }

    // Fill everything with white...
    //
    gc.setForeground( GuiResource.getInstance().getColorBlack() );
    gc.setBackground( GuiResource.getInstance().getColorBackground() );
    gc.fillRectangle( 0, 0, event.width, event.height );

    // For text and lines...
    //
    gc.setForeground( GuiResource.getInstance().getColorBlack() );
    gc.setLineWidth( 1 );

    // Remember the area owners
    //
    areaOwners = new ArrayList<>();

    // The size of the canvas right now?
    //
    org.eclipse.swt.graphics.Rectangle canvasBounds = wCanvas.getBounds();

    // Did we draw before?
    // If so we might have a maximum height and a scrollbar selection
    //
    if ( totalContentHeight > 0 ) {
      ScrollBar verticalBar = wCanvas.getVerticalBar();

      if ( totalContentHeight > canvasBounds.height ) {
        heightOffSet = totalContentHeight * verticalBar.getSelection() / ( 100 - verticalBar.getThumb() );
      } else {
        heightOffSet = 0;
      }

      System.out.println("Bar="+verticalBar.getSelection()+"%  thumb="+verticalBar.getThumb()+"%  offset="+heightOffSet+"  total="+totalContentHeight);
    }

    // Draw all actions
    // Loop over the categories, if any...
    //
    int height = 0; // should always be about the same
    int categoryNr = 0;
    int x = margin;
    int y = margin - heightOffSet;

    firstShownItem = null;

    while ( categoryNr < categories.size() || ( ( !useCategories || categories.isEmpty() ) && categoryNr == 0 ) ) {

      CategoryAndOrder categoryAndOrder;
      if ( !useCategories || categories.isEmpty() ) {
        categoryAndOrder = null;
      } else {
        categoryAndOrder = categories.get( categoryNr );
      }

      // Get the list of actions for the given categoryAndOrder
      //
      List<Item> itemsToPaint = findItemsForCategory( categoryAndOrder );

      if ( !itemsToPaint.isEmpty() ) {
        if ( categoryAndOrder != null ) {
          // Draw the category header
          //
          gc.setFont( headerFont );
          org.eclipse.swt.graphics.Point categoryExtent = gc.textExtent( categoryAndOrder.category );
          // gc.drawLine( margin, y-1, canvasBounds.width - xMargin, y-1 );
          gc.drawText( categoryAndOrder.category, x, y );
          areaOwners.add( new AreaOwner( AreaOwner.AreaType.CUSTOM, x, y + heightOffSet, categoryExtent.x, categoryExtent.y, new Point( 0, heightOffSet ), null, categoryAndOrder ) );
          y += categoryExtent.y + yMargin;
          gc.setLineWidth( 1 );
          gc.drawLine( margin, y - yMargin, canvasBounds.width - xMargin, y - yMargin );
        }

        gc.setFont( itemsFont );


        // Paint the action items
        //
        for ( Item item : itemsToPaint ) {

          lastShownItem = item;
          if ( firstShownItem == null ) {
            firstShownItem = item;
          }

          String name = Const.NVL( item.action.getName(), item.action.getId() );

          org.eclipse.swt.graphics.Rectangle imageBounds = item.image.getBounds();
          org.eclipse.swt.graphics.Point nameExtent = gc.textExtent( name );

          int width = Math.max( nameExtent.x, imageBounds.width );
          height = nameExtent.y + margin + imageBounds.height;

          if ( x + width + xMargin > canvasBounds.width ) {
            x = margin;
            y += height + yMargin;
          }

          if ( item.isSelected() ) {
            gc.setLineWidth( 2 );
            gc.setBackground( highlightColor );
            gc.fillRoundRectangle( x - xMargin / 2, y - yMargin / 2, width + xMargin, height + yMargin, margin, margin );
          }

          // So we draw the icon in the centre of the name text...
          //
          gc.drawImage( item.image, x + nameExtent.x / 2 - imageBounds.width / 2, y );

          // Then we draw the text underneath
          //
          gc.drawText( name, x, y + imageBounds.height + margin );

          // Reset the background color
          //
          gc.setLineWidth( 1 );
          gc.setBackground( GuiResource.getInstance().getColorBackground() );

          // The drawn area is the complete rectangle
          //
          AreaOwner areaOwner = new AreaOwner( AreaOwner.AreaType.CUSTOM, x, y + heightOffSet, width, height, new Point( 0, heightOffSet ), null, item );
          areaOwners.add( areaOwner );
          item.setAreaOwner( areaOwner );

          // Now we advance x and y to where we want to draw the next one...
          //
          x += width + xMargin;
          if ( x > canvasBounds.width ) {
            x = margin;
            y += height + yMargin;
          }
        }

        // Back to the left on a next line to draw the next category (if any)
        //
        x = margin;
        y += height + yMargin;
      }

      // Pick the next category
      //
      categoryNr++;
      y += yMargin;
    }

    totalContentHeight = y + heightOffSet;

    if ( previousTotalContentHeight != totalContentHeight ) {
      previousTotalContentHeight = totalContentHeight;
      updateVerticalBar();
    }

    if ( needsDoubleBuffering ) {
      // Draw the image onto the canvas and get rid of the resources
      //
      event.gc.drawImage( image, 0, 0 );
      gc.dispose();
      image.dispose();
    }
  }

  private List<Item> findItemsForCategory( CategoryAndOrder categoryAndOrder ) {
    List<Item> list = new ArrayList<>();
    for ( Item filteredItem : filteredItems ) {
      if ( categoryAndOrder == null || categoryAndOrder.category.equalsIgnoreCase( filteredItem.action.getCategory() ) ) {
        list.add( filteredItem );
      } else if ( CATEGORY_OTHER.equals( categoryAndOrder.category ) && StringUtils.isEmpty( filteredItem.action.getCategory() ) ) {
        list.add( filteredItem );
      }
    }
    return list;
  }


  private void selectItem( Item selectedItem, boolean scroll ) {

    for ( Item item : items ) {
      item.setSelected( false );
    }

    if ( selectedItem == null ) {
      wlTooltip.setText( "" );
    } else {
      this.selectedItem = selectedItem;
      wlTooltip.setText( Const.NVL( selectedItem.getAction().getTooltip(), "" ) );
      selectedItem.setSelected( true );

      // See if we need to show the selected item.
      //
      if ( scroll && totalContentHeight > 0 ) {
        org.eclipse.swt.graphics.Rectangle canvasBounds = wCanvas.getBounds();
        Rectangle area = selectedItem.getAreaOwner().getArea();
        ScrollBar verticalBar = wCanvas.getVerticalBar();
        if ( area.y + area.height + 2 * yMargin > canvasBounds.height ) {
          verticalBar.setSelection( Math.min( verticalBar.getSelection() + verticalBar.getPageIncrement(), 100 - verticalBar.getThumb() ) );
        } else if ( area.y < 0 ) {
          verticalBar.setSelection( Math.max( verticalBar.getSelection() - verticalBar.getPageIncrement(), 0 ) );
        }

      }
    }

    wCanvas.redraw();
  }

  private void filter( String text ) {

    if ( text == null ) {
      text = "";
    }

    String[] filters = text.split( "," );
    for ( int i = 0; i < filters.length; i++ ) {
      filters[ i ] = Const.trim( filters[ i ] );
    }

    filteredItems.clear();
    for ( Item item : items ) {
      GuiAction action = item.getAction();

      if ( StringUtils.isEmpty( text ) || action.containsFilterStrings( filters ) ) {
        filteredItems.add( item );
      }
    }

    if ( paintInitialized ) {

      if ( filteredItems.isEmpty() ) {
        selectItem( null, false );
      }

      // if selected item is exclude, change to a new default selection: first in the list
      //
      else if ( !filteredItems.contains( selectedItem ) ) {
        selectItem( filteredItems.get( 0 ), false );
      }
      // Update vertical bar
      //
      this.updateVerticalBar();
    }

    wCanvas.redraw();
  }

  private void onFocusLost() {
    // focusLost = true;

    // dispose();
  }

  private void onModifySearch() {
    String text = wSearch.getText();
    this.filter( text );
  }

  private void onKeyPressed( KeyEvent event ) {

    if ( filteredItems.isEmpty() ) {
      return;
    }

    // Which item area are we currently using as a base...
    //
    org.apache.hop.core.gui.Rectangle area = null;
    ScrollBar verticalBar = wCanvas.getVerticalBar();

    if ( selectedItem == null ) {
      // Select the first shown item
      if ( firstShownItem != null ) {
        area = firstShownItem.getAreaOwner().getArea();
      }
    } else {
      area = selectedItem.getAreaOwner().getArea();
    }

    switch ( event.keyCode ) {
      case SWT.ARROW_DOWN:
        // Find the next item down...
        //
        selectItemDown( area );
        break;
      case SWT.ARROW_UP:
        selectItemUp( area );
        break;
      case SWT.PAGE_UP:
        break;
      case SWT.PAGE_DOWN:
        break;
      case SWT.ARROW_LEFT:
        selectItemLeft( area );
        break;
      case SWT.ARROW_RIGHT:
        selectItemRight( area );
        break;
      case SWT.HOME:
        verticalBar.setSelection( 0 );
        selectItem( firstShownItem, true );
        break;
      case SWT.END:
        Rectangle lastArea = lastShownItem.getAreaOwner().getArea();
        int bottomY = lastArea.y + lastArea.height + yMargin;
        int percentage = (int)((100-verticalBar.getThumb()) * ((double)bottomY / totalContentHeight));
        verticalBar.setSelection( percentage - verticalBar.getThumb()/2 );
        selectItem( lastShownItem, true );
        break;
    }

  }

  private void selectClosest( Rectangle area, List<AreaOwner> areas ) {
    // Sort by distance...
    //
    areas.sort( ( o1, o2 ) -> (int) ( o1.getArea().distance( area ) - o2.getArea().distance( area ) ) );

    if ( !areas.isEmpty() ) {
      Item item = (Item) areas.get( 0 ).getOwner();
      selectItem( item, true );
    }
  }

  /**
   * Find an area owner directly to the right of the area
   *
   * @param area
   */
  private void selectItemRight( Rectangle area ) {
    List<AreaOwner> rightAreas = new ArrayList<>();
    for ( AreaOwner areaOwner : areaOwners ) {
      if ( areaOwner.getOwner() instanceof Item ) {
        // Only keep the items to the left
        //
        Rectangle r = areaOwner.getArea();
        if ( r.x > area.x + area.width ) {
          if ( r.y - 2 * yMargin < area.y && r.y + 2 * yMargin > area.y ) {
            rightAreas.add( areaOwner );
          }
        }
      }
    }
    selectClosest( area, rightAreas );
  }

  /**
   * Find an area owner directly to the left of the area
   *
   * @param area
   */
  private void selectItemLeft( Rectangle area ) {
    List<AreaOwner> leftAreas = new ArrayList<>();
    for ( AreaOwner areaOwner : areaOwners ) {
      if ( areaOwner.getOwner() instanceof Item ) {
        // Only keep the items to the left
        //
        Rectangle r = areaOwner.getArea();
        if ( r.x < area.x ) {

          // Select only in the same band of items
          //
          if ( r.y - 2 * yMargin < area.y && r.y + 2 * yMargin > area.y ) {
            leftAreas.add( areaOwner );
          }
        }
      }
    }
    selectClosest( area, leftAreas );
  }

  /**
   * Find an area owner directly to the top of the area
   *
   * @param area
   */
  private void selectItemUp( Rectangle area ) {
    List<AreaOwner> topAreas = new ArrayList<>();
    for ( AreaOwner areaOwner : areaOwners ) {
      if ( areaOwner.getOwner() instanceof Item ) {
        // Only keep the items to the left
        //
        if ( areaOwner.getArea().y < area.y ) {
          topAreas.add( areaOwner );
        }
      }
    }
    selectClosest( area, topAreas );
  }

  private void selectItemDown( Rectangle area ) {
    List<AreaOwner> bottomAreas = new ArrayList<>();
    for ( AreaOwner areaOwner : areaOwners ) {
      if ( areaOwner.getOwner() instanceof Item ) {
        // Only keep the items to the left
        //
        Rectangle r = areaOwner.getArea();
        if ( r.y > area.y + area.height ) {
          bottomAreas.add( areaOwner );
        }
      }
    }
    selectClosest( area, bottomAreas );
  }


  private void updateVerticalBar() {
    ScrollBar verticalBar = wCanvas.getVerticalBar();
    org.eclipse.swt.graphics.Rectangle canvasBounds = wCanvas.getBounds();

    if ( totalContentHeight < canvasBounds.height ) {
      verticalBar.setEnabled( false );
      verticalBar.setVisible( false );
    } else {
      verticalBar.setEnabled( true );
      verticalBar.setVisible( true );

      verticalBar.setMinimum( 0 );
      verticalBar.setMaximum( 100 );

      // How much can we show in percentage?
      // That's the size of the thumb
      //
      int percentage = (int) ( (double) 100 * canvasBounds.height / totalContentHeight );
      verticalBar.setThumb( percentage );
      verticalBar.setPageIncrement( percentage / 2 );
      verticalBar.setIncrement( percentage / 10 );

      // Set the selection as well...
      //
      int selection = Math.max(0, (int)((double)100 * ( heightOffSet - canvasBounds.height) / totalContentHeight));
      verticalBar.setSelection( selection );
    }
  }

  private Item findItem( int x, int y ) {

    for ( AreaOwner areaOwner : areaOwners ) {
      if ( areaOwner.contains( x, y ) ) {
        if ( areaOwner.getOwner() instanceof Item ) {
          return (Item) areaOwner.getOwner();
        }
      }
    }

    return null;
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

  /**
   * Gets focusLost
   *
   * @return value of focusLost
   */
  public boolean isFocusLost() {
    return focusLost;
  }

  /**
   * @param focusLost The focusLost to set
   */
  public void setFocusLost( boolean focusLost ) {
    this.focusLost = focusLost;
  }
}
