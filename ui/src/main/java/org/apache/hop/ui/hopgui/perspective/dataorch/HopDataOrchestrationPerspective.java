package org.apache.hop.ui.hopgui.perspective.dataorch;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiToolbarElement;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.widget.TabFolderReorder;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiKeyHandler;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.HopFileTypeHandlerInterface;
import org.apache.hop.ui.hopgui.file.trans.HopGuiTransGraph;
import org.apache.hop.ui.hopgui.file.trans.HopTransFileType;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePlugin;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabFolder2Adapter;
import org.eclipse.swt.custom.CTabFolderEvent;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

@HopPerspectivePlugin(
  id = "HopDataOrchestrationPerspective",
  name = "Data Orchestration",
  description = "The Hop Data Orchestration Perspective for pipelines and workflows"
)
@GuiPlugin(
  id = "GuiPlugin-HopDataOrchestrationPerspective"
)
public class HopDataOrchestrationPerspective implements IHopPerspective {

  public static final String STRING_NEW_TRANSFORMATION_PREFIX = "Transformation";

  private static HopDataOrchestrationPerspective perspective;
  private HopGui hopGui;
  private Composite parent;

  private Composite composite;
  private FormData formData;

  private CTabFolder tabFolder;

  private List<TabItemHandler> items;
  private TabItemHandler activeItem;

  private Stack<Integer> tabSelectionHistory;
  private int tabSelectionIndex;

  public static final HopDataOrchestrationPerspective getInstance() {
    if ( perspective == null ) {
      perspective = new HopDataOrchestrationPerspective();
    }
    return perspective;
  }

  private HopDataOrchestrationPerspective() {
    items = new ArrayList<>();
    activeItem = null;
    tabSelectionHistory = new Stack<>();
    tabSelectionIndex = 0;
  }

  @GuiToolbarElement(
    id = "20010-perspective-data-orchestration", type = GuiElementType.TOOLBAR_BUTTON,
    image = "ui/images/transformation.svg", toolTip = "Data Orchestration", parentId = HopGui.GUI_PLUGIN_PERSPECTIVES_PARENT_ID,
    parent = HopGui.GUI_PLUGIN_PERSPECTIVES_PARENT_ID
  )
  public void activate() {
    hopGui.getPerspectiveManager().showPerspective( this.getClass() );
  }

  @Override public void show() {
    composite.setVisible( true );
  }

  @Override public void hide() {
    composite.setVisible( false );
  }

  @Override public boolean isActive() {
    return composite != null && !composite.isDisposed() && composite.isVisible();
  }

  @Override public void initialize( HopGui hopGui, Composite parent ) {
    this.hopGui = hopGui;
    this.parent = parent;

    PropsUI props = PropsUI.getInstance();

    composite = new Composite( parent, SWT.NONE );
    //composite.setBackground( GUIResource.getInstance().getColorBackground() );
    FormLayout layout = new FormLayout();
    //layout.marginLeft = props.getMargin();
    //layout.marginTop = props.getMargin();
    layout.marginRight = props.getMargin();
    layout.marginBottom = props.getMargin();
    composite.setLayout( layout );

    formData = new FormData();
    formData.left = new FormAttachment( 0, 0 );
    formData.top = new FormAttachment( 0, 0 );
    formData.right = new FormAttachment( 100, 0 );
    formData.bottom = new FormAttachment( 100, 0 );
    composite.setLayoutData( formData );

    // A tab folder covers the complete area...
    //
    tabFolder = new CTabFolder( composite, SWT.MULTI | SWT.BORDER );
    //tabFolder.setSimple( false );
    //tabFolder.setBackground( GUIResource.getInstance().getColorBackground() );
    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment( 0, 0 );
    fdLabel.right = new FormAttachment( 100, 0 );
    fdLabel.top = new FormAttachment( 0, 0 );
    fdLabel.bottom = new FormAttachment( 100, 0 );
    tabFolder.setLayoutData( fdLabel );

    tabFolder.addCTabFolder2Listener( new CTabFolder2Adapter() {
      @Override public void close( CTabFolderEvent event ) {
        handleTabCloseEvent(event);
      }
    });
    tabFolder.addListener( SWT.Selection, event-> handTabSelectionEvent(event) );

    // Support reorder tab item
    new TabFolderReorder(tabFolder);

  }

  private void handTabSelectionEvent( Event event ) {
    CTabItem tabItem = (CTabItem) event.item;
    activeItem = findTabItemHandler( tabItem );
    if (activeItem!=null) {
      activeItem.getTypeHandler().redraw();
      activeItem.getTypeHandler().updateGui();
    }
    int tabIndex = tabFolder.indexOf( tabItem );
    Integer lastIndex = tabSelectionHistory.isEmpty() ? null : tabSelectionHistory.peek();
    if ( lastIndex == null || lastIndex != tabIndex ) {
      tabSelectionHistory.push( tabIndex );
      tabSelectionIndex = tabSelectionHistory.size()-1;
    }
  }

  private void handleTabCloseEvent( CTabFolderEvent event ) {
    // A tab is closed.  We need to handle this gracefully.
    // - Look up which tab it is
    // - Look up which file it contains
    // - Save the file if it was changed
    // - Remove the tab and file from the list
    //
    CTabItem tabItem = (CTabItem) event.item;
    int tabIndex = tabFolder.indexOf( tabItem );
    TabItemHandler tabItemHandler = findTabItemHandler( tabItem );
    if (tabItemHandler==null) {
      hopGui.getLog().logError( "Tab item handler not found for tab item "+tabItem.toString() );
      return;
    }
    HopFileTypeHandlerInterface typeHandler = tabItemHandler.getTypeHandler();
    remove(typeHandler);

    // Also switch to the last used tab
    // But first remove all from the selection history
    //
    if (tabIndex>=0) {
      // Remove the index from the tab selection history
      //
      int historyIndex = tabSelectionHistory.indexOf( tabIndex );
      while ( historyIndex>=0 ) {
        if (historyIndex<=tabSelectionIndex) {
          tabSelectionIndex--;
        }
        tabSelectionHistory.remove( historyIndex );

        // Search again
        historyIndex = tabSelectionHistory.indexOf( tabIndex );
      }

      // Compress the history: 2 the same files visited after each other become one.
      //
      Stack<Integer> newHistory = new Stack<>();
      Integer previous = null;
      for (int i=0;i<tabSelectionHistory.size();i++ ) {
        Integer index = tabSelectionHistory.get(i);
        if (previous==null || previous!=index) {
          newHistory.add(index);
        } else {
          if (tabSelectionIndex>=i) {
            tabSelectionIndex--;
          }
        }
        previous=index;
      }
      tabSelectionHistory=newHistory;

      // Correct the history taken the removed tab into account
      //
      for ( int i=0;i<tabSelectionHistory.size();i++) {
        int index = tabSelectionHistory.get( i );
        if (index>tabIndex) {
          tabSelectionHistory.set( i, index-- );
        }
      }

      // Select the appropriate tab on the stack
      //
      if ( tabSelectionIndex<0) {
        tabSelectionIndex=0;
      } else if (tabSelectionIndex>=tabSelectionHistory.size() ) {
        tabSelectionIndex=tabSelectionHistory.size()-1;
      }
      if (!tabSelectionHistory.isEmpty()) {
        Integer activeIndex = tabSelectionHistory.get( tabSelectionIndex );
        if (activeIndex<items.size()) {
          activeItem = items.get( activeIndex );
          tabFolder.setSelection( activeIndex );
          activeItem.getTypeHandler().updateGui();
        }
      }
    }
  }

  public TabItemHandler findTabItemHandler(CTabItem tabItem) {
    int index = tabFolder.indexOf( tabItem );
    if (index<0 || index>=items.size()) {
      return null;
    }
    return items.get(index);
  }

  public TabItemHandler findTabItemHandler(HopFileTypeHandlerInterface handler) {
    for (TabItemHandler item : items) {
      // This compares the handler payload, typically TransMeta, JobMeta and so on.
      if (item.getTypeHandler().equals( handler )) {
        return item;
      }
    }
    return null;
  }

  /**
   * Add a new transformation tab to the tab folder...
   *
   * @param transMeta
   * @return
   */
  public HopFileTypeHandlerInterface addTransformation( Composite parent, HopGui hopGui, TransMeta transMeta, HopTransFileType transFile ) throws HopException {
    CTabItem tabItem = new CTabItem( tabFolder, SWT.CLOSE );
    tabItem.setImage( GUIResource.getInstance().getImageToolbarTrans() );
    HopGuiTransGraph transGraph = new HopGuiTransGraph( tabFolder, hopGui, tabItem, this, transMeta, transFile );
    tabItem.setControl( transGraph );

    // Set the tab name
    //
    tabItem.setText( Const.NVL( transGraph.buildTabName(), "" ) );

    // Switch to the tab
    tabFolder.setSelection( tabItem );
    activeItem = new TabItemHandler(tabItem, transGraph);
    items.add( activeItem );

    // Remove all the history above the current tabSelectionIndex
    //
    while (tabSelectionHistory.size()-1>tabSelectionIndex) {
      tabSelectionHistory.pop();
    }
    int tabIndex = tabFolder.indexOf( tabItem );
    tabSelectionHistory.add( tabIndex );
    tabSelectionIndex = tabSelectionHistory.size()-1;

    try {
      ExtensionPointHandler.callExtensionPoint( hopGui.getLog(), HopExtensionPoint.HopGuiNewTransformationTab.id, transGraph );
    } catch(Exception e) {
      throw new HopException( "Error calling extension point plugin for plugin id "+HopExtensionPoint.HopGuiNewTransformationTab.id+" trying to handle a new transformation tab", e );
    }

    transGraph.setFocus();

    return transGraph;
  }

  /**
   * Remove the file type handler from this perspective, from the tab folder.
   * This simply tries to remove the item, does not
   * @param typeHandler The file type handler to remove
   * @return true if the handler was removed from the perspective, false if it wasn't (cancelled, not possible, ...)
   */
  @Override public boolean remove( HopFileTypeHandlerInterface typeHandler ) {
    TabItemHandler tabItemHandler = findTabItemHandler( typeHandler );
    if (tabItemHandler==null) {
      return false;
    }
    if (typeHandler.isCloseable()) {
      // Remove the tab item handler from the list
      // Then close the tab item...
      //
      items.remove( tabItemHandler );
      CTabItem tabItem = tabItemHandler.getTabItem();
      tabItem.dispose();

      // Also remove the keyboard shortcuts for this handler
      //
      HopGuiKeyHandler.getInstance().removeParentObjectToHandle(typeHandler);
      hopGui.getMainHopGuiComposite().setFocus();

      return true;
    }
    return false;
  }

  /**
   * Get the active file type handler.  If none is active return an empty handler which does do anything.
   * @return the active file type handler or if none is active return an empty handler which does do anything.
   */
  @Override public HopFileTypeHandlerInterface getActiveFileTypeHandler() {
    if (activeItem==null) {
      return new EmptyHopFileTypeHandler();
    }
    return activeItem.getTypeHandler();
  }

  @Override public void navigateToPreviousFile() {
    if (hasNavigationPreviousFile()) {
      tabSelectionIndex--;
      Integer tabIndex = tabSelectionHistory.get( tabSelectionIndex );
      activeItem = items.get( tabIndex );
      tabFolder.setSelection( tabIndex );
      activeItem.getTypeHandler().updateGui();
    }
  }

  @Override public void navigateToNextFile() {
    if (hasNavigationNextFile()) {
      tabSelectionIndex++;
      Integer tabIndex = tabSelectionHistory.get( tabSelectionIndex );
      activeItem = items.get( tabIndex );
      tabFolder.setSelection( tabIndex );
      activeItem.getTypeHandler().updateGui();
    }
  }

  @Override
  public boolean hasNavigationPreviousFile() {
    return tabSelectionIndex>0 && tabSelectionIndex<tabSelectionHistory.size();
  }

  @Override
  public boolean hasNavigationNextFile() {
    return tabSelectionIndex+1 < tabSelectionHistory.size();
  }

  /**
   * Get the currently active context handlers in the perspective...
   * @return
   */
  public List<IGuiContextHandler> getContextHandlers() {
    List<IGuiContextHandler> handlers = new ArrayList<>();
    // For every file type we have a context handler...
    //
    HopFileTypeHandlerInterface fileTypeHandler = getActiveFileTypeHandler();
    handlers.addAll( fileTypeHandler.getContextHandlers() );
    return handlers;
  }


  /**
   * Gets items
   *
   * @return value of items
   */
  public List<TabItemHandler> getItems() {
    return items;
  }

  /**
   * @param items The items to set
   */
  public void setItems( List<TabItemHandler> items ) {
    this.items = items;
  }

  /**
   * Gets activeItem
   *
   * @return value of activeItem
   */
  public TabItemHandler getActiveItem() {
    return activeItem;
  }

  /**
   * @param activeItem The activeItem to set
   */
  public void setActiveItem( TabItemHandler activeItem ) {
    this.activeItem = activeItem;
  }

  /**
   * Gets hopGui
   *
   * @return value of hopGui
   */
  public HopGui getHopGui() {
    return hopGui;
  }

  /**
   * @param hopGui The hopGui to set
   */
  public void setHopGui( HopGui hopGui ) {
    this.hopGui = hopGui;
  }

  /**
   * Gets parent
   *
   * @return value of parent
   */
  public Composite getParent() {
    return parent;
  }

  /**
   * @param parent The parent to set
   */
  public void setParent( Composite parent ) {
    this.parent = parent;
  }

  /**
   * Gets composite
   *
   * @return value of composite
   */
  @Override public Composite getComposite() {
    return composite;
  }

  /**
   * @param composite The composite to set
   */
  public void setComposite( Composite composite ) {
    this.composite = composite;
  }

  /**
   * Gets formData
   *
   * @return value of formData
   */
  @Override public FormData getFormData() {
    return formData;
  }

  /**
   * @param formData The formData to set
   */
  public void setFormData( FormData formData ) {
    this.formData = formData;
  }

  /**
   * Gets tabFolder
   *
   * @return value of tabFolder
   */
  public CTabFolder getTabFolder() {
    return tabFolder;
  }

  /**
   * @param tabFolder The tabFolder to set
   */
  public void setTabFolder( CTabFolder tabFolder ) {
    this.tabFolder = tabFolder;
  }
}
