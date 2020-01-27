package org.apache.hop.ui.hopgui.perspective.dataorch;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiToolbarElement;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.hopgui.HopGui;
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
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;

import java.util.ArrayList;
import java.util.List;

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

  public static final HopDataOrchestrationPerspective getInstance() {
    if ( perspective == null ) {
      perspective = new HopDataOrchestrationPerspective();
    }
    return perspective;
  }

  private HopDataOrchestrationPerspective() {
    items = new ArrayList<>();
    activeItem = null;
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

    composite = new Composite( parent, SWT.NONE );
    composite.setBackground( GUIResource.getInstance().getColorBackground() );
    FormLayout layout = new FormLayout();
    layout.marginLeft = Const.MARGIN;
    layout.marginTop = Const.MARGIN;
    layout.marginLeft = Const.MARGIN;
    layout.marginBottom = Const.MARGIN;
    composite.setLayout( layout );

    formData = new FormData();
    formData.left = new FormAttachment( 0, 0 );
    formData.top = new FormAttachment( 0, 0 );
    formData.right = new FormAttachment( 100, 0 );
    formData.bottom = new FormAttachment( 100, 0 );
    composite.setLayoutData( formData );

    // A tab folder covers the complete area...
    //
    tabFolder = new CTabFolder( composite, SWT.NONE );
    tabFolder.setBackground( GUIResource.getInstance().getColorBackground() );
    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment( 0, 0 );
    fdLabel.right = new FormAttachment( 100, 0 );
    fdLabel.top = new FormAttachment( 0, 0 );
    fdLabel.bottom = new FormAttachment( 100, 0 );
    tabFolder.setLayoutData( fdLabel );

    tabFolder.addCTabFolder2Listener( new CTabFolder2Adapter() {
      @Override public void close( CTabFolderEvent event ) {
        // A tab is closed.  We need to handle this gracefully.
        // - Look up which tab it is
        // - Look up which file it contains
        // - Save the file if it was changed
        // - Remove the tab and file from the list
        //
        CTabItem tabItem = (CTabItem) event.item;
        TabItemHandler tabItemHandler = findTabItemHandler( tabItem );
        if (tabItemHandler==null) {
          hopGui.getLog().logError( "Tab item handler not found for tab item "+tabItem.toString() );
          return;
        }
        HopFileTypeHandlerInterface typeHandler = tabItemHandler.getTypeHandler();

        // Don't close the tab if we had an error saving the file
        //
        if (typeHandler.isCloseable()) {
          items.remove( tabItemHandler );
        } else {
          event.doit = false;
        }
      }
    });

    tabFolder.addListener( SWT.Selection, e->{
      activeItem = findTabItemHandler( (CTabItem) e.item );
      if (activeItem!=null) {
        activeItem.getTypeHandler().redraw();
        activeItem.getTypeHandler().updateGui();
      }
    } );

  }

  public TabItemHandler findTabItemHandler(CTabItem tabItem) {
    int index = tabFolder.indexOf( tabItem );
    if (index<0) {
      return null;
    }
    return items.get(index);
  }

  /**
   * Add a new transformation tab to the tab folder...
   *
   * @param transMeta
   * @return
   */
  public HopFileTypeHandlerInterface addTransformation( Composite parent, HopGui hopGui, TransMeta transMeta, HopTransFileType transFile ) throws HopException {
    CTabItem tabItem = new CTabItem( tabFolder, SWT.CLOSE );
    HopGuiTransGraph transGraph = new HopGuiTransGraph( tabFolder, hopGui, tabItem, transMeta, transFile );
    tabItem.setControl( transGraph );

    // Set the tab name
    //
    tabItem.setText( Const.NVL( transGraph.buildTabName(), "" ) );

    // Switch to the tab
    tabFolder.setSelection( tabItem );
    activeItem = new TabItemHandler(tabItem, transGraph);
    items.add( activeItem );

    try {
      ExtensionPointHandler.callExtensionPoint( hopGui.getLog(), HopExtensionPoint.HopGuiNewTransformationTab.id, transGraph );
    } catch(Exception e) {
      throw new HopException( "Error calling extension point plugin for plugin id "+HopExtensionPoint.HopGuiNewTransformationTab.id+" trying to handle a new transformation tab", e );
    }

    return transGraph;
  }

  @Override public HopFileTypeHandlerInterface getActiveFileTypeHandler() {
    if (activeItem==null) {
      return null;
    }
    return activeItem.getTypeHandler();
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
