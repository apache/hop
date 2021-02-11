/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui.perspective.dataorch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchableCallback;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.TabFolderReorder;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiKeyHandler;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyFileType;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopWorkflowFileType;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePlugin;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.workflow.WorkflowMeta;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabFolder2Adapter;
import org.eclipse.swt.custom.CTabFolderEvent;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;

@HopPerspectivePlugin(
  id = "100-HopDataOrchestrationPerspective",
  name = "Data Orchestration",
  image = "ui/images/data_orch.svg",
  description = "The Hop Data Orchestration Perspective for pipelines and workflows"
)
@GuiPlugin (description="Hop Data Orchestration Perspective GUI" )
public class HopDataOrchestrationPerspective implements IHopPerspective {

  public static final String ID_PERSPECTIVE_TOOLBAR_ITEM = "20010-perspective-data-orchestration";

  private final HopPipelineFileType<PipelineMeta> pipelineFileType;
  private final HopWorkflowFileType<WorkflowMeta> workflowFileType;

  private HopGui hopGui;
  private Composite parent;

  private Composite composite;
  private FormData formData;

  private CTabFolder tabFolder;

  private List<TabItemHandler> items;
  private TabItemHandler activeItem;

  private Stack<Integer> tabSelectionHistory;
  private int tabSelectionIndex;

  public HopDataOrchestrationPerspective() {
    items = new CopyOnWriteArrayList<>();
     
    activeItem = null;
    tabSelectionHistory = new Stack<>();
    tabSelectionIndex = 0;

    pipelineFileType = new HopPipelineFileType<>();
    workflowFileType = new HopWorkflowFileType<>();
  }

  @Override public String getId() {
    return "data-orch";
  }

  @Override public void activate() {
	  hopGui.setActivePerspective(this);
  }

  @Override
  public void perspectiveActivated() {
      HopGui.getInstance().handleFileCapabilities(getActiveFileTypeHandler().getFileType(), false, false);
  }
	
  @Override public boolean isActive() {
    return hopGui.isActivePerspective(this); // composite != null && !composite.isDisposed() && composite.isVisible();;
  }

  @Override public void initialize( HopGui hopGui, Composite parent ) {
    this.hopGui = hopGui;
    this.parent = parent;

    PropsUi props = PropsUi.getInstance();

    composite = new Composite( parent, SWT.NONE );
    //composite.setBackground( GuiResource.getInstance().getColorBackground() );
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
    props.setLook( tabFolder, Props.WIDGET_STYLE_TAB );
    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment( 0, 0 );
    fdLabel.right = new FormAttachment( 100, 0 );
    fdLabel.top = new FormAttachment( 0, 0 );
    fdLabel.bottom = new FormAttachment( 100, 0 );
    tabFolder.setLayoutData( fdLabel );

    tabFolder.addCTabFolder2Listener( new CTabFolder2Adapter() {
      @Override public void close( CTabFolderEvent event ) {
        handleTabCloseEvent( event );
      }
    } );
    tabFolder.addListener( SWT.Selection, event -> handleTabSelectionEvent( event ) );  
    
    
    // Create tab item context menu
    Menu menu = new Menu(tabFolder);
    tabFolder.setMenu(menu);
    tabFolder.addListener( SWT.MenuDetect, event -> handleTabMenuDetectEvent( event ) );

    // Create menu item
    MenuItem miClose = new MenuItem(menu, SWT.NONE);
    miClose.setText("Close");
    miClose.addListener( SWT.Selection, (event) -> {    	
      if ( activeItem!=null ) {    	  
    	  activeItem.getTypeHandler().close();
      }
    });
            
    MenuItem miCloseOthers = new MenuItem(menu, SWT.NONE);
    miCloseOthers.setText("Close Others");
    miCloseOthers.addListener( SWT.Selection, (event) -> {
    	TabItemHandler currentItem = activeItem; 
     	items.forEach((item) -> {
    		if ( !item.equals(currentItem) ) {
    			// FIXME: Works only if you activate the item
   				activeItem = item;
   				item.getTypeHandler().close();
        	}
    	});        
    });
        
    MenuItem miCloseAll = new MenuItem(menu, SWT.NONE);
    miCloseAll.setText("Close All");
    miCloseAll.addListener( SWT.Selection, (event) -> {    	   
    	items.forEach((item) -> {
			// FIXME: Works only if you activate the item
			activeItem = item;
       		item.getTypeHandler().close();
    	});
    });  
        
    // Support reorder tab item
    new TabFolderReorder( tabFolder );

  }

  private void handleTabMenuDetectEvent( Event event ) {  
	  CTabItem tabItem = tabFolder.getSelection();	  
	  if ( tabItem==null ) {
		  event.doit=false;
		  return;
	  }
  }
  
  private void handleTabSelectionEvent( Event event ) {
    CTabItem tabItem = (CTabItem) event.item;
    activeItem = findTabItemHandler( tabItem );
    if ( activeItem != null ) {
      activeItem.getTypeHandler().redraw();
      activeItem.getTypeHandler().updateGui();
    }
    int tabIndex = tabFolder.indexOf( tabItem );
    Integer lastIndex = tabSelectionHistory.isEmpty() ? null : tabSelectionHistory.peek();
    if ( lastIndex == null || lastIndex != tabIndex ) {
      tabSelectionHistory.push( tabIndex );
      tabSelectionIndex = tabSelectionHistory.size() - 1;
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
    if ( tabItemHandler == null ) {
      hopGui.getLog().logError( "Tab item handler not found for tab item " + tabItem.toString() );
      return;
    }
    IHopFileTypeHandler typeHandler = tabItemHandler.getTypeHandler();   
    boolean isRemoved = remove( typeHandler );
    
    // Ignore event if canceled
    if ( ! isRemoved ) {    
    	event.doit = false;
    	return;
    }
    
    // Also switch to the last used tab
    // But first remove all from the selection history
    //
    if ( tabIndex >= 0 ) {
      // Remove the index from the tab selection history
      //
      int historyIndex = tabSelectionHistory.indexOf( tabIndex );
      while ( historyIndex >= 0 ) {
        if ( historyIndex <= tabSelectionIndex ) {
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
      for ( int i = 0; i < tabSelectionHistory.size(); i++ ) {
        Integer index = tabSelectionHistory.get( i );
        if ( previous == null || previous != index ) {
          newHistory.add( index );
        } else {
          if ( tabSelectionIndex >= i ) {
            tabSelectionIndex--;
          }
        }
        previous = index;
      }
      tabSelectionHistory = newHistory;

      // Correct the history taken the removed tab into account
      //
      for ( int i = 0; i < tabSelectionHistory.size(); i++ ) {
        int index = tabSelectionHistory.get( i );
        if ( index > tabIndex ) {
          tabSelectionHistory.set( i, index-- );
        }
      }

      // Select the appropriate tab on the stack
      //
      if ( tabSelectionIndex < 0 ) {
        tabSelectionIndex = 0;
      } else if ( tabSelectionIndex >= tabSelectionHistory.size() ) {
        tabSelectionIndex = tabSelectionHistory.size() - 1;
      }
      if ( !tabSelectionHistory.isEmpty() ) {
        Integer activeIndex = tabSelectionHistory.get( tabSelectionIndex );
        if ( activeIndex < items.size() ) {
          activeItem = items.get( activeIndex );
          tabFolder.setSelection( activeIndex );
          activeItem.getTypeHandler().updateGui();
        }
      }
      
      // If all tab are closed
      //
      if ( tabFolder.getItemCount()==0 ) {    	  
    	HopGui.getInstance().handleFileCapabilities( new EmptyFileType(), false, false );
      }
    }
  }

  public TabItemHandler findTabItemHandler( CTabItem tabItem ) {
    int index = tabFolder.indexOf( tabItem );
    if ( index < 0 || index >= items.size() ) {
      return null;
    }
    return items.get( index );
  }

  public TabItemHandler findTabItemHandler( IHopFileTypeHandler handler ) {
    for ( TabItemHandler item : items ) {
      // This compares the handler payload, typically PipelineMeta, WorkflowMeta and so on.
      if ( item.getTypeHandler().equals( handler ) ) {
        return item;
      }
    }
    return null;
  }

  /**
   * Add a new pipeline tab to the tab folder...
   *
   * @param pipelineMeta
   * @return
   */
  public IHopFileTypeHandler addPipeline( HopGui hopGui, PipelineMeta pipelineMeta, HopPipelineFileType pipelineFile ) throws HopException {
    CTabItem tabItem = new CTabItem( tabFolder, SWT.CLOSE );
    tabItem.setImage( GuiResource.getInstance().getImagePipeline() );
    HopGuiPipelineGraph pipelineGraph = new HopGuiPipelineGraph( tabFolder, hopGui, tabItem, this, pipelineMeta, pipelineFile );
    tabItem.setControl( pipelineGraph );

    // Set the tab name
    //
    updateTabLabel( tabItem, pipelineMeta.getFilename(), pipelineMeta.getName() );

    // Update the internal variables (file specific) in the pipeline graph variables
    //
    pipelineMeta.setInternalHopVariables( pipelineGraph.getVariables() );

    // Update the variables using the list of parameters
    //
    hopGui.setParametersAsVariablesInUI( pipelineMeta, pipelineGraph.getVariables() );

    // Switch to the tab
    tabFolder.setSelection( tabItem );
    activeItem = new TabItemHandler( tabItem, pipelineGraph );
    items.add( activeItem );

    // Remove all the history above the current tabSelectionIndex
    //
    while ( tabSelectionHistory.size() - 1 > tabSelectionIndex ) {
      tabSelectionHistory.pop();
    }
    int tabIndex = tabFolder.indexOf( tabItem );
    tabSelectionHistory.add( tabIndex );
    tabSelectionIndex = tabSelectionHistory.size() - 1;

    try {
      ExtensionPointHandler.callExtensionPoint( hopGui.getLog(), pipelineGraph.getVariables(), HopExtensionPoint.HopGuiNewPipelineTab.id, pipelineGraph );
    } catch ( Exception e ) {
      throw new HopException( "Error calling extension point plugin for plugin id " + HopExtensionPoint.HopGuiNewPipelineTab.id + " trying to handle a new pipeline tab", e );
    }

    pipelineGraph.adjustScrolling();

    pipelineGraph.setFocus();

    return pipelineGraph;
  }

  public void updateTabLabel( CTabItem tabItem, String filename, String name ) {
    if ( !tabItem.isDisposed() ) {
      tabItem.setText( Const.NVL( name, "<>" ) );
      tabItem.setToolTipText( filename );
    }
  }

  /**
   * Add a new workflow tab to the tab folder...
   *
   * @param workflowMeta
   * @return The file type handler
   */
  public IHopFileTypeHandler addWorkflow( HopGui hopGui, WorkflowMeta workflowMeta, HopWorkflowFileType workflowFile ) throws HopException {
    CTabItem tabItem = new CTabItem( tabFolder, SWT.CLOSE );
    tabItem.setImage( GuiResource.getInstance().getImageWorkflow() );
    HopGuiWorkflowGraph workflowGraph = new HopGuiWorkflowGraph( tabFolder, hopGui, tabItem, this, workflowMeta, workflowFile );
    tabItem.setControl( workflowGraph );

    // Update the internal variables (file specific) in the workflow graph variables
    //
    workflowMeta.setInternalHopVariables( workflowGraph.getVariables() );

    // Update the variables using the list of parameters
    //
    hopGui.setParametersAsVariablesInUI( workflowMeta, workflowGraph.getVariables() );

    // Set the tab name
    //
    updateTabLabel( tabItem, workflowMeta.getFilename(), workflowMeta.getName() );

    // Switch to the tab
    tabFolder.setSelection( tabItem );
    activeItem = new TabItemHandler( tabItem, workflowGraph );
    items.add( activeItem );

    // Remove all the history above the current tabSelectionIndex
    //
    while ( tabSelectionHistory.size() - 1 > tabSelectionIndex ) {
      tabSelectionHistory.pop();
    }
    int tabIndex = tabFolder.indexOf( tabItem );
    tabSelectionHistory.add( tabIndex );
    tabSelectionIndex = tabSelectionHistory.size() - 1;

    try {
      ExtensionPointHandler.callExtensionPoint( hopGui.getLog(), workflowGraph.getVariables(), HopExtensionPoint.HopGuiNewWorkflowTab.id, workflowGraph );
    } catch ( Exception e ) {
      throw new HopException( "Error calling extension point plugin for plugin id " + HopExtensionPoint.HopGuiNewWorkflowTab.id + " trying to handle a new workflow tab", e );
    }

    workflowGraph.setFocus();

    return workflowGraph;
  }

  /**
   * Remove the file type handler from this perspective, from the tab folder.
   * This simply tries to remove the item, does not
   *
   * @param typeHandler The file type handler to remove
   * @return true if the handler was removed from the perspective, false if it wasn't (cancelled, not possible, ...)
   */
  @Override public boolean remove( IHopFileTypeHandler typeHandler ) {
    TabItemHandler tabItemHandler = findTabItemHandler( typeHandler );
    if ( tabItemHandler == null ) {
      return false;
    }
    if ( typeHandler.isCloseable() ) {
      // Remove the tab item handler from the list
      // Then close the tab item...
      //
      items.remove( tabItemHandler );
      CTabItem tabItem = tabItemHandler.getTabItem();
      tabItem.dispose();

      // Also remove the keyboard shortcuts for this handler
      //
      HopGuiKeyHandler.getInstance().removeParentObjectToHandle( typeHandler );
      hopGui.getMainHopGuiComposite().setFocus();

      if ( typeHandler.getSubject() != null ) {
        if ( typeHandler.getSubject() instanceof PipelineMeta ) {
          try {
            ExtensionPointHandler.callExtensionPoint( hopGui.getLog(), hopGui.getVariables(), HopExtensionPoint.HopGuiPipelineAfterClose.id, typeHandler.getSubject() );
          } catch ( Exception e ) {
            hopGui.getLog().logError( "Error calling extension point 'HopGuiPipelineAfterClose'", e );
          }
        } else if ( typeHandler.getSubject() instanceof WorkflowMeta ) {
          try {
            ExtensionPointHandler.callExtensionPoint( hopGui.getLog(), hopGui.getVariables(), HopExtensionPoint.HopGuiWorkflowAfterClose.id, typeHandler.getSubject() );
          } catch ( Exception e ) {
            hopGui.getLog().logError( "Error calling extension point 'HopGuiWorkflowAfterClose'", e );
          }
        }
      }

      return true;
    }
    return false;
  }

  /**
   * Get the active file type handler.  If none is active return an empty handler which does do anything.
   *
   * @return the active file type handler or if none is active return an empty handler which does do anything.
   */
  @Override public IHopFileTypeHandler getActiveFileTypeHandler() {
    if ( activeItem == null ) {
      return new EmptyHopFileTypeHandler();
    }
    return activeItem.getTypeHandler();
  }

  @Override public void setActiveFileTypeHandler( IHopFileTypeHandler activeFileTypeHandler ) {
    TabItemHandler tabItemHandler = findTabItemHandler( activeFileTypeHandler );
    if ( tabItemHandler == null ) {
      return; // Can't find the file
    }
    // Select the tab
    //
    switchToTab( tabItemHandler );
  }

  public void switchToTab( TabItemHandler tabItemHandler ) {
    tabFolder.setSelection( tabItemHandler.getTabItem() );
    tabFolder.showItem( tabItemHandler.getTabItem() );
    tabFolder.setFocus();
    activeItem = tabItemHandler;
  }

  public List<IHopFileType> getSupportedHopFileTypes() {
    return Arrays.asList( pipelineFileType, workflowFileType );
  }

  @Override public void navigateToPreviousFile() {
    if ( hasNavigationPreviousFile() ) {
      tabSelectionIndex--;
      Integer tabIndex = tabSelectionHistory.get( tabSelectionIndex );
      activeItem = items.get( tabIndex );
      tabFolder.setSelection( tabIndex );
      activeItem.getTypeHandler().updateGui();
    }
  }

  @Override public void navigateToNextFile() {
    if ( hasNavigationNextFile() ) {
      tabSelectionIndex++;
      Integer tabIndex = tabSelectionHistory.get( tabSelectionIndex );
      activeItem = items.get( tabIndex );
      tabFolder.setSelection( tabIndex );
      activeItem.getTypeHandler().updateGui();
    }
  }

  @Override
  public boolean hasNavigationPreviousFile() {
    return tabSelectionIndex > 0 && tabSelectionIndex < tabSelectionHistory.size();
  }

  @Override
  public boolean hasNavigationNextFile() {
    return tabSelectionIndex + 1 < tabSelectionHistory.size();
  }

  /**
   * Get the currently active context handlers in the perspective...
   *
   * @return
   */
  public List<IGuiContextHandler> getContextHandlers() {
    List<IGuiContextHandler> handlers = new ArrayList<>();
    // For every file type we have a context handler...
    //
    for ( IHopFileType fileType : getSupportedHopFileTypes() ) {
      handlers.addAll( fileType.getContextHandlers() );
    }
    return handlers;
  }

  /**
   * Update all the tab labels...
   */
  public void updateTabs() {
    for ( TabItemHandler item : items ) {
      IHopFileTypeHandler typeHandler = item.getTypeHandler();
      updateTabLabel( item.getTabItem(), typeHandler.getFilename(), typeHandler.getName() );
    }
  }

  public TabItemHandler findTabItemHandlerWithFilename( String filename ) {
    if ( filename == null ) {
      return null;
    }
    for ( TabItemHandler item : items ) {
      if ( filename.equals(item.getTypeHandler().getFilename()) ) {
        return item;
      }
    }
    return null;
  }

  @Override public List<ISearchable> getSearchables() {
    List<ISearchable> searchables = new ArrayList<>();
    for (final TabItemHandler item : items) {
      // The type handler is the pipeline or workflow
      //
      IHopFileTypeHandler typeHandler = item.getTypeHandler();
      searchables.add( new ISearchable() {
        @Override public String getLocation() {
          return "Data orchestration perspective in tab : "+item.getTabItem().getText();
        }

        @Override public String getName() {
          return typeHandler.getName();
        }

        @Override public String getType() {
          return typeHandler.getFileType().getName();
        }

        @Override public String getFilename() {
          return typeHandler.getFilename();
        }

        @Override public Object getSearchableObject() {
          return typeHandler.getSubject();
        }

        @Override public ISearchableCallback getSearchCallback() {
          return ( searchable, searchResult ) -> {
            activate();
            switchToTab( item );
          };
        }
      } );
    }
    return searchables;
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

  @Override public Control getControl() {
    return composite;
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

  /**
   * Gets pipelineFileType
   *
   * @return value of pipelineFileType
   */
  public HopPipelineFileType<PipelineMeta> getPipelineFileType() {
    return pipelineFileType;
  }

  /**
   * Gets jobFileType
   *
   * @return value of jobFileType
   */
  public HopWorkflowFileType<WorkflowMeta> getWorkflowFileType() {
    return workflowFileType;
  }
}
