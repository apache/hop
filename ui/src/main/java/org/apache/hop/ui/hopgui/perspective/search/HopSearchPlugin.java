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

package org.apache.hop.ui.hopgui.perspective.search;

import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiToolbarElement;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePlugin;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@HopPerspectivePlugin(
  id = "HopSearchPerspective",
  name = "Search",
  description = "The Hop Search Perspective"
)
@GuiPlugin
public class HopSearchPlugin implements IHopPerspective {

  public static final String ID_PERSPECTIVE_TOOLBAR_ITEM = "20020-perspective-search";

  private static HopSearchPlugin perspective;

  private HopGui hopGui;
  private Composite parent;
  private Composite composite;
  private FormData formData;

  public static final HopSearchPlugin getInstance() {
    if ( perspective == null ) {
      perspective = new HopSearchPlugin();
    }
    return perspective;
  }

  private HopSearchPlugin() {
  }

  @Override public String getId() {
    return "search";
  }

  @GuiToolbarElement(
    id = ID_PERSPECTIVE_TOOLBAR_ITEM,
    type = GuiElementType.TOOLBAR_BUTTON,
    image = "ui/images/search.svg",
    toolTip = "Search", parentId = HopGui.GUI_PLUGIN_PERSPECTIVES_PARENT_ID,
    parent = HopGui.GUI_PLUGIN_PERSPECTIVES_PARENT_ID
  )
  public void activate() {
    hopGui.getPerspectiveManager().showPerspective( this.getClass() );
  }

  @Override public IHopFileTypeHandler getActiveFileTypeHandler() {
    return null; // Not handling anything really
  }

  @Override public List<IHopFileType> getSupportedHopFileTypes() {
    return Collections.emptyList();
  }

  @Override
  public void show() {
    composite.setVisible( true );
    hopGui.getPerspectivesToolbarWidgets().findToolItem( ID_PERSPECTIVE_TOOLBAR_ITEM ).setImage( GuiResource.getInstance().getImageToolbarSearch() );
  }

  @Override
  public void hide() {
    composite.setVisible( false );
    hopGui.getPerspectivesToolbarWidgets().findToolItem( ID_PERSPECTIVE_TOOLBAR_ITEM ).setImage( GuiResource.getInstance().getImageToolbarSearchInactive() );
  }

  @Override
  public boolean isActive() {
    return composite != null && !composite.isDisposed() && composite.isVisible();
  }

  @Override public void initialize( HopGui hopGui, Composite parent ) {
    this.hopGui = hopGui;
    this.parent = parent;

    PropsUi props = PropsUi.getInstance();

    composite = new Composite( parent, SWT.NONE );
    composite.setBackground( GuiResource.getInstance().getColorBlueCustomGrid() );
    FormLayout layout = new FormLayout();
    layout.marginLeft = props.getMargin();
    layout.marginTop = props.getMargin();
    layout.marginLeft = props.getMargin();
    layout.marginBottom = props.getMargin();
    composite.setLayout( layout );

    formData = new FormData();
    formData.left = new FormAttachment( 0, 0 );
    formData.top = new FormAttachment( 0, 0 );
    formData.right = new FormAttachment( 100, 0 );
    formData.bottom = new FormAttachment( 100, 0 );
    composite.setLayoutData( formData );

    // Add a simple label to test
    //
    Label label = new Label( composite, SWT.LEFT );
    label.setBackground( GuiResource.getInstance().getColorBlueCustomGrid() );
    label.setText( "The Search perspective" );
    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment( 0, 0 );
    fdLabel.right = new FormAttachment( 100, 0 );
    fdLabel.top = new FormAttachment( 0, 0 );
    label.setLayoutData( fdLabel );
  }

  @Override public boolean remove( IHopFileTypeHandler typeHandler ) {
    return false; // Nothing to do here
  }

  @Override public List<TabItemHandler> getItems() {
    return null;
  }

  @Override public void navigateToPreviousFile() {

  }

  @Override public void navigateToNextFile() {

  }

  @Override public boolean hasNavigationPreviousFile() {
    return false;
  }

  @Override public boolean hasNavigationNextFile() {
    return false;
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

  @Override public List<IGuiContextHandler> getContextHandlers() {
    List<IGuiContextHandler> handlers = new ArrayList<>();
    return handlers;
  }
}
