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

package org.apache.hop.ui.core.metadata;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

/** Abstract implementation of all metadata editors. */
public abstract class MetadataEditor<T extends IHopMetadata> extends MetadataFileTypeHandler
    implements IMetadataEditor {

  private static final Class<?> PKG = MetadataEditorDialog.class; // For Translator

  protected HopGui hopGui;
  protected MetadataManager<T> manager;
  protected T metadata;

  protected String title;
  protected String toolTip;
  protected Image titleImage;
  protected Image image;
  protected boolean isChanged = false;
  protected String originalName;

  public MetadataEditor(HopGui hopGui, MetadataManager<T> manager, T metadata) {
    this.hopGui = hopGui;
    this.manager = manager;
    this.metadata = metadata;
    this.originalName = metadata.getName();

    // Search metadata annotation
    Type superclass = getClass().getGenericSuperclass();
    ParameterizedType parameterized = (ParameterizedType) superclass;
    Class<?> managedClass = (Class<?>) parameterized.getActualTypeArguments()[0];
    HopMetadata annotation = managedClass.getAnnotation(HopMetadata.class);

    // Initialize editor
    this.setTitle(metadata.getName());

    String titleToolTip = annotation.name();
    if (StringUtils.isNotEmpty(metadata.getMetadataProviderName())) {
      titleToolTip+= Const.CR+"Source: "+metadata.getMetadataProviderName();
    }
    this.setTitleToolTip(titleToolTip);

    this.setTitleImage(
        GuiResource.getInstance()
            .getImage(
                annotation.image(),
                managedClass.getClassLoader(),
                ConstUi.SMALL_ICON_SIZE,
                ConstUi.SMALL_ICON_SIZE));

    // Use SwtSvgImageUtil because GuiResource cache have small icon.
    this.setImage(
        SwtSvgImageUtil.getImage(
            hopGui.getDisplay(),
            managedClass.getClassLoader(),
            annotation.image(),
            ConstUi.LARGE_ICON_SIZE,
            ConstUi.LARGE_ICON_SIZE));
  }

  public Button[] createButtonsForButtonBar(final Composite parent) {
    return null;
  }

  public HopGui getHopGui() {
    return hopGui;
  }

  public MetadataManager<T> getMetadataManager() {
    return manager;
  }

  public T getMetadata() {
    return metadata;
  }

  public Shell getShell() {
    return hopGui.getShell();
  }

  public Image getImage() {
    return image;
  }

  protected void setImage(Image image) {
    this.image = image;
  }

  @Override
  public String getTitle() {
    return title;
  }

  protected void setTitle(String title) {
    this.title = title;
  }

  @Override
  public Image getTitleImage() {
    return titleImage;
  }

  protected void setTitleImage(Image image) {
    this.titleImage = image;
  }

  @Override
  public String getTitleToolTip() {
    return toolTip;
  }

  protected void setTitleToolTip(String toolTip) {
    this.toolTip = toolTip;
  }

  @Override
  public boolean isChanged() {
    return isChanged;
  }

  protected void resetChanged() {
    this.isChanged = false;
  }

  protected void setChanged() {
    if (this.isChanged == false) {
      this.isChanged = true;
      MetadataPerspective.getInstance().updateEditor(this);
    }
  }

  /** Inline usage: copy information from the metadata onto the various widgets */
  public abstract void setWidgetsContent();

  /**
   * Inline usage: Reads the information or state of the various widgets and modifies the provided
   * metadata object.
   *
   * @param meta The metadata object to populate from the widgets
   */
  public abstract void getWidgetsContent(T meta);

  @Override
  public boolean isCloseable() {

    // Check if the metadata is saved. If not, ask for it to be saved.
    //
    if (isChanged()) {

      MessageBox messageDialog =
          new MessageBox(getShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO | SWT.CANCEL);
      messageDialog.setText(manager.getManagedName());
      messageDialog.setMessage(
          BaseMessages.getString(
              PKG, "MetadataEditor.WantToSaveBeforeClosing.Message", getTitle()));

      int answer = messageDialog.open();

      if ((answer & SWT.YES) != 0) {
        try {
          save();
        } catch (Exception e) {
          new ErrorDialog(getShell(), "Error", "Error preparing editor close", e);
          return false;
        }
      }

      if ((answer & SWT.CANCEL) != 0) {
        return false;
      }
    }

    return true;
  }

  @Override
  public void save() throws HopException {

    getWidgetsContent(metadata);
    String name = metadata.getName();

    boolean isCreated = false;
    boolean isRename = false;

    if (StringUtils.isEmpty(name)) {
      throw new HopException(BaseMessages.getString(PKG, "MetadataEditor.Error.NoName"));
    }

    if (StringUtils.isEmpty(originalName)) {
      isCreated = true;
    }
    // If rename
    //
    else if (!originalName.equals(name)) {

      // See if the name collides with an existing one...
      //
      IHopMetadataSerializer<T> serializer = manager.getSerializer();

      if (serializer.exists(name)) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "MetadataEditor.Error.NameAlreadyExists", name));
      } else {
        isRename = true;
      }
    }

    // Save it in the metadata
    manager.getSerializer().save(metadata);

    if (isCreated)
      ExtensionPointHandler.callExtensionPoint(
          hopGui.getLog(), manager.getVariables(), HopExtensionPoint.HopGuiMetadataObjectCreated.id, metadata );
    else
      ExtensionPointHandler.callExtensionPoint(
          hopGui.getLog(), manager.getVariables(), HopExtensionPoint.HopGuiMetadataObjectUpdated.id, metadata );

    // Reset changed flag
    this.isChanged = false;
    this.title = metadata.getName();

    if (isRename) {
      manager.getSerializer().delete(originalName);
      this.originalName = metadata.getName();
    }

    MetadataPerspective.getInstance().updateEditor(this);
  }

  @Override
  public void saveAs(String filename) throws HopException {
    throw new HopException("Metadata editor doesn't support saveAs");
  }

  @Override
  public boolean setFocus() {
    return true;
  }

  @Override
  public void dispose() {}
}
