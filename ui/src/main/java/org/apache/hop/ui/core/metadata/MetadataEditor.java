/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Objects;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.TranslateUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.bus.HopGuiEvents;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.apache.hop.ui.util.HelpUtils;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Shell;

/** Abstract implementation of all metadata editors. */
public abstract class MetadataEditor<T extends IHopMetadata> extends MetadataFileTypeHandler
    implements IMetadataEditor<T> {

  private static final Class<?> PKG = MetadataEditorDialog.class;

  @Getter protected HopGui hopGui;
  protected MetadataManager<T> manager;
  protected T metadata;

  protected String title;
  protected String toolTip;
  protected Image titleImage;
  @Getter protected Image image;
  protected boolean isChanged = false;
  protected String originalName;

  public MetadataEditor(HopGui hopGui, MetadataManager<T> manager, T metadata) {
    super(metadata);
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
    this.toolTip = TranslateUtil.translate(annotation.name(), managedClass);
    this.title = Utils.isEmpty(originalName) ? toolTip : originalName;
    if (StringUtils.isNotEmpty(metadata.getMetadataProviderName())) {
      toolTip += Const.CR + "Source: " + metadata.getMetadataProviderName();
    }

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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetadataEditor<?> that = (MetadataEditor<?>) o;
    return Objects.equals(metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        hopGui, manager, metadata, title, toolTip, titleImage, image, isChanged, originalName);
  }

  public Button[] createButtonsForButtonBar(final Composite parent) {
    return null;
  }

  protected Button createHelpButton(final Shell shell) {
    HopMetadata annotation = manager.getManagedClass().getAnnotation(HopMetadata.class);
    IPlugin plugin =
        PluginRegistry.getInstance().getPlugin(MetadataPluginType.class, annotation.key());
    return HelpUtils.createHelpButton(shell, plugin);
  }

  public MetadataManager<T> getMetadataManager() {
    return manager;
  }

  @Override
  public T getMetadata() {
    return metadata;
  }

  @Override
  public void setMetadata(T metadata) {
    this.metadata = metadata;
  }

  public Shell getShell() {
    return hopGui.getShell();
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
  public boolean hasChanged() {
    return isChanged;
  }

  @Override
  public void resetChanged() {
    this.isChanged = false;
  }

  @Override
  public void setChanged() {
    if (!this.isChanged) {
      this.isChanged = true;
      MetadataPerspective.getInstance().updateEditor(this);
      try {
        hopGui.getEventsHandler().fire(HopGuiEvents.MetadataChanged.name());
      } catch (HopException e) {
        throw new RuntimeException(e);
      }
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
    if (this.hasChanged()) {

      MessageBox messageDialog =
          new MessageBox(getShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO | SWT.CANCEL);
      messageDialog.setText(
          TranslateUtil.translate(manager.getManagedName(), manager.getManagedClass()));
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

    if (StringUtils.startsWith(name, "$")) {
      throw new HopException(BaseMessages.getString(PKG, "MetadataEditor.Error.IncorrectName"));
    }

    // The serializer of the metadata
    //
    IHopMetadataSerializer<T> serializer = manager.getSerializer();

    if (StringUtils.isEmpty(originalName)) {
      isCreated = true;
    }

    // If rename
    //
    else if (!originalName.equals(name)) {

      // See if the name collides with an existing one...
      //
      if (serializer.exists(name)) {
        throw new HopException(
            BaseMessages.getString(PKG, "MetadataEditor.Error.NameAlreadyExists", name));
      } else {
        isRename = true;
      }
    }

    // Save it in the metadata
    serializer.save(metadata);

    if (isCreated)
      ExtensionPointHandler.callExtensionPoint(
          hopGui.getLog(),
          manager.getVariables(),
          HopExtensionPoint.HopGuiMetadataObjectCreated.id,
          metadata);
    else
      ExtensionPointHandler.callExtensionPoint(
          hopGui.getLog(),
          manager.getVariables(),
          HopExtensionPoint.HopGuiMetadataObjectUpdated.id,
          metadata);

    // Reset changed flag
    this.isChanged = false;
    this.title = metadata.getName();

    if (isRename) {
      // Code hardening for scenario where a new metadata object isn't yet persisted on disk yet.
      // The delete command can throw an error otherwise.
      //
      if (serializer.exists(originalName)) {
        serializer.delete(originalName);
      }
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

  @Override
  public void updateGui() {
    hopGui
        .getDisplay()
        .asyncExec(
            () ->
                hopGui.handleFileCapabilities(this.getFileType(), this.hasChanged(), false, false));
  }
}
