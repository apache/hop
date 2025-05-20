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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.util.TranslateUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.GuiContextHandler;
import org.apache.hop.ui.hopgui.context.GuiContextUtil;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.apache.hop.ui.util.SwtErrorHandler;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Shell;

/**
 * This is a utility class that allows you to create or edit metadata objects in a generic fashion
 */
public class MetadataManager<T extends IHopMetadata> {

  private static final Class<?> PKG = MetadataManager.class; // For i18n
  public static final String CONST_ERROR = "Error";
  public static final String CONST_ERROR_EDITING_METADATA = "Error editing metadata";
  private final Shell parentShell;
  @Getter @Setter private IHopMetadataProvider metadataProvider;
  @Getter @Setter private IVariables variables;
  @Getter @Setter private ClassLoader classLoader;
  @Setter @Getter private Class<T> managedClass;

  public MetadataManager(
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      Class<T> managedClass,
      Shell parentShell) {
    this.variables = variables;
    this.classLoader = managedClass.getClassLoader();
    this.metadataProvider = metadataProvider;
    this.managedClass = managedClass;
    this.parentShell = parentShell;
  }

  /**
   * edit an element
   *
   * @return True if anything was changed
   */
  public boolean editMetadata() {
    HopGui hopGui = HopGui.getInstance();
    try {
      List<String> names = getNames();

      // Plugin details from the managed class...
      //
      HopMetadata hopMetadata = HopMetadataUtil.getHopMetadataAnnotation(managedClass);

      // Show an action dialog...
      //
      List<GuiAction> actions = new ArrayList<>();
      for (final String name : names) {
        GuiAction action =
            new GuiAction(
                name,
                GuiActionType.Modify,
                name,
                name + " : " + hopMetadata.description(),
                hopMetadata.image(),
                (shiftAction, controlAction, t) -> editMetadata(name));
        action.setClassLoader(getClassLoader());
        actions.add(action);
      }
      return GuiContextUtil.getInstance()
          .handleActionSelection(
              hopGui.getActiveShell(),
              "Select the " + hopMetadata.name() + " to edit",
              new GuiContextHandler("HopGuiMetadataContext", actions));

    } catch (Exception e) {
      new ErrorDialog(hopGui.getActiveShell(), CONST_ERROR, CONST_ERROR_EDITING_METADATA, e);
      return false;
    }
  }

  /**
   * delete an element
   *
   * @return True if anything was changed
   */
  public boolean deleteMetadata() {
    HopGui hopGui = HopGui.getInstance();
    try {
      List<String> names = getNames();

      HopMetadata hopMetadata = HopMetadataUtil.getHopMetadataAnnotation(managedClass);

      // Show an action dialog...
      //
      List<GuiAction> actions = new ArrayList<>();
      for (final String name : names) {
        GuiAction action =
            new GuiAction(
                name,
                GuiActionType.Delete,
                name,
                name + " : " + hopMetadata.description(),
                hopMetadata.image(),
                (shiftAction, controlAction, t) -> deleteMetadata(name));
        action.setClassLoader(getClassLoader());
        actions.add(action);
      }
      return GuiContextUtil.getInstance()
          .handleActionSelection(
              hopGui.getShell(),
              "Select the " + hopMetadata.name() + " to delete after confirmation",
              new GuiContextHandler("HopGuiMetadaContext", actions));

    } catch (Exception e) {
      new ErrorDialog(hopGui.getActiveShell(), CONST_ERROR, "Error deleting metadata", e);
      return false;
    }
  }

  /**
   * We look at the managed class name, add Dialog to it and then simply us that class to edit the
   * dialog.
   *
   * @param elementName The name of the element to edit
   * @return True, if anything was changed
   */
  public boolean editMetadata(String elementName) {

    if (StringUtils.isEmpty(elementName)) {
      return false;
    }

    HopGui hopGui = HopGui.getInstance();

    try {
      IHopMetadataSerializer<T> serializer = metadataProvider.getSerializer(managedClass);

      // Load the metadata element from the metadata
      //
      T element = serializer.load(getVariables().resolve(elementName));
      if (element == null) {
        // Something removed or renamed the element in the background
        //
        throw new HopException(
            "Unable to find element '" + getVariables().resolve(elementName) + "' in the metadata");
      }

      initializeElementVariables(element);

      MetadataEditor<T> editor = this.createEditor(element);
      editor.setTitle(element.getName());

      // Open this element in the metadata perspective if that one is active.
      //
      MetadataPerspective perspective = HopGui.getMetadataPerspective();
      if (perspective.isActive()) {
        perspective.addEditor(editor);
        return false;
      } else {
        // Make sure to use the correct parent shell
        //
        MetadataEditorDialog dialog = new MetadataEditorDialog(parentShell, editor);
        String result = dialog.open();

        if (result != null) {
          ExtensionPointHandler.callExtensionPoint(
              hopGui.getLog(),
              variables,
              HopExtensionPoint.HopGuiMetadataObjectUpdated.id,
              element);
          return true;
        } else {
          return false;
        }
      }

    } catch (Exception e) {
      if (!SwtErrorHandler.handleException(e)) {
        new ErrorDialog(hopGui.getActiveShell(), CONST_ERROR, CONST_ERROR_EDITING_METADATA, e);
      }
      return false;
    }
  }

  public void editWithEditor(String name) {
    if (name == null) {
      return;
    }

    HopGui hopGui = HopGui.getInstance();

    try {
      HopMetadata annotation = HopMetadataUtil.getHopMetadataAnnotation(managedClass);

      MetadataPerspective perspective = MetadataPerspective.getInstance();
      MetadataEditor<?> editor = perspective.findEditor(annotation.key(), name);

      if (editor == null) {

        // Load the metadata element from the metadata
        //
        IHopMetadataSerializer<T> serializer = metadataProvider.getSerializer(managedClass);
        T element = serializer.load(name);
        if (element == null) {
          // Something removed or renamed the element in the background
          //
          throw new HopException("Unable to find element '" + name + "' in the metadata");
        }

        initializeElementVariables(element);

        perspective.addEditor(createEditor(element));
      } else {
        perspective.setActiveEditor(editor);
      }
    } catch (Exception e) {
      if (!SwtErrorHandler.handleException(e)) {
        new ErrorDialog(hopGui.getActiveShell(), CONST_ERROR, CONST_ERROR_EDITING_METADATA, e);
      }
    }
  }

  private void initializeElementVariables(T element) {
    if (element instanceof IVariables iVariables) {
      iVariables.initializeFrom(variables);
    }
  }

  /**
   * delete an element
   *
   * @param elementName The name of the element to delete
   * @return True if anything was deleted
   */
  public boolean deleteMetadata(String elementName) {

    if (StringUtils.isEmpty(elementName)) {
      return false;
    }

    MessageBox confirmBox =
        new MessageBox(HopGui.getInstance().getShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO);
    confirmBox.setText("Delete?");
    confirmBox.setMessage("Are you sure you want to delete element " + elementName + "?");
    int anwser = confirmBox.open();
    if ((anwser & SWT.YES) == 0) {
      return false;
    }

    try {
      IHopMetadataSerializer<T> serializer = getSerializer();

      // delete the metadata object from the metadata
      //
      T object = serializer.delete(elementName);

      // Just to be precise.
      //
      initializeElementVariables(object);

      ExtensionPointHandler.callExtensionPoint(
          HopGui.getInstance().getLog(),
          variables,
          HopExtensionPoint.HopGuiMetadataObjectDeleted.id,
          object);

      return true;

    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          CONST_ERROR,
          "Error deleting metadata element " + elementName,
          e);
      return false;
    }
  }

  public boolean rename(String oldName, String newName) throws HopException {
    IHopMetadataSerializer<T> serializer = this.getSerializer();

    if (serializer.exists(newName)) {
      MessageBox messageBox =
          new MessageBox(HopGui.getInstance().getShell(), SWT.ICON_ERROR | SWT.OK);
      messageBox.setText(TranslateUtil.translate(getManagedName(), getManagedClass()));
      messageBox.setMessage("Name '" + newName + "' already exists.");
      messageBox.open();

      return false;
    }

    T metadata = this.loadElement(oldName);
    metadata.setName(newName);
    serializer.save(metadata);
    serializer.delete(oldName);

    return true;
  }

  public IHopMetadataSerializer<T> getSerializer() throws HopException {
    return metadataProvider.getSerializer(managedClass);
  }

  public boolean openMetaDialog(T object, IHopMetadataSerializer<T> serializer) throws Exception {
    if (object == null) {
      return false;
    }
    HopGui hopGui = HopGui.getInstance();

    String dialogClassName = calculateDialogClassname();

    // Create the dialog class editor...
    // Always pass the shell, the metadata and the object to edit...
    //
    Class<?>[] constructorArguments =
        new Class<?>[] {Shell.class, IHopMetadataProvider.class, managedClass};
    Object[] constructorParameters = new Object[] {hopGui.getShell(), metadataProvider, object};

    Class<IMetadataDialog> dialogClass;
    try {
      dialogClass = (Class<IMetadataDialog>) classLoader.loadClass(dialogClassName);
    } catch (ClassNotFoundException e1) {
      String simpleDialogClassName = calculateSimpleDialogClassname();
      try {
        dialogClass = (Class<IMetadataDialog>) classLoader.loadClass(simpleDialogClassName);
      } catch (ClassNotFoundException e2) {
        try {
          dialogClass = (Class<IMetadataDialog>) Class.forName(dialogClassName);
        } catch (ClassNotFoundException e3) {
          dialogClass = (Class<IMetadataDialog>) Class.forName(simpleDialogClassName);
        }
      }
    }
    Constructor<IMetadataDialog> constructor;
    try {
      constructor = dialogClass.getDeclaredConstructor(constructorArguments);
    } catch (NoSuchMethodException nsm) {
      constructorArguments =
          new Class<?>[] {Shell.class, IHopMetadataProvider.class, managedClass, IVariables.class};
      constructorParameters =
          new Object[] {hopGui.getShell(), metadataProvider, object, hopGui.getVariables()};
      constructor = dialogClass.getDeclaredConstructor(constructorArguments);
    }
    if (constructor == null) {
      throw new HopException(
          "Unable to find dialog class ("
              + dialogClassName
              + ") constructor with arguments: Shell, IHopMetadataProvider, T and optionally IVariables");
    }

    IMetadataDialog dialog = constructor.newInstance(constructorParameters);
    String name = dialog.open();
    if (name != null) {
      // Save it in the metadata
      serializer.save(object);

      ExtensionPointHandler.callExtensionPoint(
          HopGui.getInstance().getLog(),
          variables,
          HopExtensionPoint.HopGuiMetadataObjectUpdated.id,
          object);

      return true;
    } else {
      return false;
    }
  }

  public T newMetadata() {
    try {
      // Create a new instance of the managed class
      //
      T element = managedClass.getDeclaredConstructor().newInstance();
      initializeElementVariables(element);

      return newMetadata(element);
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(), CONST_ERROR, "Error creating new metadata element", e);
      return null;
    }
  }

  public T newMetadata(T element) {
    try {

      ExtensionPointHandler.callExtensionPoint(
          HopGui.getInstance().getLog(),
          variables,
          HopExtensionPoint.HopGuiMetadataObjectCreateBeforeDialog.id,
          element);

      MetadataEditor<T> editor = this.createEditor(element);

      // Always open this in a separate dialog so that we block until we have a name for the new
      // element.
      // Parent code can then use that name to fill in the proper widget.
      //
      MetadataEditorDialog dialog =
          new MetadataEditorDialog(HopGui.getInstance().getShell(), editor);

      String name = dialog.open();
      if (name != null) {
        ExtensionPointHandler.callExtensionPoint(
            HopGui.getInstance().getLog(),
            variables,
            HopExtensionPoint.HopGuiMetadataObjectCreated.id,
            element);
      }
      return element;
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(), CONST_ERROR, "Error editing new metadata element", e);
      return null;
    }
  }

  public T newMetadataWithEditor(String virtualPath) {
    HopGui hopGui = HopGui.getInstance();

    try {

      // Create a new instance of the managed class
      //
      T element = managedClass.getDeclaredConstructor().newInstance();
      initializeElementVariables(element);
      element.setVirtualPath(virtualPath);

      ExtensionPointHandler.callExtensionPoint(
          hopGui.getLog(),
          variables,
          HopExtensionPoint.HopGuiMetadataObjectCreateBeforeDialog.id,
          element);

      MetadataEditor<T> editor = this.createEditor(element);
      editor.setTitle(
          BaseMessages.getString(
              PKG,
              "MetadataManager.New.Label",
              TranslateUtil.translate(this.getManagedName(), managedClass)));

      MetadataPerspective.getInstance().addEditor(editor);

      return element;
    } catch (Exception e) {
      if (SwtErrorHandler.handleException(e)) {
        new ErrorDialog(
            hopGui.getActiveShell(), CONST_ERROR, "Error creating new metadata element", e);
      }
      return null;
    }
  }

  public List<String> getNames() throws HopException {
    try {
      List<String> names = getSerializer().listObjectNames();
      Collections.sort(names);
      return names;

    } catch (Exception e) {
      throw new HopException(
          "Unable to get list of element names in the MetaStore for class "
              + managedClass.getName(),
          e);
    }
  }

  public String[] getNamesArray() throws HopException {
    try {
      return getNames().toArray(new String[0]);
    } catch (Exception e) {
      throw new HopException(
          "Unable to get element names array in the MetaStore for class " + managedClass.getName(),
          e);
    }
  }

  public String calculateDialogClassname() {
    String dialogClassName = managedClass.getName();
    dialogClassName = dialogClassName.replaceFirst("\\.hop\\.", ".hop.ui.");
    dialogClassName += "Dialog";
    return dialogClassName;
  }

  public String calculateSimpleDialogClassname() {
    String dialogClassName = managedClass.getName();
    dialogClassName += "Dialog";
    return dialogClassName;
  }

  public MetadataEditor<T> createEditor(T metadata) throws HopException {

    // Find the class editor...
    //
    String className = managedClass.getName();
    className += "Editor";
    Class<MetadataEditor<T>> editorClass;
    try {
      editorClass = (Class<MetadataEditor<T>>) classLoader.loadClass(className);
    } catch (ClassNotFoundException e1) {
      className = managedClass.getName();
      className = className.replaceFirst("\\.hop\\.", ".hop.ui.");
      className += "Editor";
      try {
        editorClass = (Class<MetadataEditor<T>>) classLoader.loadClass(className);
      } catch (ClassNotFoundException e) {
        throw new HopException("Unable to find editor class (" + className + ")");
      }
    }

    // Create the class editor...
    // Always pass the HopGui, the metadata manager and the object to edit...
    //
    try {

      Class<?>[] constructorArguments =
          new Class<?>[] {HopGui.class, MetadataManager.class, metadata.getClass()};

      Constructor<MetadataEditor<T>> constructor;
      constructor = editorClass.getDeclaredConstructor(constructorArguments);

      return constructor.newInstance(HopGui.getInstance(), this, metadata);
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | SecurityException
        | IllegalArgumentException
        | InvocationTargetException e) {
      throw new HopException("Unable to create editor for class " + managedClass.getName(), e);
    }
  }

  protected String getManagedName() {
    HopMetadata annotation = managedClass.getAnnotation(HopMetadata.class);
    if (annotation != null) {
      return annotation.name();
    }
    return null;
  }

  public T loadElement(String selectedItem) throws HopException {
    T element = getSerializer().load(selectedItem);
    initializeElementVariables(element);
    return element;
  }
}
