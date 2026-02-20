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

package org.apache.hop.ui.core.gui;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarItem;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiKeyHandler;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Listener;

public class BaseGuiWidgets {

  /**
   * Every set of widgets (toolbar, composite, menu, ...) gets its own unique ID It will cause a new
   * object to be created per unique ID for the listener or GUI plugins if this plugin wasn't
   * registered yet.
   */
  protected String instanceId;

  /**
   * For convenience, we remember the classname of the GUI plugin that creates and owns these
   * widgets.
   */
  protected String guiPluginClassName;

  /** The plugin object which is registered */
  protected Object guiPluginObject;

  public BaseGuiWidgets(String instanceId) {
    this.instanceId = instanceId;
  }

  /**
   * Let the GUI plugin system know that there is no need to instantiate new objects for the given
   * class. Instead, this object can be taken. Make sure to call dispose() to prevent a (slow)
   * memory leak. Call this method before creating the widgets themselves.
   *
   * @param guiPluginObject
   */
  public void registerGuiPluginObject(Object guiPluginObject) {
    this.guiPluginObject = guiPluginObject;
    GuiRegistry guiRegistry = GuiRegistry.getInstance();
    guiPluginClassName = guiPluginObject.getClass().getName();
    guiRegistry.registerGuiPluginObject(
        HopGui.getInstance().getId(), guiPluginClassName, instanceId, guiPluginObject);
  }

  protected void addDeRegisterGuiPluginObjectListener(Control control) {
    control.addDisposeListener(
        e ->
            GuiRegistry.getInstance()
                .removeGuiPluginObjects(HopGui.getInstance().getId(), instanceId));
  }

  public void dispose() {
    String hopGuiId = HopGui.getInstance().getId();
    GuiRegistry.getInstance().removeGuiPluginObjects(hopGuiId, instanceId);
  }

  protected static Object findGuiPluginInstance(
      ClassLoader classLoader, String listenerClassName, String instanceId) throws Exception {
    try {
      // This is the class that owns the listener method
      // It's a GuiPlugin class in other words
      //
      String hopGuiId = HopGui.getInstance().getId();
      Object guiPluginObject =
          GuiRegistry.getInstance().findGuiPluginObject(hopGuiId, listenerClassName, instanceId);
      if (guiPluginObject == null) {
        // Create a new instance
        //
        guiPluginObject = classLoader.loadClass(listenerClassName).getConstructor().newInstance();

        // Store it
        //
        GuiRegistry.getInstance()
            .registerGuiPluginObject(hopGuiId, listenerClassName, instanceId, guiPluginObject);
      }
      HopGuiKeyHandler.getInstance().addParentObjectToHandle(guiPluginObject);
      return guiPluginObject;
    } catch (Exception e) {
      throw new HopException(
          "Error finding GuiPlugin instance for class '"
              + listenerClassName
              + "' and instance ID : "
              + instanceId,
          e);
    }
  }

  protected String[] getComboItems(GuiToolbarItem toolbarItem) {
    try {
      Object singleton =
          findGuiPluginInstance(
              toolbarItem.getClassLoader(), toolbarItem.getListenerClass(), instanceId);
      if (singleton == null) {
        LogChannel.UI.logError(
            "Could not get instance of class '"
                + toolbarItem.getListenerClass()
                + " for toolbar item "
                + toolbarItem
                + ", combo values method : "
                + toolbarItem.getGetComboValuesMethod());
        return new String[] {};
      }

      // TODO: create a method finder where we can simply give a list of objects that we have
      // available
      // You can find them in any order that the developer chose and just pass them that way.
      //
      Method method;
      Object[] arguments;
      try {
        method =
            singleton
                .getClass()
                .getMethod(
                    toolbarItem.getGetComboValuesMethod(),
                    ILogChannel.class,
                    IHopMetadataProvider.class);
        arguments = new Object[] {LogChannel.UI, HopGui.getInstance().getMetadataProvider()};
      } catch (NoSuchMethodException nsme) {
        try {
          method =
              singleton
                  .getClass()
                  .getMethod(
                      toolbarItem.getGetComboValuesMethod(),
                      ILogChannel.class,
                      IHopMetadataProvider.class,
                      String.class); // Instance ID
          arguments =
              new Object[] {LogChannel.UI, HopGui.getInstance().getMetadataProvider(), instanceId};
        } catch (NoSuchMethodException nsme2) {
          // Try to find the method without arguments...
          //
          try {
            method = singleton.getClass().getMethod(toolbarItem.getGetComboValuesMethod());
            arguments = new Object[] {};
          } catch (NoSuchMethodException nsme3) {
            throw new HopException(
                "Unable to find method '"
                    + toolbarItem.getGetComboValuesMethod()
                    + "' without parameters or with parameters ILogChannel and IHopMetadataProvider in class '"
                    + toolbarItem.getListenerClass()
                    + "'",
                nsme2);
          }
        }
      }
      List<String> values = (List<String>) method.invoke(singleton, arguments);
      return values.toArray(new String[0]);
    } catch (Exception e) {
      LogChannel.UI.logError(
          "Error getting list of combo items for method '"
              + toolbarItem.getGetComboValuesMethod()
              + "' in class : "
              + toolbarItem.getListenerClass(),
          e);
      return new String[] {};
    }
  }

  protected Listener getListener(
      ClassLoader classLoader, String listenerClassName, String listenerMethodName) {

    // Call the method to which the GuiToolbarElement annotation belongs.
    //
    return e -> {
      try {
        // See if we can find a static method which accepts this instance as an argument.
        // What's the registered GUI object we have?
        //
        try {
          Class<?> listenerClass = classLoader.loadClass(listenerClassName);
          Method listenerMethod =
              listenerClass.getMethod(listenerMethodName, guiPluginObject.getClass());
          listenerMethod.invoke(null, guiPluginObject);
          return;
        } catch (NoSuchMethodException
            | ClassNotFoundException
            | InvocationTargetException exception) {
          // Ignore this and re-try with the standard empty method
        } catch (Exception exception) {
          // An exception thrown by the method itself
          throw exception;
        }

        Object guiPluginInstance =
            findGuiPluginInstance(classLoader, listenerClassName, instanceId);
        Method listenerMethod = guiPluginInstance.getClass().getDeclaredMethod(listenerMethodName);
        listenerMethod.invoke(guiPluginInstance);

      } catch (Exception exception) {
        LogChannel.UI.logError(
            "Unable to call method "
                + listenerMethodName
                + " in class "
                + listenerClassName
                + " : "
                + exception.getMessage(),
            exception);
      }
    };
  }

  /**
   * Gets instanceId
   *
   * @return value of instanceId
   */
  public String getInstanceId() {
    return instanceId;
  }

  /**
   * @param instanceId The instanceId to set
   */
  public void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  /**
   * Gets guiPluginClassName
   *
   * @return value of guiPluginClassName
   */
  public String getGuiPluginClassName() {
    return guiPluginClassName;
  }

  /**
   * Sets guiPluginClassName
   *
   * @param guiPluginClassName value of guiPluginClassName
   */
  public void setGuiPluginClassName(String guiPluginClassName) {
    this.guiPluginClassName = guiPluginClassName;
  }

  /**
   * Gets guiPluginObject
   *
   * @return value of guiPluginObject
   */
  public Object getGuiPluginObject() {
    return guiPluginObject;
  }

  /**
   * Sets guiPluginObject
   *
   * @param guiPluginObject value of guiPluginObject
   */
  public void setGuiPluginObject(Object guiPluginObject) {
    this.guiPluginObject = guiPluginObject;
  }
}
