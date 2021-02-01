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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElements;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** This class contains the widgets for the GUI elements of a GUI Plugin */
public class GuiCompositeWidgets {

  private IVariables variables;
  private Map<String, Control> labelsMap;
  private Map<String, Control> widgetsMap;
  private int maxNrItems;
  private int nrItems;
  private IGuiPluginCompositeWidgetsListener compositeWidgetsListener;

  public GuiCompositeWidgets(IVariables variables) {
    this(variables, 0);
  }

  public GuiCompositeWidgets(IVariables variables, int maxNrItems) {
    this.variables = variables;
    this.maxNrItems = maxNrItems;
    labelsMap = new HashMap<>();
    widgetsMap = new HashMap<>();
    nrItems = 0;
    compositeWidgetsListener = null;
  }

  public void createCompositeWidgets(
      Object sourceData,
      String parentKey,
      Composite parent,
      String parentGuiElementId,
      Control lastControl) {

    /*
     The developer wants to be informed of any change to the content of the widget
     We're just creating the widgets here so once the data is set it will generate a lot of modify listener events
    */
    if (sourceData != null && sourceData instanceof IGuiPluginCompositeWidgetsListener) {
      compositeWidgetsListener = (IGuiPluginCompositeWidgetsListener) sourceData;
    }

    // Find the GUI Elements for the given class...
    //
    GuiRegistry registry = GuiRegistry.getInstance();
    String key;
    if (StringUtils.isEmpty(parentKey)) {
      key = sourceData.getClass().getName();
    } else {
      key = parentKey;
    }
    GuiElements guiElements = registry.findGuiElements(key, parentGuiElementId);
    if (guiElements == null) {
      System.err.println(
          "Create widgets: no GUI elements found for parent: "
              + key
              + ", parent ID: "
              + parentGuiElementId);
      return;
    }

    // Loop over the GUI elements, create and remember the widgets...
    //
    addCompositeWidgets(sourceData, parent, guiElements, lastControl);

    if (compositeWidgetsListener != null) {
      compositeWidgetsListener.widgetsCreated(this);
    }

    // Force re-layout
    //
    parent.layout(true, true);
  }

  private Control addCompositeWidgets(
      Object sourceObject, Composite parent, GuiElements guiElements, Control lastControl) {

    if (guiElements.isIgnored()) {
      return lastControl;
    }

    PropsUi props = PropsUi.getInstance();
    Label label = null;
    Control control = null;

    // Do we add the element or the children?
    //
    if (guiElements.getId() != null) {

      // Add the label on the left hand side...
      //
      if (StringUtils.isNotEmpty(guiElements.getLabel())) {
        label = new Label(parent, SWT.RIGHT | SWT.SINGLE);
        props.setLook(label);
        label.setText(Const.NVL(guiElements.getLabel(), ""));
        if (StringUtils.isNotEmpty(guiElements.getToolTip())) {
          label.setToolTipText(guiElements.getToolTip());
        }
        FormData fdLabel = new FormData();
        fdLabel.left = new FormAttachment(0, 0);
        if (lastControl == null) {
          fdLabel.top = new FormAttachment(0, props.getMargin());
        } else {
          fdLabel.top = new FormAttachment(lastControl, props.getMargin());
        }
        fdLabel.right = new FormAttachment(Const.MIDDLE_PCT, 0);
        label.setLayoutData(fdLabel);
        labelsMap.put(guiElements.getId(), label);
      }

      // Add the GUI element
      //
      switch (guiElements.getType()) {
        case TEXT:
          if (guiElements.isVariablesEnabled()) {
            TextVar textVar = new TextVar(variables, parent, SWT.BORDER | SWT.SINGLE | SWT.LEFT);
            props.setLook(textVar);
            if (guiElements.isPassword()) {
              textVar.setEchoChar('*');
            }
            widgetsMap.put(guiElements.getId(), textVar);
            addModifyListener(textVar.getTextWidget(), guiElements.getId());
            control = textVar;
          } else {
            Text text = new Text(parent, SWT.BORDER | SWT.SINGLE | SWT.LEFT);
            props.setLook(text);
            if (guiElements.isPassword()) {
              text.setEchoChar('*');
            }
            widgetsMap.put(guiElements.getId(), text);
            addModifyListener(text, guiElements.getId() );
            control = text;
          }
          break;
        case CHECKBOX:
          Button button = new Button(parent, SWT.CHECK | SWT.LEFT);
          props.setLook(button);
          widgetsMap.put(guiElements.getId(), button);
          addModifyListener(button, guiElements.getId() );
          control = button;
          break;
        case COMBO:
          if (guiElements.isVariablesEnabled()) {
            ComboVar comboVar = new ComboVar(variables, parent, SWT.BORDER | SWT.SINGLE | SWT.LEFT);
            props.setLook(comboVar);
            widgetsMap.put(guiElements.getId(), comboVar);
            comboVar.setItems(getComboItems(sourceObject, guiElements.getGetComboValuesMethod()));
            control = comboVar;
          } else {
            Combo combo = new Combo(parent, SWT.BORDER | SWT.SINGLE | SWT.LEFT);
            props.setLook(combo);
            combo.setItems(getComboItems(sourceObject, guiElements.getGetComboValuesMethod()));
            widgetsMap.put(guiElements.getId(), combo);
            control = combo;
          }
          break;
        default:
          break;
      }

      if (control != null) {
        FormData fdControl = new FormData();
        if (label != null) {
          fdControl.left = new FormAttachment(Const.MIDDLE_PCT, props.getMargin());
          fdControl.right = new FormAttachment(100, 0);
          fdControl.top = new FormAttachment(label, 0, SWT.CENTER);
        } else {
          fdControl.left = new FormAttachment(Const.MIDDLE_PCT, props.getMargin());
          fdControl.right = new FormAttachment(100, 0);
          if (lastControl != null) {
            fdControl.top = new FormAttachment(lastControl, props.getMargin());
          } else {
            fdControl.top = new FormAttachment(0, 0);
          }
        }
        control.setLayoutData(fdControl);
        return control;
      } else {
        return lastControl;
      }
    }

    // Add the children
    //
    Control previousControl = lastControl;
    List<GuiElements> children = guiElements.getChildren();

    // Sort by ID
    Collections.sort(children);

    for (GuiElements child : guiElements.getChildren()) {
      previousControl = addCompositeWidgets(sourceObject, parent, child, previousControl);
      nrItems++;
    }

    // We might need to add a number of extra lines...
    // Let's just add empty labels..
    //
    for (; nrItems < maxNrItems; nrItems++) {
      label = new Label(parent, SWT.RIGHT | SWT.SINGLE);
      props.setLook(label);
      label.setText("                                                                    ");
      FormData fdLabel = new FormData();
      fdLabel.left = new FormAttachment(0, 0);
      if (previousControl == null) {
        fdLabel.top = new FormAttachment(0, 0);
      } else {
        fdLabel.top = new FormAttachment(previousControl, props.getMargin());
      }
      fdLabel.right = new FormAttachment(Const.MIDDLE_PCT, 0);
      label.setLayoutData(fdLabel);
      previousControl = label;
    }

    return previousControl;
  }

  /**
   * If a widget changes
   *
   * @param control
   * @param widgetId
   */
  private void addModifyListener( final Control control, String widgetId ) {
    if (compositeWidgetsListener != null) {
      if (control instanceof Button) {
        control.addListener(
            SWT.Selection, e -> compositeWidgetsListener.widgetModified(this, control, widgetId));
      } else {
        control.addListener(
            SWT.Modify, e -> compositeWidgetsListener.widgetModified(this, control, widgetId ));
      }
    }
  }

  private String[] getComboItems(Object sourceObject, String getComboValuesMethod) {
    try {
      Method method =
          sourceObject
              .getClass()
              .getMethod(getComboValuesMethod, ILogChannel.class, IHopMetadataProvider.class);
      if (method == null) {
        throw new HopException(
            "Unable to find method '"
                + getComboValuesMethod
                + "' with parameters ILogChannel and IHopMetadataProvider in object '"
                + sourceObject
                + "'");
      }
      List<String> names =
          (List<String>)
              method.invoke(
                  sourceObject, LogChannel.UI, HopGui.getInstance().getMetadataProvider());
      return names.toArray(new String[0]);
    } catch (Exception e) {
      LogChannel.UI.logError(
          "Error getting list of combo items for method '"
              + getComboValuesMethod
              + "' on source object: "
              + sourceObject,
          e);
      return new String[] {};
    }
  }

  public void setWidgetsContents(
      Object sourceData, Composite parentComposite, String parentGuiElementId) {

    GuiRegistry registry = GuiRegistry.getInstance();
    GuiElements guiElements =
        registry.findGuiElements(sourceData.getClass().getName(), parentGuiElementId);
    if (guiElements == null) {
      return;
    }

    setWidgetsData(sourceData, guiElements);

    if (compositeWidgetsListener != null) {
      compositeWidgetsListener.widgetsCreated(this);
    }

    parentComposite.layout(true, true);
  }

  private void setWidgetsData(Object sourceData, GuiElements guiElements) {

    if (guiElements.isIgnored()) {
      return;
    }

    // Do we add the element or the children?
    //
    if (guiElements.getId() != null) {

      Control control = widgetsMap.get(guiElements.getId());
      if (control != null) {

        // What's the value?
        //
        Object value = null;
        try {
          value =
              new PropertyDescriptor(guiElements.getFieldName(), sourceData.getClass())
                  .getReadMethod()
                  .invoke(sourceData);
        } catch (Exception e) {
          System.err.println(
              "Unable to get value for field: '"
                  + guiElements.getFieldName()
                  + "' : "
                  + e.getMessage());
          e.printStackTrace();
        }
        String stringValue = value == null ? "" : Const.NVL(value.toString(), "");

        switch (guiElements.getType()) {
          case TEXT:
            if (guiElements.isVariablesEnabled()) {
              TextVar textVar = (TextVar) control;
              textVar.setText(stringValue);
            } else {
              Text text = (Text) control;
              text.setText(stringValue);
            }
            break;
          case CHECKBOX:
            Button button = (Button) control;
            button.setSelection((Boolean) value);
            break;
          case COMBO:
            if (guiElements.isVariablesEnabled()) {
              ComboVar comboVar = (ComboVar) control;
              comboVar.setText(stringValue);
            } else {
              Combo combo = (Combo) control;
              combo.setText(stringValue);
            }
            break;
          default:
            System.err.println(
                "WARNING: setting data on widget with ID "
                    + guiElements.getId()
                    + " : not implemented type "
                    + guiElements.getType()
                    + " yet.");
            break;
        }

      } else {
        System.err.println(
            "Widget not found to set value on for id: "
                + guiElements.getId()
                + ", label: "
                + guiElements.getLabel());
      }
    } else {

      // Add the children
      //
      for (GuiElements child : guiElements.getChildren()) {
        setWidgetsData(sourceData, child);
      }
    }
  }

  public void getWidgetsContents(Object sourceData, String parentGuiElementId) {
    GuiRegistry registry = GuiRegistry.getInstance();
    GuiElements guiElements =
        registry.findGuiElements(sourceData.getClass().getName(), parentGuiElementId);
    if (guiElements == null) {
      System.err.println(
          "getWidgetsContents: no GUI elements found for class: "
              + sourceData.getClass().getName()
              + ", parent ID: "
              + parentGuiElementId);
      return;
    }

    getWidgetsData(sourceData, guiElements);
  }

  private void getWidgetsData(Object sourceData, GuiElements guiElements) {
    if (guiElements.isIgnored()) {
      return;
    }

    // Do we add the element or the children?
    //
    if (guiElements.getId() != null) {

      Control control = widgetsMap.get(guiElements.getId());
      if (control != null) {

        // What's the value?
        //
        Object value = null;

        switch (guiElements.getType()) {
          case TEXT:
            if (guiElements.isVariablesEnabled()) {
              TextVar textVar = (TextVar) control;
              value = textVar.getText();
            } else {
              Text text = (Text) control;
              value = text.getText();
            }
            break;
          case CHECKBOX:
            Button button = (Button) control;
            value = button.getSelection();
            break;
          case COMBO:
            if (guiElements.isVariablesEnabled()) {
              ComboVar comboVar = (ComboVar) control;
              value = comboVar.getText();
            } else {
              Combo combo = (Combo) control;
              value = combo.getText();
            }
            break;
          default:
            System.err.println(
                "WARNING: getting data from widget with ID "
                    + guiElements.getId()
                    + " : not implemented type "
                    + guiElements.getType()
                    + " yet.");
            break;
        }

        // Set the value on the source data object
        //
        try {
          new PropertyDescriptor(guiElements.getFieldName(), sourceData.getClass())
              .getWriteMethod()
              .invoke(sourceData, value);
        } catch (Exception e) {
          System.err.println(
              "Unable to set value '"
                  + value
                  + "'on field: '"
                  + guiElements.getFieldName()
                  + "' : "
                  + e.getMessage());
          e.printStackTrace();
        }

      } else {
        System.err.println(
            "Widget not found to set value on for id: "
                + guiElements.getId()
                + ", label: "
                + guiElements.getLabel());
      }
    } else {

      // Add the children
      //
      for (GuiElements child : guiElements.getChildren()) {
        getWidgetsData(sourceData, child);
      }
    }
  }

  public void enableWidgets(Object sourceData, String parentGuiElementId, boolean enabled) {
    GuiRegistry registry = GuiRegistry.getInstance();
    GuiElements guiElements =
        registry.findGuiElements(sourceData.getClass().getName(), parentGuiElementId);
    if (guiElements == null) {
      System.err.println(
          "enableWidgets: no GUI elements found for class: "
              + sourceData.getClass().getName()
              + ", parent ID: "
              + parentGuiElementId);
      return;
    }

    enableWidget(sourceData, guiElements, enabled);
  }

  private void enableWidget(Object sourceData, GuiElements guiElements, boolean enabled) {
    if (guiElements.isIgnored()) {
      return;
    }

    // Do we add the element or the children?
    //
    if (guiElements.getId() != null) {

      // TODO: look for flag to have custom enable/disable code
      //
      // Temp fix to keep DriverClass enabled
      if (!guiElements.getId().matches("driverClass")) {
        Control label = labelsMap.get(guiElements.getId());
        Control widget = widgetsMap.get(guiElements.getId());
        if (label != null) {
          label.setEnabled(enabled);
        } else {
          System.err.println("Label not found to enable/disable: " + guiElements);
        }
        if (widget != null) {
          widget.setEnabled(enabled);
        } else {
          System.err.println("Widget not found to enable/disable: " + guiElements);
        }
      }
    } else {
      // Add the children
      //
      for (GuiElements child : guiElements.getChildren()) {
        enableWidget(sourceData, child, enabled);
      }
    }
  }

  /**
   * Gets variables
   *
   * @return value of variables
   */
  public IVariables getVariables() {
    return variables;
  }

  /** @param variables The variables to set */
  public void setVariables( IVariables variables) {
    this.variables = variables;
  }

  /**
   * Gets labelsMap
   *
   * @return value of labelsMap
   */
  public Map<String, Control> getLabelsMap() {
    return labelsMap;
  }

  /** @param labelsMap The labelsMap to set */
  public void setLabelsMap(Map<String, Control> labelsMap) {
    this.labelsMap = labelsMap;
  }

  /**
   * Gets widgetsMap
   *
   * @return value of widgetsMap
   */
  public Map<String, Control> getWidgetsMap() {
    return widgetsMap;
  }

  /** @param widgetsMap The widgetsMap to set */
  public void setWidgetsMap(Map<String, Control> widgetsMap) {
    this.widgetsMap = widgetsMap;
  }

}
