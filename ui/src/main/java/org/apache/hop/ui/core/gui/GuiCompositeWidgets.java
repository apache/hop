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

package org.apache.hop.ui.core.gui;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiElements;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.ITypeFilename;
import org.apache.hop.core.gui.plugin.ITypeMetadata;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Text;

/** This class contains the widgets for the GUI elements of a GUI Plugin */
public class GuiCompositeWidgets {
  public static final String CONST_PARENT_ID = ", parent ID: ";

  @Setter @Getter private IVariables variables;

  @Getter @Setter private Map<String, Control> labelsMap;

  @Getter @Setter private Map<String, Control> widgetsMap;

  private int nrItems;

  @Getter @Setter private IGuiPluginCompositeWidgetsListener compositeWidgetsListener;

  @Getter @Setter private IGuiPluginCompositeButtonsListener compositeButtonsListener;

  public GuiCompositeWidgets(IVariables variables) {
    this(variables, 0);
  }

  /**
   * @deprecated The maximum number of items used to pad to a maximum number of control lines is no
   *     longer implemented.
   * @param variables
   * @param maxNrItems
   */
  @Deprecated(since = "2.0")
  public GuiCompositeWidgets(IVariables variables, int maxNrItems) {
    this.variables = variables;
    labelsMap = new HashMap<>();
    widgetsMap = new HashMap<>();
    compositeWidgetsListener = null;
  }

  public void createCompositeWidgets(
      Object sourceData,
      String parentKey,
      Composite parent,
      String parentGuiElementId,
      Control lastControl) {
    if (sourceData == null) {
      // Nothing to do here. We can't detect widgets without an object.
      return;
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
    // Do not log error for NoneDatabaseMeta
    if (guiElements == null) {
      // Do not log for NoneDatabaseMeta
      if (!key.contains("NoneDatabaseMeta")) {
        LogChannel.UI.logError(
            "Create widgets: no GUI elements found for parent: "
                + key
                + CONST_PARENT_ID
                + parentGuiElementId);
      }
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

    int extraVerticalMargin = 0;
    if (lastControl instanceof Button) {
      // Checkbox: add a bit of margin
      extraVerticalMargin = (int) (3 * PropsUi.getInstance().getZoomFactor());
    }

    PropsUi props = PropsUi.getInstance();
    Label label = null;
    Control control = null;

    // Do we add the element or the children?
    //
    if (guiElements.getId() != null) {

      GuiElementType elementType = guiElements.getType();

      // Add the label on the left-hand side...
      // For metadata, the label is handled in the meta selection line widget below
      //
      if (StringUtils.isNotEmpty(guiElements.getLabel())
          && elementType != GuiElementType.METADATA
          && elementType != GuiElementType.BUTTON
          && elementType != GuiElementType.LINK) {
        label = new Label(parent, SWT.RIGHT | SWT.SINGLE);
        PropsUi.setLook(label);
        label.setText(Const.NVL(guiElements.getLabel(), ""));
        if (StringUtils.isNotEmpty(guiElements.getToolTip())) {
          label.setToolTipText(guiElements.getToolTip());
        }
        FormData fdLabel = new FormData();
        fdLabel.left = new FormAttachment(0, 0);
        if (lastControl == null) {
          fdLabel.top = new FormAttachment(0, PropsUi.getMargin());
        } else {
          fdLabel.top = new FormAttachment(lastControl, PropsUi.getMargin() + extraVerticalMargin);
        }
        fdLabel.right = new FormAttachment(props.getMiddlePct(), -PropsUi.getMargin());
        label.setLayoutData(fdLabel);
        labelsMap.put(guiElements.getId(), label);
      }

      // Add the GUI element
      //
      switch (elementType) {
        case TEXT, FILENAME, FOLDER:
          control = getTextControl(parent, guiElements, props, lastControl, label);
          break;
        case CHECKBOX:
          control = getCheckboxControl(parent, guiElements, props, lastControl, label);
          break;
        case COMBO:
          control = getComboControl(sourceObject, parent, guiElements, props, lastControl, label);
          break;
        case METADATA:
          control = getMetadataControl(parent, guiElements, props, lastControl);
          break;
        case BUTTON:
          control = getButtonControl(sourceObject, parent, guiElements, props, lastControl);
          break;
        case LINK:
          control = getLinkControl(parent, guiElements, props, lastControl);
        default:
          break;
      }

      if (control != null) {
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

    return previousControl;
  }

  private Control getComboControl(
      Object sourceObject,
      Composite parent,
      GuiElements guiElements,
      PropsUi props,
      Control lastControl,
      Label label) {
    Control control;
    String[] comboItems = getEnumValues(guiElements.getFieldClass());
    if (comboItems == null) {
      if (StringUtils.isNotEmpty(guiElements.getGetComboValuesMethod())) {
        comboItems = getComboItems(sourceObject, guiElements.getGetComboValuesMethod());
      } else {
        comboItems = new String[] {};
      }
    }
    if (guiElements.isVariablesEnabled()) {
      ComboVar comboVar = new ComboVar(variables, parent, SWT.BORDER | SWT.SINGLE | SWT.LEFT);
      PropsUi.setLook(comboVar);
      widgetsMap.put(guiElements.getId(), comboVar);
      comboVar.setItems(comboItems);
      control = comboVar;
    } else {
      Combo combo = new Combo(parent, SWT.BORDER | SWT.SINGLE | SWT.LEFT);
      PropsUi.setLook(combo);
      combo.setItems(comboItems);
      widgetsMap.put(guiElements.getId(), combo);
      control = combo;
    }

    addModifyListener(control, guiElements.getId());

    layoutControlBetweenLabelAndRightControl(props, lastControl, label, control, null);

    return control;
  }

  private Control getMetadataControl(
      Composite parent, GuiElements guiElements, PropsUi props, Control lastControl) {

    ITypeMetadata typeMetadata = instantiateTypeMetadata(guiElements);
    MetaSelectionLine<? extends IHopMetadata> metaSelectionLine =
        new MetaSelectionLine<>(
            variables,
            HopGui.getInstance().getMetadataProvider(),
            typeMetadata.getMetadataClass(),
            parent,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            guiElements.getLabel(),
            guiElements.getToolTip(),
            false,
            true);

    widgetsMap.put(guiElements.getId(), metaSelectionLine);

    // Fill the items...
    try {
      metaSelectionLine.fillItems();
    } catch (HopException e) {
      LogChannel.UI.logError("Error getting metadata items", e);
    }

    addModifyListener(metaSelectionLine.getComboWidget(), guiElements.getId());

    layoutControlBelowLast(props, lastControl, metaSelectionLine);

    return metaSelectionLine;
  }

  private Button getButtonControl(
      Object sourceObject,
      Composite parent,
      GuiElements guiElements,
      PropsUi props,
      Control lastControl) {

    Button button = new Button(parent, SWT.PUSH);
    PropsUi.setLook(button);
    button.setText(Const.NVL(guiElements.getLabel(), ""));
    if (StringUtils.isNotEmpty(guiElements.getToolTip())) {
      button.setToolTipText(guiElements.getToolTip());
    }
    widgetsMap.put(guiElements.getId(), button);

    button.addListener(
        SWT.Selection,
        event -> {
          // This widget annotation was on top of a method.
          // We need to instantiate the method using the provided classloader.
          //
          Method buttonMethod = guiElements.getButtonMethod();
          Class<?> methodClass = buttonMethod.getDeclaringClass();

          try {
            // If the source object is a metadata editor, it's useful to get the contents of all the
            // widgets.
            // That way you can more easily test connections and the like.
            // Otherwise, the source object has stale metadata in it.
            //
            if (compositeButtonsListener != null) {
              compositeButtonsListener.buttonPressed(sourceObject);
            }

            Object guiObject = methodClass.getDeclaredConstructor().newInstance();

            // Invoke the button method
            //
            buttonMethod.invoke(guiObject, sourceObject);
          } catch (Exception e) {
            LogChannel.UI.logError(
                "Error invoking method "
                    + buttonMethod.getName()
                    + " in class "
                    + methodClass.getName(),
                e);
          }
        });

    layoutControlBetweenLabelAndRightControl(props, lastControl, null, button, null);

    return button;
  }

  private Link getLinkControl(
      Composite parent, GuiElements guiElements, PropsUi props, Control lastControl) {

    Link link = new Link(parent, SWT.NONE);
    PropsUi.setLook(link);
    link.setText(Const.NVL(guiElements.getLabel(), ""));
    if (StringUtils.isNotEmpty(guiElements.getToolTip())) {
      link.setToolTipText(guiElements.getToolTip());
    }
    widgetsMap.put(guiElements.getId(), link);

    link.addListener(
        SWT.Selection,
        event -> {
          // This widget annotation was on top of a method.
          // We need to instantiate the method using the provided classloader.
          //
          Method buttonMethod = guiElements.getButtonMethod();
          Class<?> methodClass = buttonMethod.getDeclaringClass();

          try {

            Object guiObject = methodClass.getDeclaredConstructor().newInstance();

            // Invoke the link method
            //
            if (buttonMethod.getParameterCount() == 0) {
              buttonMethod.invoke(guiObject);
            } else {
              // Also pass along the event to detect which link was clicked.
              buttonMethod.invoke(guiObject, event);
            }
          } catch (Exception e) {
            LogChannel.UI.logError(
                "Error invoking method "
                    + buttonMethod.getName()
                    + " in class "
                    + methodClass.getName(),
                e);
          }
        });

    layoutControlBelowLast(props, lastControl, link);

    return link;
  }

  /**
   * See if the annotated field is an enum. If this is the case we can take the combo values from
   * the enum names.
   *
   * @param fieldClass The field class
   * @return The list of enum names or null if this is not an enum
   */
  private String[] getEnumValues(Class<?> fieldClass) {
    try {
      if (fieldClass.isEnum()) {
        Object[] enumConstants = fieldClass.getEnumConstants();
        String[] values = new String[enumConstants.length];
        for (int i = 0; i < values.length; i++) {
          values[i] = enumConstants[i].toString();
        }
        return values;
      } else {
        // Not an enum
        return null;
      }
    } catch (Exception e) {
      // This is unexpected, log it!
      //
      LogChannel.UI.logError(
          "Error finding enum values of field class: " + fieldClass.getName(), e);
      return null;
    }
  }

  private Control getCheckboxControl(
      Composite parent, GuiElements guiElements, PropsUi props, Control lastControl, Label label) {
    Control control;
    Button button = new Button(parent, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(button);
    widgetsMap.put(guiElements.getId(), button);
    addModifyListener(button, guiElements.getId());
    control = button;

    layoutControlBetweenLabelAndRightControl(props, lastControl, label, control, null);

    return control;
  }

  private Control getTextControl(
      Composite parent, GuiElements guiElements, PropsUi props, Control lastControl, Label label) {
    Control control;
    Control actionControl = null; // The control to add an action to
    Text text;

    switch (guiElements.getType()) {
      case FILENAME:
        Button wbBrowse = new Button(parent, SWT.PUSH);
        wbBrowse.setText(BaseMessages.getString("System.Button.Browse"));
        layoutControlOnRight(props, lastControl, wbBrowse, label);
        actionControl = wbBrowse;
        break;
      case FOLDER:
        wbBrowse = new Button(parent, SWT.PUSH);
        wbBrowse.setText(BaseMessages.getString("System.Button.Browse"));
        layoutControlOnRight(props, lastControl, wbBrowse, label);
        actionControl = wbBrowse;
        break;
      default:
        break;
    }

    if (guiElements.isVariablesEnabled()) {
      int style = SWT.BORDER | SWT.SINGLE | SWT.LEFT;
      if (guiElements.isPassword()) {
        style |= SWT.PASSWORD;
      }
      TextVar textVar = new TextVar(variables, parent, style);
      PropsUi.setLook(textVar);
      widgetsMap.put(guiElements.getId(), textVar);
      addModifyListener(textVar.getTextWidget(), guiElements.getId());
      control = textVar;
      text = textVar.getTextWidget();
    } else {
      int style = SWT.BORDER | SWT.SINGLE | SWT.LEFT;
      if (guiElements.isPassword()) {
        style |= SWT.PASSWORD;
      }
      text = new Text(parent, style);
      PropsUi.setLook(text);
      widgetsMap.put(guiElements.getId(), text);
      addModifyListener(text, guiElements.getId());
      control = text;
    }

    layoutControlBetweenLabelAndRightControl(props, lastControl, label, control, actionControl);

    // Add an action based on the sub-type:
    switch (guiElements.getType()) {
      case FILENAME:
        // Ask for a filename
        //
        ITypeFilename typeFilename = instantiateTypeFilename(guiElements);
        if (actionControl != null) {
          actionControl.addListener(
              SWT.Selection,
              e -> {
                String filename =
                    BaseDialog.presentFileDialog(
                        parent.getShell(),
                        null,
                        variables,
                        typeFilename.getFilterExtensions(),
                        typeFilename.getFilterNames(),
                        true);
                if (StringUtils.isNotEmpty(filename)) {
                  text.setText(filename);
                }
              });
        }
        break;
      case FOLDER:
        // ask for a folder
        //
        if (actionControl != null) {
          actionControl.addListener(
              SWT.Selection,
              e -> {
                String folder = BaseDialog.presentDirectoryDialog(parent.getShell());
                if (StringUtils.isNotEmpty(folder)) {
                  text.setText(folder);
                }
              });
        }
        break;
      default:
        break;
    }

    return control;
  }

  public ITypeFilename instantiateTypeFilename(GuiElements guiElements) {
    Class<? extends ITypeFilename> typeFilenameClass = guiElements.getTypeFilename();
    if (typeFilenameClass == null) {
      throw new RuntimeException(
          "Please specify a ITypeFilename class to use for widget " + guiElements.getId());
    }
    // Instantiate the class...
    //
    try {
      return typeFilenameClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(
          "Error instantiating class "
              + typeFilenameClass.getName()
              + " for GUI elements "
              + guiElements.getId()
              + " and type "
              + guiElements.getType(),
          e);
    }
  }

  public ITypeMetadata instantiateTypeMetadata(GuiElements guiElements) {
    Class<? extends ITypeMetadata> typeMetadataClass = guiElements.getTypeMetadata();
    if (typeMetadataClass == null) {
      throw new RuntimeException(
          "Please specify a ITypeMetadata class to use for widget " + guiElements.getId());
    }
    // Instantiate the class...
    //
    try {
      return typeMetadataClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(
          "Error instantiating class "
              + typeMetadataClass.getName()
              + " for GUI elements "
              + guiElements.getId()
              + " and type "
              + guiElements.getType(),
          e);
    }
  }

  private void layoutControlOnRight(
      PropsUi props, Control lastControl, Control control, Label label) {
    FormData fdControl = new FormData();
    fdControl.right = new FormAttachment(100, 0);
    if (label != null) {
      fdControl.top = new FormAttachment(label, 0, SWT.CENTER);
    } else {
      if (lastControl != null) {
        fdControl.top = new FormAttachment(lastControl, PropsUi.getMargin());
      } else {
        fdControl.top = new FormAttachment(0, 0);
      }
    }
    control.setLayoutData(fdControl);
  }

  private void layoutControlBetweenLabelAndRightControl(
      PropsUi props, Control lastControl, Label label, Control control, Control rightControl) {
    FormData fdControl = new FormData();
    if (label != null) {
      fdControl.left = new FormAttachment(props.getMiddlePct(), 0);
      if (rightControl == null) {
        fdControl.right = new FormAttachment(100, 0);
      } else {
        fdControl.right = new FormAttachment(rightControl, -5);
      }
      fdControl.top = new FormAttachment(label, 0, SWT.CENTER);
    } else {
      fdControl.left = new FormAttachment(props.getMiddlePct(), 0);
      if (rightControl == null) {
        fdControl.right = new FormAttachment(100, 0);
      } else {
        fdControl.right = new FormAttachment(rightControl, -5);
      }
      if (lastControl != null) {
        fdControl.top = new FormAttachment(lastControl, PropsUi.getMargin());
      } else {
        fdControl.top = new FormAttachment(0, 0);
      }
    }
    control.setLayoutData(fdControl);
  }

  private void layoutControlBelowLast(PropsUi props, Control lastControl, Control control) {
    FormData fdControl = new FormData();
    fdControl.left = new FormAttachment(0, 0);
    fdControl.right = new FormAttachment(100, 0);
    if (lastControl != null) {
      fdControl.top = new FormAttachment(lastControl, PropsUi.getMargin());
    } else {
      fdControl.top = new FormAttachment(0, 0);
    }
    control.setLayoutData(fdControl);
  }

  /**
   * If a widget changes
   *
   * @param control
   * @param widgetId
   */
  private void addModifyListener(final Control control, String widgetId) {
    if (control instanceof Button) {
      control.addListener(SWT.Selection, event -> notifyWidgetModified(event, control, widgetId));
    } else if (control instanceof Combo || control instanceof ComboVar) {
      control.addListener(SWT.Selection, event -> notifyWidgetModified(event, control, widgetId));
      control.addListener(SWT.Modify, event -> notifyWidgetModified(event, control, widgetId));
    } else {
      control.addListener(SWT.Modify, event -> notifyWidgetModified(event, control, widgetId));
    }
  }

  protected void notifyWidgetModified(final Event event, final Control control, String widgetId) {
    if (compositeWidgetsListener != null) {
      compositeWidgetsListener.widgetModified(this, control, widgetId);
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
    if (sourceData == null) {
      // We can't determine the widgets without an object.
      return;
    }
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
    // No data to set for a button widget
    if (guiElements.getType() == GuiElementType.BUTTON) {
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
          LogChannel.UI.logError(
              "Unable to get value for field: '"
                  + guiElements.getFieldName()
                  + "' : "
                  + e.getMessage());
          e.printStackTrace();
        }
        String stringValue = value == null ? "" : Const.NVL(value.toString(), "");

        switch (guiElements.getType()) {
          case TEXT, FILENAME, FOLDER:
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
          case METADATA:
            MetaSelectionLine line = (MetaSelectionLine) control;
            line.setText(stringValue);
            break;
          case BUTTON, LINK:
            // No data to set
            break;
          default:
            LogChannel.UI.logError(
                "WARNING: setting data on widget with ID "
                    + guiElements.getId()
                    + " : not implemented type "
                    + guiElements.getType()
                    + " yet.");
            break;
        }

      } else {
        LogChannel.UI.logError(
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
      // Do not log for NoneDatabaseMeta
      if (!sourceData.getClass().getName().contains("NoneDatabaseMeta")) {
        LogChannel.UI.logError(
            "getWidgetsContents: no GUI elements found for class: "
                + sourceData.getClass().getName()
                + CONST_PARENT_ID
                + parentGuiElementId);
      }
      return;
    }

    getWidgetsData(sourceData, guiElements);
  }

  private void getWidgetsData(Object sourceData, GuiElements guiElements) {
    if (guiElements.isIgnored()) {
      return;
    }
    // No data to retrieve from a button widget
    if (guiElements.getType() == GuiElementType.BUTTON) {
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
          case TEXT, FILENAME, FOLDER:
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
          case METADATA:
            MetaSelectionLine line = (MetaSelectionLine) control;
            value = line.getText();
            break;
          case BUTTON, LINK:
            // No data to retrieve from widget
            break;
          default:
            LogChannel.UI.logError(
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
          Class<?> fieldClass = guiElements.getFieldClass();
          if (fieldClass.isEnum()) {
            // set enum value
            //
            Class<Enum> enumClass = (Class<Enum>) fieldClass;

            // Look up the value as an enum...
            //
            if (value != null) {
              value = Enum.valueOf(enumClass, value.toString());
            }
          }
          new PropertyDescriptor(guiElements.getFieldName(), sourceData.getClass())
              .getWriteMethod()
              .invoke(sourceData, value);

        } catch (Exception e) {
          LogChannel.UI.logError(
              "Unable to set value '"
                  + value
                  + "'on field: '"
                  + guiElements.getFieldName()
                  + "' : "
                  + e.getMessage());
          e.printStackTrace();
        }

      } else {
        LogChannel.UI.logError(
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

  public IGuiPluginCompositeWidgetsListener getWidgetsListener() {
    return compositeWidgetsListener;
  }

  public void setWidgetsListener(IGuiPluginCompositeWidgetsListener listener) {
    this.compositeWidgetsListener = listener;
  }

  public void enableWidgets(Object sourceData, String parentGuiElementId, boolean enabled) {
    GuiRegistry registry = GuiRegistry.getInstance();
    GuiElements guiElements =
        registry.findGuiElements(sourceData.getClass().getName(), parentGuiElementId);

    if (guiElements == null) {
      // Do not log for NoneDatabaseMeta
      if (!sourceData.getClass().getName().contains("NoneDatabaseMeta")) {
        LogChannel.UI.logError(
            "enableWidgets: no GUI elements found for class: "
                + sourceData.getClass().getName()
                + CONST_PARENT_ID
                + parentGuiElementId);
      }
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
          LogChannel.UI.logError("Label not found to enable/disable: " + guiElements);
        }
        if (widget != null) {
          widget.setEnabled(enabled);
        } else {
          LogChannel.UI.logError("Widget not found to enable/disable: " + guiElements);
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
}
