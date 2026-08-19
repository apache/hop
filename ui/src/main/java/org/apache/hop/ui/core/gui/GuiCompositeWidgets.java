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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiElements;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.ITypeFilename;
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
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
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
  public static final String NONE_DATABASE_META = "NoneDatabaseMeta";

  @Setter @Getter private IVariables variables;

  @Getter @Setter private Map<String, Control> labelsMap;

  @Getter @Setter private Map<String, Control> widgetsMap;

  /**
   * The extra control some elements put on the right of their widget, currently the Browse button
   * of a FILENAME or FOLDER element. Kept so {@link #setWidgetsHidden} can hide a whole row.
   */
  @Getter private Map<String, Control> actionWidgetsMap;

  private int nrItems;

  @Getter @Setter private IGuiPluginCompositeWidgetsListener compositeWidgetsListener;

  @Getter @Setter private IGuiPluginCompositeButtonsListener compositeButtonsListener;

  /** Parent composite of the last {@link #createCompositeWidgets} call (for button refresh). */
  private Composite widgetsParentComposite;

  /** Parent GUI element id of the last {@link #createCompositeWidgets} call. */
  private String widgetsParentGuiElementId;

  /** Layout flavour of the last {@link #createCompositeWidgets} call, needed to re-hang rows. */
  private boolean widgetsUseNewLayout;

  /** The control the first row was hung below, null when it is the top of the composite. */
  private Control widgetsFirstLastControl;

  /**
   * The {@link FormData#height} a row had before {@link #setWidgetsHidden} collapsed it, so that
   * showing it again restores the size it was created with rather than a default one.
   */
  private final Map<Control, Integer> collapsedHeights = new HashMap<>();

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
    actionWidgetsMap = new HashMap<>();
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
      if (!key.contains(NONE_DATABASE_META)) {
        LogChannel.UI.logError(
            "Create widgets: no GUI elements found for parent: "
                + key
                + CONST_PARENT_ID
                + parentGuiElementId);
      }
      return;
    }

    this.widgetsParentComposite = parent;
    this.widgetsParentGuiElementId = parentGuiElementId;
    this.widgetsFirstLastControl = lastControl;

    // Loop over the GUI elements, create and remember the widgets...
    //
    boolean useNewLayout = isConfigPlugin(sourceData.getClass());
    this.widgetsUseNewLayout = useNewLayout;
    addCompositeWidgets(sourceData, parent, guiElements, lastControl, useNewLayout);

    if (compositeWidgetsListener != null) {
      compositeWidgetsListener.widgetsCreated(this);
    }

    // Force re-layout
    //
    parent.layout(true, true);
  }

  /**
   * Check if a class has the @ConfigPlugin annotation
   *
   * @param clazz The class to check
   * @return true if the class has @ConfigPlugin annotation
   */
  private boolean isConfigPlugin(Class<?> clazz) {
    return clazz.isAnnotationPresent(ConfigPlugin.class);
  }

  /**
   * Hide the given widgets and close the gap they leave behind.
   *
   * <p>The rows sit in a {@link FormLayout} chain where each one hangs below the previous one, so
   * simply making a row invisible would leave a hole in the middle of the dialog. This re-hangs
   * every visible row on the last row that is still visible, then lays the composite out again.
   *
   * <p>The hidden rows are collapsed onto that same row rather than left where they were. {@link
   * FormLayout} takes no notice of visibility -- it lays out and measures invisible controls
   * exactly like visible ones -- so a hidden row that kept its original attachments would go on
   * reserving its height, and the composite would grow by every row it hides instead of shrinking.
   *
   * <p>Call this from {@link IGuiPluginCompositeWidgetsListener} when a plugin has options that
   * only apply to some of its settings, to keep the ones that cannot do anything out of the way.
   *
   * @param sourceData the object the widgets were created for
   * @param hiddenIds ids of the GUI elements to hide; every other element is made visible again
   */
  public void setWidgetsHidden(Object sourceData, Set<String> hiddenIds) {
    if (sourceData == null
        || widgetsParentComposite == null
        || widgetsParentComposite.isDisposed()) {
      return;
    }
    GuiElements root =
        GuiRegistry.getInstance()
            .findGuiElements(sourceData.getClass().getName(), widgetsParentGuiElementId);
    if (root == null) {
      return;
    }

    List<GuiElements> children = new ArrayList<>(root.getChildren());
    Collections.sort(children);

    Control lastVisible = widgetsFirstLastControl;
    Set<String> handled = new HashSet<>();
    for (GuiElements element : children) {
      if (element.isIgnored() || element.getId() == null) {
        continue;
      }
      // The registry appends without checking for duplicates, so an element can be in here more
      // than once if the GUI plugins were scanned twice. There is only ever one widget per id, and
      // hanging it below itself on a second pass would make the layout circular.
      //
      if (!handled.add(element.getId())) {
        continue;
      }
      Control label = labelsMap.get(element.getId());
      Control widget = widgetsMap.get(element.getId());
      Control action = actionWidgetsMap.get(element.getId());
      boolean hidden = hiddenIds.contains(element.getId());

      setControlVisible(label, !hidden);
      setControlVisible(widget, !hidden);
      setControlVisible(action, !hidden);

      if (hidden) {
        collapseRow(lastVisible, label, widget, action);
        continue;
      }

      restoreRowHeight(label);
      restoreRowHeight(widget);
      restoreRowHeight(action);

      if (widget == null || widget.isDisposed()) {
        continue;
      }
      reattachRow(element, label, widget, action, lastVisible);
      lastVisible = widget;
    }

    widgetsParentComposite.layout(true, true);

    // The composite itself has changed height, so whatever it sits in has to reflow as well.
    //
    Composite grandParent = widgetsParentComposite.getParent();
    if (grandParent != null && !grandParent.isDisposed()) {
      grandParent.layout(true, true);
    }
  }

  private void setControlVisible(Control control, boolean visible) {
    if (control != null && !control.isDisposed()) {
      control.setVisible(visible);
    }
  }

  /**
   * Park a hidden row on the last row that is still visible and take its height away, so that it
   * ends exactly where that row ends and adds nothing to the composite.
   *
   * <p>Zeroing the height is what does the work; the attachment only keeps the row from being
   * measured against a control that may itself have moved. The height it had is remembered so
   * {@link #restoreRowHeight} can put it back -- a multi-line TEXT element sizes itself to its line
   * count, and losing that would leave a one-line box behind when the row comes back.
   */
  private void collapseRow(Control lastVisible, Control... controls) {
    for (Control control : controls) {
      if (control == null
          || control.isDisposed()
          || !(control.getLayoutData() instanceof FormData formData)) {
        continue;
      }
      collapsedHeights.putIfAbsent(control, formData.height);
      formData.height = 0;
      formData.top =
          lastVisible == null ? new FormAttachment(0, 0) : new FormAttachment(lastVisible, 0);
    }
  }

  /** Give a row back the height {@link #collapseRow} took from it. */
  private void restoreRowHeight(Control control) {
    if (control == null || control.isDisposed()) {
      return;
    }
    Integer height = collapsedHeights.remove(control);
    if (height != null && control.getLayoutData() instanceof FormData formData) {
      formData.height = height;
    }
  }

  /**
   * Re-point the top attachments of one row at {@code lastVisible}, mirroring what the layout
   * methods do at creation time. Only the top attachment changes: left, right and any explicit
   * height stay as they were.
   */
  private void reattachRow(
      GuiElements element, Control label, Control widget, Control action, Control lastVisible) {

    int extraVerticalMargin =
        lastVisible instanceof Button ? (int) (3 * PropsUi.getInstance().getZoomFactor()) : 0;

    if (label != null && !label.isDisposed() && label.getLayoutData() instanceof FormData fdLabel) {
      fdLabel.top =
          lastVisible == null
              ? new FormAttachment(0, PropsUi.getMargin())
              : new FormAttachment(lastVisible, PropsUi.getMargin() + extraVerticalMargin);
    }

    if (widget.getLayoutData() instanceof FormData fdWidget) {
      boolean centeredOnLabel =
          !widgetsUseNewLayout
              && element.getType() == GuiElementType.CHECKBOX
              && lastVisible != null
              && label != null
              && !label.isDisposed();
      if (centeredOnLabel) {
        fdWidget.top = new FormAttachment(label, 0, SWT.CENTER);
      } else if (lastVisible == null) {
        fdWidget.top = new FormAttachment(0, PropsUi.getMargin());
      } else {
        fdWidget.top = new FormAttachment(lastVisible, PropsUi.getMargin());
      }
    }

    if (action != null
        && !action.isDisposed()
        && action.getLayoutData() instanceof FormData fdAction) {
      fdAction.top =
          lastVisible == null
              ? new FormAttachment(0, PropsUi.getMargin())
              : new FormAttachment(lastVisible, PropsUi.getMargin());
    }
  }

  private Control addCompositeWidgets(
      Object sourceObject,
      Composite parent,
      GuiElements guiElements,
      Control lastControl,
      boolean useNewLayout) {

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

      // Add the label
      // For metadata, button, and link, the label is handled in the widget itself
      // For checkbox in new layout, the label is handled in the widget itself
      //
      if (StringUtils.isNotEmpty(guiElements.getLabel())
          && elementType != GuiElementType.METADATA
          && elementType != GuiElementType.BUTTON
          && elementType != GuiElementType.LINK
          && !(useNewLayout && elementType == GuiElementType.CHECKBOX)) {
        // Use new layout (label above) for ConfigPlugin classes, old layout (label on left) for
        // others
        int labelStyle = useNewLayout ? SWT.LEFT : (SWT.RIGHT | SWT.SINGLE);
        label = new Label(parent, labelStyle);
        PropsUi.setLook(label);
        label.setText(Const.NVL(guiElements.getLabel(), ""));
        if (StringUtils.isNotEmpty(guiElements.getToolTip())) {
          label.setToolTipText(guiElements.getToolTip());
        }
        FormData fdLabel = new FormData();
        fdLabel.left = new FormAttachment(0, 0);
        if (useNewLayout) {
          // New layout: label spans full width
          fdLabel.right = new FormAttachment(100, 0);
        } else {
          // Old layout: label on left side (up to middle percentage)
          fdLabel.right = new FormAttachment(props.getMiddlePct(), -PropsUi.getMargin());
        }
        if (lastControl == null) {
          fdLabel.top = new FormAttachment(0, PropsUi.getMargin());
        } else {
          fdLabel.top = new FormAttachment(lastControl, PropsUi.getMargin() + extraVerticalMargin);
        }
        label.setLayoutData(fdLabel);
        labelsMap.put(guiElements.getId(), label);
      }

      // Add the GUI element
      //
      switch (elementType) {
        case TEXT, FILENAME, FOLDER, MULTI_LINE_TEXT:
          control = getTextControl(parent, guiElements, props, lastControl, label, useNewLayout);
          break;
        case CHECKBOX:
          control =
              getCheckboxControl(parent, guiElements, props, lastControl, label, useNewLayout);
          break;
        case COMBO:
          control =
              getComboControl(
                  sourceObject, parent, guiElements, props, lastControl, label, useNewLayout);
          break;
        case METADATA:
          control = getMetadataControl(parent, guiElements, props, lastControl, useNewLayout);
          break;
        case BUTTON:
          control =
              getButtonControl(sourceObject, parent, guiElements, props, lastControl, useNewLayout);
          break;
        case LINK:
          control = getLinkControl(parent, guiElements, props, lastControl, useNewLayout);
          break;
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
      previousControl =
          addCompositeWidgets(sourceObject, parent, child, previousControl, useNewLayout);
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
      Label label,
      boolean useNewLayout) {
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

    layoutControlBetweenLabelAndRightControl(
        props, lastControl, label, control, null, useNewLayout);

    return control;
  }

  private Control getMetadataControl(
      Composite parent,
      GuiElements guiElements,
      PropsUi props,
      Control lastControl,
      boolean useNewLayout) {

    MetaSelectionLine<? extends IHopMetadata> metaSelectionLine =
        new MetaSelectionLine<>(
            variables,
            HopGui.getInstance().getMetadataProvider(),
            guiElements.getMetadataClass(),
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

    layoutControlBelowLast(props, lastControl, metaSelectionLine, useNewLayout);

    return metaSelectionLine;
  }

  private Button getButtonControl(
      Object sourceObject,
      Composite parent,
      GuiElements guiElements,
      PropsUi props,
      Control lastControl,
      boolean useNewLayout) {

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

            // Invoke the button method (mutations apply to sourceObject)
            //
            buttonMethod.invoke(guiObject, sourceObject);

            // Re-bind form fields from the (possibly mutated) source object. Template-load and
            // similar buttons open modal dialogs; refresh both immediately and on the next UI
            // cycle so the parent form reliably shows the new values after the shell restores.
            final Object mutatedSource = sourceObject;
            final Button sourceButton = button;
            final String widgetId = guiElements.getId();
            refreshWidgetsAfterButton(mutatedSource, sourceButton, widgetId);
            if (button.getDisplay() != null && !button.getDisplay().isDisposed()) {
              button
                  .getDisplay()
                  .asyncExec(
                      () -> {
                        if (sourceButton.isDisposed()) {
                          return;
                        }
                        refreshWidgetsAfterButton(mutatedSource, sourceButton, widgetId);
                      });
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

    layoutControlBetweenLabelAndRightControl(props, lastControl, null, button, null, useNewLayout);

    return button;
  }

  /**
   * After an annotated BUTTON method returns, push {@code sourceObject} field values back into the
   * composite widgets and notify listeners. Safe to call more than once.
   */
  private void refreshWidgetsAfterButton(
      Object sourceObject, Button sourceButton, String widgetId) {
    if (compositeButtonsListener != null) {
      compositeButtonsListener.afterButtonPressed(sourceObject);
    }
    if (widgetsParentComposite != null
        && widgetsParentGuiElementId != null
        && !widgetsParentComposite.isDisposed()) {
      setWidgetsContents(sourceObject, widgetsParentComposite, widgetsParentGuiElementId);
      if (!widgetsParentComposite.isDisposed()) {
        widgetsParentComposite.layout(true, true);
        widgetsParentComposite.redraw();
      }
    }
    if (compositeWidgetsListener != null && sourceButton != null && !sourceButton.isDisposed()) {
      compositeWidgetsListener.widgetModified(this, sourceButton, widgetId);
    }
  }

  private Link getLinkControl(
      Composite parent,
      GuiElements guiElements,
      PropsUi props,
      Control lastControl,
      boolean useNewLayout) {

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

    layoutControlBelowLast(props, lastControl, link, useNewLayout);

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
      Composite parent,
      GuiElements guiElements,
      PropsUi props,
      Control lastControl,
      Label label,
      boolean useNewLayout) {
    Control control;
    Button button = new Button(parent, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(button);
    if (useNewLayout) {
      // New layout: label text on the checkbox itself
      button.setText(Const.NVL(guiElements.getLabel(), ""));
      if (StringUtils.isNotEmpty(guiElements.getToolTip())) {
        button.setToolTipText(guiElements.getToolTip());
      }
    } else {
      // Old layout: checkbox has no text (label is separate)
      if (StringUtils.isNotEmpty(guiElements.getToolTip())) {
        button.setToolTipText(guiElements.getToolTip());
      }
    }
    widgetsMap.put(guiElements.getId(), button);
    addModifyListener(button, guiElements.getId());
    control = button;

    if (useNewLayout) {
      // New layout: checkboxes are laid out below the last control, full width
      layoutControlBelowLast(props, lastControl, control, useNewLayout);
    } else {
      // Old layout: checkbox next to label
      layoutControlBetweenLabelAndRightControl(
          props, lastControl, label, control, null, useNewLayout, true);
    }

    return control;
  }

  private Control getTextControl(
      Composite parent,
      GuiElements guiElements,
      PropsUi props,
      Control lastControl,
      Label label,
      boolean useNewLayout) {
    Control control;
    Control actionControl = null; // The control to add an action to
    Text text;

    switch (guiElements.getType()) {
      case FILENAME:
        Button wbBrowse = new Button(parent, SWT.PUSH);
        wbBrowse.setText(BaseMessages.getString("System.Button.Browse"));
        layoutControlOnRight(lastControl, wbBrowse, label, useNewLayout);
        actionWidgetsMap.put(guiElements.getId(), wbBrowse);
        actionControl = wbBrowse;
        break;
      case FOLDER:
        wbBrowse = new Button(parent, SWT.PUSH);
        wbBrowse.setText(BaseMessages.getString("System.Button.Browse"));
        layoutControlOnRight(lastControl, wbBrowse, label, useNewLayout);
        actionWidgetsMap.put(guiElements.getId(), wbBrowse);
        actionControl = wbBrowse;
        break;
      default:
        break;
    }

    boolean multiLine = guiElements.getType() == GuiElementType.MULTI_LINE_TEXT;
    int style;
    if (multiLine) {
      // Multi-line does not use password masking.
      style = SWT.BORDER | SWT.MULTI | SWT.LEFT | SWT.WRAP | SWT.V_SCROLL | SWT.H_SCROLL;
    } else {
      style = SWT.BORDER | SWT.SINGLE | SWT.LEFT;
    }

    if (guiElements.isVariablesEnabled()) {
      if (!multiLine && guiElements.isPassword()) {
        String toolTip =
            StringUtils.isNotEmpty(guiElements.getToolTip()) ? guiElements.getToolTip() : null;
        // PasswordTextVar never mirrors the field value in the tooltip (TextVar may on some
        // platforms when echo char is reported as '\\0' for PASSWORD fields).
        PasswordTextVar textVar = new PasswordTextVar(variables, parent, style, toolTip);
        PropsUi.setLook(textVar);
        widgetsMap.put(guiElements.getId(), textVar);
        addModifyListener(textVar.getTextWidget(), guiElements.getId());
        control = textVar;
        text = textVar.getTextWidget();
      } else {
        TextVar textVar = new TextVar(variables, parent, style);
        PropsUi.setLook(textVar);
        widgetsMap.put(guiElements.getId(), textVar);
        addModifyListener(textVar.getTextWidget(), guiElements.getId());
        control = textVar;
        text = textVar.getTextWidget();
      }
    } else {
      if (!multiLine && guiElements.isPassword()) {
        style |= SWT.PASSWORD;
      }
      text = new Text(parent, style);
      PropsUi.setLook(text);
      widgetsMap.put(guiElements.getId(), text);
      addModifyListener(text, guiElements.getId());
      control = text;
    }

    layoutControlBetweenLabelAndRightControl(
        props, lastControl, label, control, actionControl, useNewLayout);

    if (multiLine) {
      applyMultiLineTextHeight(props, control, text, guiElements.getMultiLineTextHeight());
    }

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
      throw new HopRuntimeException(
          "Please specify a ITypeFilename class to use for widget " + guiElements.getId());
    }
    // Instantiate the class...
    //
    try {
      return typeFilenameClass.getConstructor().newInstance();
    } catch (Exception e) {
      throw new HopRuntimeException(
          "Error instantiating class "
              + typeFilenameClass.getName()
              + " for GUI elements "
              + guiElements.getId()
              + " and type "
              + guiElements.getType(),
          e);
    }
  }

  /**
   * Sets an explicit FormData height for multi-line text so FormLayout does not collapse the
   * control to a single line.
   *
   * @param props UI properties (zoom)
   * @param control the outer control (may be TextVar composite)
   * @param text the inner SWT Text
   * @param lines preferred height in text lines
   */
  private void applyMultiLineTextHeight(PropsUi props, Control control, Text text, int lines) {
    int lineCount = Math.max(1, lines);
    int lineHeight = 0;
    try {
      if (text != null && !text.isDisposed()) {
        lineHeight = text.getLineHeight();
      }
    } catch (Exception e) {
      // Fall through to font-based estimate
    }
    if (lineHeight <= 0) {
      // Typical default font height * zoom when the control is not yet realized
      lineHeight = (int) Math.ceil(15 * props.getZoomFactor());
    }
    // Small padding per line for borders / inter-line spacing
    int linePx = lineHeight + Math.max(1, PropsUi.getMargin() / 2);
    Object layoutData = control.getLayoutData();
    if (layoutData instanceof FormData fdControl) {
      fdControl.height = lineCount * linePx;
    }
  }

  private void layoutControlOnRight(
      Control lastControl, Control control, Label label, boolean useNewLayout) {
    FormData fdControl = new FormData();
    fdControl.right = new FormAttachment(100, 0);
    if (label != null) {
      if (useNewLayout) {
        // New layout: control goes on the right, aligned with the row below the label
        fdControl.top = new FormAttachment(label, PropsUi.getMargin() / 2);
      } else {
        // Old layout: control aligned with lastControl to maintain proper vertical spacing
        if (lastControl != null) {
          fdControl.top = new FormAttachment(lastControl, PropsUi.getMargin());
        } else {
          fdControl.top = new FormAttachment(0, PropsUi.getMargin());
        }
      }
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
      PropsUi props,
      Control lastControl,
      Label label,
      Control control,
      Control rightControl,
      boolean useNewLayout) {
    layoutControlBetweenLabelAndRightControl(
        props, lastControl, label, control, rightControl, useNewLayout, false);
  }

  private void layoutControlBetweenLabelAndRightControl(
      PropsUi props,
      Control lastControl,
      Label label,
      Control control,
      Control rightControl,
      boolean useNewLayout,
      boolean checkBox) {
    FormData fdControl = new FormData();
    if (label != null) {
      if (useNewLayout) {
        // New layout: control goes below the label, full width (or next to right control)
        fdControl.left = new FormAttachment(0, 0);
        if (rightControl == null) {
          fdControl.right = new FormAttachment(100, 0);
        } else {
          fdControl.right = new FormAttachment(rightControl, -PropsUi.getMargin());
        }
        fdControl.top = new FormAttachment(label, PropsUi.getMargin() / 2);
      } else {
        // Old layout: control on right side, next to label
        fdControl.left = new FormAttachment(props.getMiddlePct(), 0);
        if (rightControl == null) {
          // Do not stretch checkboxes across the remaining width — on some platforms a
          // full-width SWT.CHECK looks like an empty push button next to the label.
          if (!checkBox) {
            fdControl.right = new FormAttachment(100, 0);
          }
        } else {
          fdControl.right = new FormAttachment(rightControl, -5);
        }
        // Attach to lastControl to create proper vertical spacing between widgets
        if (lastControl != null) {
          if (checkBox) {
            // Center on the label
            fdControl.top = new FormAttachment(label, 0, SWT.CENTER);
          } else {
            fdControl.top = new FormAttachment(lastControl, PropsUi.getMargin());
          }
        } else {
          fdControl.top = new FormAttachment(0, PropsUi.getMargin());
        }
      }
    } else {
      // No label
      if (useNewLayout) {
        fdControl.left = new FormAttachment(0, 0);
        if (rightControl == null) {
          fdControl.right = new FormAttachment(100, 0);
        } else {
          fdControl.right = new FormAttachment(rightControl, -PropsUi.getMargin());
        }
      } else {
        fdControl.left = new FormAttachment(props.getMiddlePct(), 0);
        if (rightControl == null) {
          fdControl.right = new FormAttachment(100, 0);
        } else {
          fdControl.right = new FormAttachment(rightControl, -5);
        }
      }
      if (lastControl != null) {
        fdControl.top = new FormAttachment(lastControl, PropsUi.getMargin());
      } else {
        fdControl.top = new FormAttachment(0, 0);
      }
    }
    control.setLayoutData(fdControl);
  }

  private void layoutControlBelowLast(
      PropsUi props, Control lastControl, Control control, boolean useNewLayout) {
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
      LogChannel.UI.logError(
          "setWidgetsContents: no GUI elements found for class: "
              + sourceData.getClass().getName()
              + CONST_PARENT_ID
              + parentGuiElementId);
      return;
    }

    setWidgetsData(sourceData, guiElements);

    if (compositeWidgetsListener != null) {
      compositeWidgetsListener.widgetsCreated(this);
    }

    if (parentComposite != null && !parentComposite.isDisposed()) {
      parentComposite.layout(true, true);
    }
  }

  /**
   * Read a field value from the source object using the annotated getter method name when
   * available, falling back to {@link PropertyDescriptor}.
   */
  private Object readFieldValue(Object sourceData, GuiElements guiElements) {
    try {
      if (StringUtils.isNotEmpty(guiElements.getGetterMethod())) {
        Method getter = sourceData.getClass().getMethod(guiElements.getGetterMethod());
        return getter.invoke(sourceData);
      }
    } catch (Exception e) {
      // Fall through to PropertyDescriptor
    }
    try {
      return new PropertyDescriptor(guiElements.getFieldName(), sourceData.getClass())
          .getReadMethod()
          .invoke(sourceData);
    } catch (Exception e) {
      LogChannel.UI.logError(
          "Unable to get value for field: '" + guiElements.getFieldName() + "'", e);
      return null;
    }
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

        // A widget element declared on a method - a composite, a link, a button - has no field
        // behind it, so there is no value to put into it.
        //
        if (guiElements.getFieldName() == null) {
          return;
        }

        // What's the value?
        //
        Object value = readFieldValue(sourceData, guiElements);
        String stringValue = value == null ? "" : Const.NVL(value.toString(), "");

        switch (guiElements.getType()) {
          case TEXT, FILENAME, FOLDER, MULTI_LINE_TEXT:
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
            button.setSelection(Boolean.TRUE.equals(value));
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
            try {
              line.fillItems();
            } catch (Exception e) {
              LogChannel.UI.logError(
                  "Unable to fill items for metadata widget '"
                      + guiElements.getFieldName()
                      + "': "
                      + e.getMessage());
            }
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
      if (!sourceData.getClass().getName().contains(NONE_DATABASE_META)) {
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

  /**
   * Whether a value of {@code valueClass} can be handed to a setter parameter of {@code
   * parameterType}. {@link Class#isAssignableFrom} alone is not enough: a checkbox produces a
   * {@link Boolean} for a {@code boolean} parameter, and reflection accepts that through unboxing.
   */
  private boolean isAssignable(Class<?> parameterType, Class<?> valueClass) {
    if (parameterType.isAssignableFrom(valueClass)) {
      return true;
    }
    if (!parameterType.isPrimitive()) {
      return false;
    }
    return (parameterType == boolean.class && valueClass == Boolean.class)
        || (parameterType == int.class && valueClass == Integer.class)
        || (parameterType == long.class && valueClass == Long.class)
        || (parameterType == double.class && valueClass == Double.class)
        || (parameterType == float.class && valueClass == Float.class)
        || (parameterType == short.class && valueClass == Short.class)
        || (parameterType == byte.class && valueClass == Byte.class)
        || (parameterType == char.class && valueClass == Character.class);
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

        // A widget element declared on a method - a composite, a link, a button - has no field
        // behind it, so there is no value to retrieve from it.
        //
        if (guiElements.getFieldName() == null) {
          return;
        }

        // What's the value?
        //
        Object value = null;

        switch (guiElements.getType()) {
          case TEXT, FILENAME, FOLDER, MULTI_LINE_TEXT:
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
          // Resolve the setter first: its parameter type is the authority on what this value has
          // to be. The field class the registry captured at scan time is NOT - a plugin class can
          // be loaded by more than one classloader, and an enum constant resolved against the
          // wrong one has the right name but is a different type, which reflection rejects with a
          // bare "argument type mismatch".
          //
          Method setter =
              new PropertyDescriptor(guiElements.getFieldName(), sourceData.getClass())
                  .getWriteMethod();
          if (setter == null) {
            LogChannel.UI.logError(
                "No setter found for field '"
                    + guiElements.getFieldName()
                    + "', value not applied");
            return;
          }

          Class<?> parameterType = setter.getParameterTypes()[0];

          if (parameterType.isEnum()) {
            String constantName = value == null ? null : value.toString();
            if (StringUtils.isEmpty(constantName)) {
              // Nothing picked yet. A combo reads back as empty while the dialog is still being
              // populated, and setText() on one widget sends us here for every other one. Writing
              // that over the object would discard the value we are about to display.
              //
              LogChannel.UI.logDebug(
                  "Enum field '"
                      + guiElements.getFieldName()
                      + "' read back as empty, keeping the current value");
              return;
            }
            try {
              value = Enum.valueOf((Class<Enum>) parameterType, constantName);
            } catch (IllegalArgumentException e) {
              LogChannel.UI.logDebug(
                  "Ignoring value '"
                      + constantName
                      + "' for field '"
                      + guiElements.getFieldName()
                      + "': not a constant of "
                      + parameterType.getName());
              return;
            }
          }

          // A widget that has not produced a value yet leaves this null, and a null against a
          // primitive setter fails with a bare "argument type mismatch" that names neither the
          // field nor the cause. Leave the object alone instead: the value is about to be set
          // properly, and overwriting it here is how a saved setting silently reverts to its
          // default.
          //
          if (value == null && parameterType.isPrimitive()) {
            // Never expected: a widget of a primitive-backed field always yields a value. Logged
            // at error level rather than debug precisely because it means a setting did not get
            // applied, which is otherwise invisible.
            //
            LogChannel.UI.logError(
                "Skipping field '"
                    + guiElements.getFieldName()
                    + "': no value from widget '"
                    + guiElements.getId()
                    + "' for primitive "
                    + parameterType.getName());
            return;
          }

          if (value != null && !isAssignable(parameterType, value.getClass())) {
            LogChannel.UI.logError(
                "Value of type "
                    + value.getClass().getName()
                    + " does not fit setter "
                    + setter.getName()
                    + "("
                    + parameterType.getName()
                    + ") for field '"
                    + guiElements.getFieldName()
                    + "' (widget '"
                    + guiElements.getId()
                    + "'), value not applied");
            return;
          }

          setter.invoke(sourceData, value);

        } catch (Exception e) {
          LogChannel.UI.logError(
              "Unable to set value '"
                  + value
                  + "' on field: '"
                  + guiElements.getFieldName()
                  + "' (widget '"
                  + guiElements.getId()
                  + "') : "
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
      if (!sourceData.getClass().getName().contains(NONE_DATABASE_META)) {
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

  public void setComboValues(String widgetId, String[] fieldNames) {
    Control control = widgetsMap.get(widgetId);
    if (control instanceof Combo combo) {
      combo.setItems(fieldNames);
    } else if (control instanceof ComboVar comboVar) {
      comboVar.setItems(fieldNames);
    }
  }

  /**
   * This adds a scrolled composite on a new Composite that will contain all the widgets with the
   * specified parent ID as its parent.
   *
   * @param parent The parent to add the scrolled composite to
   * @param top The top control to layout with. Null means top of the parent.
   * @param bottom The bottom control to layout with. Null means the bottom of the parent.
   * @param guiParentId The GUI parent ID to use to look up the widgets to place on the composite.
   * @param sourceData The source data to use to populate the data on the widgets.
   */
  public static GuiCompositeWidgets addScrolledComposite(
      Composite parent,
      IVariables variables,
      Control top,
      Control bottom,
      String guiParentId,
      Object sourceData) {
    ScrolledComposite scrolledComposite =
        new ScrolledComposite(parent, SWT.V_SCROLL | SWT.H_SCROLL);
    scrolledComposite.setMinSize(SWT.DEFAULT, SWT.DEFAULT);

    // The composite grabs the whole scrolled composite size
    //
    Composite composite = new Composite(scrolledComposite, SWT.NONE);
    FormLayout compositeLayout = new FormLayout();

    // Leave some room at the bottom and right for the scroll bars if they show up.
    //
    compositeLayout.marginRight = 2 * PropsUi.getFormMargin();
    compositeLayout.marginBottom = 2 * PropsUi.getFormMargin();
    composite.setLayout(compositeLayout);
    FormData fdComposite = new FormData();
    fdComposite.left = new FormAttachment(0, 0);
    fdComposite.top = new FormAttachment(0, 0);
    fdComposite.right = new FormAttachment(100, 0);
    fdComposite.bottom = new FormAttachment(100, 0);
    composite.setLayoutData(fdComposite);

    // We add all the widgets...
    //
    GuiCompositeWidgets widgets = new GuiCompositeWidgets(variables);
    widgets.createCompositeWidgets(sourceData, null, composite, guiParentId, null);
    widgets.setWidgetsContents(sourceData, composite, guiParentId);
    scrolledComposite.setContent(composite);

    // Layout the scrolled composite.
    //
    FormData fdScrolled = new FormData();
    if (top != null) {
      fdScrolled.top = new FormAttachment(top, PropsUi.getMargin());
    } else {
      fdScrolled.top = new FormAttachment(0, 0);
    }
    if (bottom != null) {
      fdScrolled.bottom = new FormAttachment(bottom, -2 * PropsUi.getMargin());
    } else {
      fdScrolled.bottom = new FormAttachment(100, 0);
    }
    fdScrolled.left = new FormAttachment(0, 0);
    fdScrolled.right = new FormAttachment(100, 0);
    scrolledComposite.setLayoutData(fdScrolled);

    composite.pack();
    Rectangle bounds = composite.getBounds();
    scrolledComposite.setContent(composite);
    scrolledComposite.setExpandHorizontal(true);
    scrolledComposite.setExpandVertical(true);
    scrolledComposite.setMinWidth(bounds.width);
    scrolledComposite.setMinHeight(bounds.height);

    return widgets;
  }
}
