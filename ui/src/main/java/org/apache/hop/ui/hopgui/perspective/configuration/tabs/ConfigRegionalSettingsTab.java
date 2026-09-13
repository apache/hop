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
 *
 */

package org.apache.hop.ui.hopgui.perspective.configuration.tabs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.tab.GuiTab;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.i18n.GlobalMessages;
import org.apache.hop.i18n.LanguageChoice;
import org.apache.hop.i18n.RegionalSettings;
import org.apache.hop.i18n.RegionalSettingsPreview;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ComboFilterPopup;
import org.apache.hop.ui.hopgui.perspective.configuration.ConfigurationPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;

/**
 * Lets the user pick the interface language and, independently of it, the locale that decides the
 * decimal separator, the grouping separator, the currency and the date and time formats.
 */
@GuiPlugin
public class ConfigRegionalSettingsTab {
  private static final Class<?> PKG = BaseDialog.class;

  private Combo wDefaultLocale;
  private Button wUseOperatingSystem;
  private Button wOverride;
  private Combo wLocale;

  private Label wShortDate;
  private Label wLongDate;
  private Label wShortTime;
  private Label wLongTime;
  private Label wNumber;
  private Label wNegativeNumber;
  private Label wCurrency;
  private Label wPercent;

  private List<Locale> localeList;

  /** Widgets fire their listeners while they are being populated; don't save on those events. */
  private boolean isInitializing;

  public ConfigRegionalSettingsTab() {
    // This instance is created in the GuiPlugin system by calling this constructor, after which it
    // calls the addConfigRegionalSettingsTab() method.
  }

  @GuiTab(
      id = "10120-config-perspective-regional-settings-tab",
      parentId = ConfigurationPerspective.CONFIG_PERSPECTIVE_TABS,
      description = "Regional settings")
  public void addConfigRegionalSettingsTab(CTabFolder wTabFolder) {
    isInitializing = true;

    int margin = PropsUi.getMargin();
    RegionalSettings settings = RegionalSettings.getInstance();
    // Mutable: a saved locale that the offered list filters out is appended to it.
    localeList = new ArrayList<>(buildLocaleList());

    CTabItem wRegionalTab = new CTabItem(wTabFolder, SWT.NONE);
    wRegionalTab.setFont(GuiResource.getInstance().getFontDefault());
    wRegionalTab.setText(BaseMessages.getString(PKG, "ConfigRegionalSettingsTab.Title"));
    wRegionalTab.setImage(GuiResource.getInstance().getImageOptions());

    ScrolledComposite sRegionalComp =
        new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    sRegionalComp.setLayout(new FormLayout());

    Composite wRegionalComp = new Composite(sRegionalComp, SWT.NONE);
    PropsUi.setLook(wRegionalComp);
    FormLayout regionalLayout = new FormLayout();
    regionalLayout.marginWidth = PropsUi.getFormMargin();
    regionalLayout.marginHeight = PropsUi.getFormMargin();
    wRegionalComp.setLayout(regionalLayout);

    Control lastControl = null;

    // Preferred language: this is the interface language only, it no longer decides the formats.
    //
    wDefaultLocale =
        createComboField(
            wRegionalComp,
            "EnterOptionsDialog.DefaultLocale.Label",
            GlobalMessages.localeDescr,
            false,
            lastControl,
            margin);
    int languageIndex =
        Const.indexOfString(
            LanguageChoice.getInstance().getDefaultLocale().toString(), GlobalMessages.localeCodes);
    if (languageIndex >= 0) {
      wDefaultLocale.select(languageIndex);
    }
    lastControl = wDefaultLocale;

    // Use the operating system regional settings.
    //
    wUseOperatingSystem =
        createCheckbox(
            wRegionalComp,
            "ConfigRegionalSettingsTab.UseOperatingSystem.Label",
            "ConfigRegionalSettingsTab.UseOperatingSystem.Tooltip",
            settings.getSource() == RegionalSettings.Source.OPERATING_SYSTEM,
            lastControl,
            margin);
    wUseOperatingSystem.addListener(
        SWT.Selection,
        e -> {
          if (wUseOperatingSystem.getSelection()) {
            wOverride.setSelection(false);
          }
          onSourceChanged();
        });
    lastControl = wUseOperatingSystem;

    // Override the regional settings with an explicitly picked locale.
    //
    wOverride =
        createCheckbox(
            wRegionalComp,
            "ConfigRegionalSettingsTab.Override.Label",
            "ConfigRegionalSettingsTab.Override.Tooltip",
            settings.getSource() == RegionalSettings.Source.CUSTOM,
            lastControl,
            margin);
    wOverride.addListener(
        SWT.Selection,
        e -> {
          if (wOverride.getSelection()) {
            wUseOperatingSystem.setSelection(false);
            // Without a selection the override would be saved as a plain language choice, leaving
            // the ticked checkbox and the configuration disagreeing.
            ensureLocaleSelected();
          }
          onSourceChanged();
        });
    lastControl = wOverride;

    // Editable, unlike the language combo: the filter popup narrows several hundred entries by
    // typing, and it needs a combo it can write into.
    wLocale =
        createComboField(
            wRegionalComp,
            "ConfigRegionalSettingsTab.Locale.Label",
            localeList.stream().map(Locale::getDisplayName).toArray(String[]::new),
            true,
            lastControl,
            margin);
    ComboFilterPopup.attach(wLocale, () -> Arrays.asList(wLocale.getItems()), null);
    selectCustomLocale(settings.getCustomLocale());
    wLocale.setEnabled(wOverride.getSelection());
    wLocale.addListener(
        SWT.Modify,
        e -> {
          // Every keystroke lands here now. Half-typed text names no locale, so wait until the
          // combo shows one of the offered entries.
          if (selectedLocaleIndex() < 0) {
            return;
          }
          refreshPreview();
          save();
        });
    lastControl = wLocale;

    // The preview of what the settings above produce, before anything is saved.
    //
    Group wDateTimeGroup =
        createPreviewGroup(
            wRegionalComp, "ConfigRegionalSettingsTab.DateTimeGroup.Label", lastControl, margin);
    wShortDate =
        createPreviewRow(wDateTimeGroup, "ConfigRegionalSettingsTab.ShortDate.Label", null, margin);
    wLongDate =
        createPreviewRow(
            wDateTimeGroup, "ConfigRegionalSettingsTab.LongDate.Label", wShortDate, margin);
    wShortTime =
        createPreviewRow(
            wDateTimeGroup, "ConfigRegionalSettingsTab.ShortTime.Label", wLongDate, margin);
    wLongTime =
        createPreviewRow(
            wDateTimeGroup, "ConfigRegionalSettingsTab.LongTime.Label", wShortTime, margin);
    lastControl = wDateTimeGroup;

    Group wNumbersGroup =
        createPreviewGroup(
            wRegionalComp, "ConfigRegionalSettingsTab.NumbersGroup.Label", lastControl, margin);
    wNumber =
        createPreviewRow(wNumbersGroup, "ConfigRegionalSettingsTab.Number.Label", null, margin);
    wNegativeNumber =
        createPreviewRow(
            wNumbersGroup, "ConfigRegionalSettingsTab.NegativeNumber.Label", wNumber, margin);
    wCurrency =
        createPreviewRow(
            wNumbersGroup, "ConfigRegionalSettingsTab.Currency.Label", wNegativeNumber, margin);
    wPercent =
        createPreviewRow(
            wNumbersGroup, "ConfigRegionalSettingsTab.Percent.Label", wCurrency, margin);

    // Registered last: the preview it triggers reads every other widget on this tab.
    //
    wDefaultLocale.addListener(
        SWT.Modify,
        e -> {
          refreshPreview();
          save();
        });

    refreshPreview();

    wRegionalComp.layout();
    wRegionalComp.pack();
    sRegionalComp.setContent(wRegionalComp);
    sRegionalComp.setExpandHorizontal(true);
    sRegionalComp.setExpandVertical(true);
    sRegionalComp.setMinWidth(wRegionalComp.getBounds().width);
    sRegionalComp.setMinHeight(wRegionalComp.getBounds().height);

    wRegionalTab.setControl(sRegionalComp);

    isInitializing = false;
  }

  /**
   * The locales offered for the override. Deliberately not {@link GlobalMessages#localeCodes},
   * which only lists the languages Hop is translated into: the regional settings are independent of
   * the interface language, so any locale the JVM knows about is a valid choice. Locales without a
   * country carry no formats worth picking, and variants and scripts would only clutter the list
   * with near-duplicates.
   */
  private List<Locale> buildLocaleList() {
    return Arrays.stream(Locale.getAvailableLocales())
        .filter(l -> !l.getCountry().isEmpty())
        .filter(l -> l.getVariant().isEmpty() && l.getScript().isEmpty())
        .sorted(Comparator.comparing(Locale::getDisplayName))
        .toList();
  }

  private void selectCustomLocale(Locale customLocale) {
    if (customLocale == null) {
      return;
    }
    if (!localeList.contains(customLocale)) {
      // A saved locale can carry a variant or a script, which the offered list filters out. Keep it
      // rather than dropping the user's choice on the floor.
      localeList.add(customLocale);
      wLocale.add(customLocale.getDisplayName());
    }
    selectLocaleAt(localeList.indexOf(customLocale));
  }

  /**
   * The index in {@link #localeList} of the locale the combo currently shows, or -1 when it shows
   * none. Resolved through the displayed text rather than {@link Combo#getSelectionIndex()}: the
   * filter popup applies a choice with {@code setText()}, which leaves the selection index behind
   * on whatever was picked before. Reading the text covers both ways of choosing, and reports "no
   * locale" while a query is being typed.
   */
  private int selectedLocaleIndex() {
    return wLocale.indexOf(wLocale.getText());
  }

  /**
   * Show the locale at {@code index}. The text is written explicitly on top of the selection: on an
   * editable combo {@code select()} alone is not guaranteed to update the text field, and the text
   * is what {@link #selectedLocaleIndex()} reads back.
   */
  private void selectLocaleAt(int index) {
    wLocale.select(index);
    wLocale.setText(wLocale.getItem(index));
  }

  private void ensureLocaleSelected() {
    if (selectedLocaleIndex() >= 0) {
      return;
    }
    // Start from the locale that is in effect right now, so ticking the override keeps the formats
    // the user is already seeing instead of silently switching them to another locale.
    Locale current = RegionalSettings.getInstance().getEffectiveLocale();
    int index = current == null ? -1 : localeList.indexOf(current);
    selectLocaleAt(index >= 0 ? index : 0);
  }

  /**
   * Reloads the widgets from the configuration, so a change made outside this tab doesn't leave it
   * showing stale values. Invoked reflectively when the configuration perspective is activated.
   */
  public void reloadValues() {
    if (wDefaultLocale == null || wDefaultLocale.isDisposed()) {
      // The tab was never built or is already disposed.
      return;
    }

    isInitializing = true;
    try {
      RegionalSettings settings = RegionalSettings.getInstance();

      int languageIndex =
          Const.indexOfString(
              LanguageChoice.getInstance().getDefaultLocale().toString(),
              GlobalMessages.localeCodes);
      if (languageIndex >= 0) {
        wDefaultLocale.select(languageIndex);
      }

      wUseOperatingSystem.setSelection(
          settings.getSource() == RegionalSettings.Source.OPERATING_SYSTEM);
      wOverride.setSelection(settings.getSource() == RegionalSettings.Source.CUSTOM);
      selectCustomLocale(settings.getCustomLocale());
      wLocale.setEnabled(wOverride.getSelection());

      refreshPreview();
    } finally {
      isInitializing = false;
    }
  }

  private void onSourceChanged() {
    wLocale.setEnabled(wOverride.getSelection());
    refreshPreview();
    save();
  }

  /**
   * The locale the current state of the widgets would produce, so the preview can show the effect
   * of a choice before it is saved.
   */
  private Locale currentEffectiveLocale() {
    if (wUseOperatingSystem.getSelection()) {
      return RegionalSettings.getInstance().getOperatingSystemLocale();
    }
    if (wOverride.getSelection() && selectedLocaleIndex() >= 0) {
      return localeList.get(selectedLocaleIndex());
    }
    int index = wDefaultLocale.getSelectionIndex();
    if (index < 0 || index >= GlobalMessages.localeCodes.length) {
      index = 0;
    }
    return EnvUtil.createLocale(GlobalMessages.localeCodes[index]);
  }

  private void refreshPreview() {
    RegionalSettingsPreview preview = RegionalSettingsPreview.of(currentEffectiveLocale());
    wShortDate.setText(preview.getShortDate());
    wLongDate.setText(preview.getLongDate());
    wShortTime.setText(preview.getShortTime());
    wLongTime.setText(preview.getLongTime());
    wNumber.setText(preview.getNumber());
    wNegativeNumber.setText(preview.getNegativeNumber());
    wCurrency.setText(preview.getCurrency());
    wPercent.setText(preview.getPercent());
  }

  private void save() {
    if (isInitializing) {
      return;
    }

    int index = wDefaultLocale.getSelectionIndex();
    if (index < 0 || index >= GlobalMessages.localeCodes.length) {
      // Code hardening, when the combo-box ever gets in a strange state,
      // use the first language as default (should be English)
      index = 0;
    }
    LanguageChoice.getInstance()
        .setDefaultLocale(EnvUtil.createLocale(GlobalMessages.localeCodes[index]));

    RegionalSettings settings = RegionalSettings.getInstance();
    if (wUseOperatingSystem.getSelection()) {
      settings.setSource(RegionalSettings.Source.OPERATING_SYSTEM);
    } else if (wOverride.getSelection() && selectedLocaleIndex() >= 0) {
      settings.setSource(RegionalSettings.Source.CUSTOM);
      settings.setCustomLocale(localeList.get(selectedLocaleIndex()));
    } else {
      settings.setSource(RegionalSettings.Source.LANGUAGE);
    }
    settings.save();
    settings.applyGui();
  }

  private Combo createComboField(
      Composite parent,
      String labelKey,
      String[] items,
      boolean editable,
      Control lastControl,
      int margin) {
    Label label = new Label(parent, SWT.LEFT);
    PropsUi.setLook(label);
    label.setText(BaseMessages.getString(PKG, labelKey));

    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment(0, 0);
    fdLabel.right = new FormAttachment(100, 0);
    if (lastControl != null) {
      fdLabel.top = new FormAttachment(lastControl, margin);
    } else {
      fdLabel.top = new FormAttachment(0, margin);
    }
    label.setLayoutData(fdLabel);

    Combo combo =
        new Combo(
            parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER | (editable ? SWT.NONE : SWT.READ_ONLY));
    PropsUi.setLook(combo);
    combo.setItems(items);

    FormData fdCombo = new FormData();
    fdCombo.left = new FormAttachment(0, 0);
    fdCombo.right = new FormAttachment(100, 0);
    fdCombo.top = new FormAttachment(label, margin / 2);
    combo.setLayoutData(fdCombo);

    return combo;
  }

  private Button createCheckbox(
      Composite parent,
      String labelKey,
      String tooltipKey,
      boolean selected,
      Control lastControl,
      int margin) {
    Button checkbox = new Button(parent, SWT.CHECK);
    PropsUi.setLook(checkbox);
    checkbox.setText(BaseMessages.getString(PKG, labelKey));
    if (tooltipKey != null) {
      checkbox.setToolTipText(BaseMessages.getString(PKG, tooltipKey));
    }
    checkbox.setSelection(selected);

    FormData fdCheckbox = new FormData();
    fdCheckbox.left = new FormAttachment(0, 0);
    fdCheckbox.right = new FormAttachment(100, 0);
    if (lastControl != null) {
      fdCheckbox.top = new FormAttachment(lastControl, margin);
    } else {
      fdCheckbox.top = new FormAttachment(0, margin);
    }
    checkbox.setLayoutData(fdCheckbox);

    return checkbox;
  }

  private Group createPreviewGroup(
      Composite parent, String labelKey, Control lastControl, int margin) {
    Group group = new Group(parent, SWT.SHADOW_NONE);
    PropsUi.setLook(group);
    group.setText(BaseMessages.getString(PKG, labelKey));

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = PropsUi.getFormMargin();
    groupLayout.marginHeight = PropsUi.getFormMargin();
    group.setLayout(groupLayout);

    FormData fdGroup = new FormData();
    fdGroup.left = new FormAttachment(0, 0);
    fdGroup.right = new FormAttachment(100, 0);
    if (lastControl != null) {
      fdGroup.top = new FormAttachment(lastControl, 2 * margin);
    } else {
      fdGroup.top = new FormAttachment(0, margin);
    }
    group.setLayoutData(fdGroup);

    return group;
  }

  /** Adds a caption and its value to a preview group and returns the label carrying the value. */
  private Label createPreviewRow(Group group, String labelKey, Control lastControl, int margin) {
    Label caption = new Label(group, SWT.RIGHT);
    PropsUi.setLook(caption);
    caption.setText(BaseMessages.getString(PKG, labelKey));

    FormData fdCaption = new FormData();
    fdCaption.left = new FormAttachment(0, 0);
    fdCaption.right = new FormAttachment(PropsUi.getInstance().getMiddlePct(), -margin);
    if (lastControl != null) {
      fdCaption.top = new FormAttachment(lastControl, margin);
    } else {
      fdCaption.top = new FormAttachment(0, margin);
    }
    caption.setLayoutData(fdCaption);

    Label value = new Label(group, SWT.LEFT);
    PropsUi.setLook(value);

    FormData fdValue = new FormData();
    fdValue.left = new FormAttachment(PropsUi.getInstance().getMiddlePct(), 0);
    fdValue.right = new FormAttachment(100, 0);
    fdValue.top = new FormAttachment(caption, 0, SWT.CENTER);
    value.setLayoutData(fdValue);

    return value;
  }
}
