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

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.security.HopDialogEditGuard;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.metadata.util.MetadataMoveStatus;
import org.apache.hop.metadata.util.MetadataMover;
import org.apache.hop.metadata.util.MetadataPlace;
import org.apache.hop.metadata.util.MetadataPlaces;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;

/**
 * Read-only combo at the top of a metadata editor. Choosing another place moves the object there.
 */
public final class MetadataLocationCombo {

  private static final Class<?> PKG = MetadataEditorDialog.class;

  private MetadataLocationCombo() {}

  /**
   * @return the combo to attach the editor under, or null when there is only one place to store
   *     metadata
   */
  public static Control create(Composite parent, MetadataEditor<?> editor) {
    MultiMetadataProvider multi = editor.getHopGui().getMetadataProvider();
    List<MetadataPlace> places = MetadataPlaces.of(multi);
    if (places.size() < 2) {
      return null;
    }

    int margin = PropsUi.getMargin();
    int middle = PropsUi.getInstance().getMiddlePct();

    Label label = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(label);
    label.setText(BaseMessages.getString(PKG, "MetadataEditor.Location.Label"));
    FormData labelData = new FormData();
    labelData.left = new FormAttachment(0, 0);
    labelData.top = new FormAttachment(0, margin);
    labelData.right = new FormAttachment(middle, -margin);
    label.setLayoutData(labelData);

    Combo combo = new Combo(parent, SWT.DROP_DOWN | SWT.READ_ONLY);
    PropsUi.setLook(combo);
    for (MetadataPlace place : places) {
      combo.add(place.label());
    }
    int selected = indexOf(places, editor.getMetadata().getMetadataProviderName());
    if (selected < 0) {
      selected = indexOfActive(places);
    }
    if (selected < 0) {
      selected = places.size() - 1;
    }
    combo.select(selected);
    applyTooltip(combo, places.get(selected));
    if (HopDialogEditGuard.isReadOnly(editor.getMetadata())) {
      combo.setEnabled(false);
    }

    FormData comboData = new FormData();
    comboData.left = new FormAttachment(middle, 0);
    comboData.top = new FormAttachment(label, 0, SWT.CENTER);
    comboData.right = new FormAttachment(100, 0);
    combo.setLayoutData(comboData);

    combo.addListener(SWT.Selection, event -> onSelection(combo, editor, places));
    return combo;
  }

  private static void onSelection(
      Combo combo, MetadataEditor<?> editor, List<MetadataPlace> places) {
    if (combo.getSelectionIndex() < 0 || combo.getSelectionIndex() >= places.size()) {
      return;
    }
    MetadataPlace target = places.get(combo.getSelectionIndex());
    IHopMetadata metadata = editor.getMetadata();
    String current = metadata.getMetadataProviderName();
    if (target.providerDescription().equals(current)) {
      applyTooltip(combo, target);
      return;
    }
    int previous = indexOf(places, current);
    if (previous < 0) {
      previous = indexOfActive(places);
    }
    if (!target.active()) {
      MessageBox confirm = new MessageBox(combo.getShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO);
      confirm.setText(BaseMessages.getString(PKG, "MetadataEditor.Location.Move.Title"));
      confirm.setMessage(
          BaseMessages.getString(
              PKG,
              "MetadataEditor.Location.Move.Message",
              StringUtils.defaultIfEmpty(metadata.getName(), editor.getTitle()),
              target.label()));
      if (confirm.open() != SWT.YES) {
        select(combo, places, previous);
        return;
      }
    }
    try {
      editor.copyWidgetsToMetadata();
    } catch (Exception e) {
      select(combo, places, previous);
      new ErrorDialog(
          combo.getShell(),
          BaseMessages.getString(PKG, "MetadataEditor.Dialog.Ok.Error.Header"),
          BaseMessages.getString(PKG, "MetadataEditor.Location.Error.Message"),
          e);
      return;
    }
    try {
      MetadataMoveStatus status =
          MetadataMover.move(
              editor.getHopGui().getMetadataProvider(),
              metadata,
              target.providerDescription(),
              editor.isPersisted());
      if (status == MetadataMoveStatus.COLLISION) {
        select(combo, places, previous);
        MessageBox box = new MessageBox(combo.getShell(), SWT.ICON_WARNING | SWT.OK);
        box.setText(BaseMessages.getString(PKG, "MetadataEditor.Location.Exists.Title"));
        box.setMessage(
            BaseMessages.getString(
                PKG, "MetadataEditor.Location.Exists.Message", metadata.getName(), target.label()));
        box.open();
        return;
      }
      if (status == MetadataMoveStatus.DELETE_FAILED) {
        MessageBox box = new MessageBox(combo.getShell(), SWT.ICON_WARNING | SWT.OK);
        box.setText(BaseMessages.getString(PKG, "MetadataEditor.Location.DeleteFailed.Title"));
        box.setMessage(
            BaseMessages.getString(
                PKG,
                "MetadataEditor.Location.DeleteFailed.Message",
                metadata.getName(),
                target.label()));
        box.open();
      }
      applyTooltip(combo, target);
      if (status == MetadataMoveStatus.MOVED
          || status == MetadataMoveStatus.RETARGETED
          || status == MetadataMoveStatus.DELETE_FAILED) {
        editor.setChanged();
      }
      if (status == MetadataMoveStatus.MOVED || status == MetadataMoveStatus.DELETE_FAILED) {
        MetadataPerspective perspective = MetadataPerspective.getInstance();
        if (perspective != null) {
          perspective.refresh();
        }
      }
    } catch (HopException e) {
      select(combo, places, previous);
      new ErrorDialog(
          combo.getShell(),
          BaseMessages.getString(PKG, "MetadataEditor.Dialog.Ok.Error.Header"),
          BaseMessages.getString(PKG, "MetadataEditor.Location.Error.Message"),
          e);
    }
  }

  private static void select(Combo combo, List<MetadataPlace> places, int index) {
    if (index >= 0 && index < places.size()) {
      combo.select(index);
      applyTooltip(combo, places.get(index));
    }
  }

  private static void applyTooltip(Combo combo, MetadataPlace place) {
    combo.setToolTipText(place.tooltip() == null ? "" : place.tooltip());
  }

  private static int indexOf(List<MetadataPlace> places, String providerDescription) {
    if (StringUtils.isEmpty(providerDescription)) {
      return -1;
    }
    for (int i = 0; i < places.size(); i++) {
      if (providerDescription.equals(places.get(i).providerDescription())) {
        return i;
      }
    }
    return -1;
  }

  private static int indexOfActive(List<MetadataPlace> places) {
    for (int i = 0; i < places.size(); i++) {
      if (places.get(i).active()) {
        return i;
      }
    }
    return -1;
  }
}
