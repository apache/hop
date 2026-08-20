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

package org.apache.hop.naming.gui;

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.naming.engine.NamingEngine;
import org.apache.hop.naming.metadata.NamingScheme;
import org.apache.hop.naming.metadata.NamingSchemeSelector;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.INamingSchemeColumnApplier;
import org.apache.hop.ui.core.widget.TableView;
import org.eclipse.swt.widgets.TableItem;

/** Applies a naming scheme to TableView columns that declared a naming type (Get Fields). */
public final class NamingSchemeColumnApplier implements INamingSchemeColumnApplier {

  static final NamingSchemeColumnApplier INSTANCE = new NamingSchemeColumnApplier();

  private NamingSchemeColumnApplier() {
    // singleton used from HopGuiStart
  }

  @Override
  public void applyAnnotatedColumns(
      TableView tableView, IHopMetadataProvider provider, String schemeName) {
    if (tableView == null || tableView.isDisposed() || provider == null) {
      return;
    }
    ColumnInfo[] columns = tableView.getColumns();
    if (columns == null || columns.length == 0) {
      return;
    }
    try {
      IHopMetadataSerializer<NamingScheme> serializer = provider.getSerializer(NamingScheme.class);
      List<NamingScheme> all = serializer.loadAll();
      int offset = tableView.hasIndexColumn() ? 1 : 0;
      for (int i = 0; i < columns.length; i++) {
        String typeCode = columns[i].getNamingSchemeType();
        if (StringUtils.isEmpty(typeCode)) {
          continue;
        }
        NamingScheme scheme = NamingSchemeSelector.resolve(all, typeCode, schemeName);
        if (scheme == null) {
          continue;
        }
        applyColumn(tableView, i + offset, scheme);
      }
    } catch (Exception e) {
      // Get Fields should still succeed when metadata is unavailable
    }
  }

  private static void applyColumn(TableView tableView, int colNr, NamingScheme scheme) {
    List<TableItem> items = tableView.getNonEmptyItems();
    if (items.isEmpty()) {
      return;
    }
    int[] rowIndices = new int[items.size()];
    String[] newValues = new String[items.size()];
    boolean changed = false;
    for (int i = 0; i < items.size(); i++) {
      TableItem item = items.get(i);
      rowIndices[i] = tableView.getTable().indexOf(item);
      String current = item.getText(colNr);
      if (NamingSchemeShortcut.shouldSkip(current)) {
        newValues[i] = current;
      } else {
        String rewritten = NamingEngine.apply(scheme, current);
        newValues[i] = rewritten != null ? rewritten : current;
        changed |= !current.equals(newValues[i]);
      }
    }
    if (changed) {
      tableView.applyColumnValues(colNr, rowIndices, newValues);
    }
  }
}
