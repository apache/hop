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

package org.apache.hop.ui.core.widget;

import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.hopgui.HopGui;

/** Process-wide {@link INamingSchemeColumnApplier} (usually the naming plugin). */
public final class NamingSchemeColumnApplierRegistry {

  private static final NamingSchemeColumnApplierRegistry INSTANCE =
      new NamingSchemeColumnApplierRegistry();

  private volatile INamingSchemeColumnApplier applier;

  private NamingSchemeColumnApplierRegistry() {
    // singleton
  }

  public static NamingSchemeColumnApplierRegistry getInstance() {
    return INSTANCE;
  }

  public void register(INamingSchemeColumnApplier columnApplier) {
    this.applier = columnApplier;
  }

  public void unregister() {
    this.applier = null;
  }

  /**
   * Apply naming schemes to annotated columns. No-op when the naming plugin is absent.
   *
   * @param tableView fields table
   * @param provider metadata provider (Hop Gui's is used when null)
   * @param schemeName optional explicit scheme
   */
  public void applyAnnotatedColumns(
      TableView tableView, IHopMetadataProvider provider, String schemeName) {
    INamingSchemeColumnApplier current = applier;
    if (current == null || tableView == null || tableView.isDisposed()) {
      return;
    }
    IHopMetadataProvider meta = provider;
    if (meta == null) {
      try {
        meta = HopGui.getInstance().getMetadataProvider();
      } catch (Exception e) {
        return;
      }
    }
    current.applyAnnotatedColumns(tableView, meta, schemeName);
  }
}
