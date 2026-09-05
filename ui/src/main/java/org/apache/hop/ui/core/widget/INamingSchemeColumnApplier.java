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

/**
 * Plugin hook that rewrites TableView name columns after Get Fields. {@code ui} must not depend on
 * the naming plugin; the plugin registers an implementation at Hop Gui start.
 */
public interface INamingSchemeColumnApplier {

  /**
   * Apply a naming scheme to every column that declared a {@code namingSchemeType}.
   *
   * @param tableView fields table
   * @param provider metadata (may be null)
   * @param schemeName optional explicit scheme name; when empty the unique matching scheme (type
   *     specific or General) is used. Multiple matches mean no automatic apply.
   */
  void applyAnnotatedColumns(TableView tableView, IHopMetadataProvider provider, String schemeName);
}
