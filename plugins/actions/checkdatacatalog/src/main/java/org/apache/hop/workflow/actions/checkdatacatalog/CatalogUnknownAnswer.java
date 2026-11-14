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

package org.apache.hop.workflow.actions.checkdatacatalog;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/**
 * What {@link ActionCheckDataCatalog} does when the catalog can't answer a check: it can't be
 * reached, refuses the token, doesn't support the check, or knows nothing to answer it with.
 */
public enum CatalogUnknownAnswer implements IEnumHasCodeAndDescription {
  /** The check fails. */
  FAIL,
  /** The check is skipped, with a line in the log. */
  IGNORE;

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getDescription() {
    return BaseMessages.getString(
        ActionCheckDataCatalog.class, "ActionCheckDataCatalog.UnknownAnswer." + name());
  }
}
