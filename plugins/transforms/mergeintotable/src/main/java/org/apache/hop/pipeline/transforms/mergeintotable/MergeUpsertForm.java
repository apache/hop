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

package org.apache.hop.pipeline.transforms.mergeintotable;

import org.apache.hop.core.database.dml.UpsertForm;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/**
 * The statement Merge into table writes: the dialect's preference, or one upsert form forced. The
 * code is stored in the transform's metadata.
 */
public enum MergeUpsertForm implements IEnumHasCodeAndDescription {
  /** The first form the dialect declares that this connection and table qualify for. */
  AUTOMATIC(null),
  MERGE(UpsertForm.MERGE),
  ON_CONFLICT(UpsertForm.ON_CONFLICT),
  ON_DUPLICATE_KEY(UpsertForm.ON_DUPLICATE_KEY);

  private static final Class<?> PKG = MergeUpsertForm.class;

  private final UpsertForm form;

  MergeUpsertForm(UpsertForm form) {
    this.form = form;
  }

  /**
   * @return the upsert form to ask for, or null to let the dialect decide
   */
  public UpsertForm getUpsertForm() {
    return form;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getDescription() {
    return BaseMessages.getString(PKG, "MergeUpsertForm." + name() + ".Description");
  }

  /**
   * @return the form of a meta, AUTOMATIC when none is set
   */
  public static UpsertForm upsertFormOf(MergeUpsertForm form) {
    return form == null ? null : form.getUpsertForm();
  }
}
