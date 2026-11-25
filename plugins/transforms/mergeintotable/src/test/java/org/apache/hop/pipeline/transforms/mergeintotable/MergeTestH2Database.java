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

import lombok.Setter;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.batch.BatchFailureContract;
import org.apache.hop.core.database.dml.DmlSyntax;
import org.apache.hop.databases.h2.H2DatabaseMeta;

/**
 * H2 with a MERGE syntax chosen by the test, so the transform is tested against what it is given
 * and not against whatever the H2 dialect itself declares. Null means "no MERGE".
 */
@DatabaseMetaPlugin(
    type = MergeTestH2Database.TYPE,
    typeDescription = "H2 with a test MERGE syntax")
public class MergeTestH2Database extends H2DatabaseMeta {

  static final String TYPE = "MERGE_TEST_H2";

  private DmlSyntax syntax;

  public MergeTestH2Database() {
    this(null);
  }

  public MergeTestH2Database(DmlSyntax syntax) {
    this.syntax = syntax;
  }

  @Override
  public DmlSyntax getDmlSyntax() {
    return syntax;
  }

  /** The batch failure contract the test gives the connection; null: not validated. */
  @Setter private BatchFailureContract contract = BatchFailureContract.MARKS_FAILED_ROWS;

  /** H2 marks the failed rows of a batch, unless the test says otherwise. */
  @Override
  public BatchFailureContract getBatchFailureContract() {
    return contract;
  }

  /** Pretend to be a database whose failed statement aborts the transaction, as PostgreSQL. */
  @Setter private boolean useSafePoints;

  @Override
  public boolean isUseSafePoints() {
    return useSafePoints || super.isUseSafePoints();
  }
}
