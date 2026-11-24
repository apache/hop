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

package org.apache.hop.pipeline.transforms.dbproc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hop.core.database.Database;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** What {@link DBProc#dispose()} does with the work of the stored procedures it called. */
class DBProcDisposeTest {

  private TransformMockHelper<DBProcMeta, DBProcData> helper;
  private DBProcMeta meta;
  private DBProcData data;
  private Database db;
  private DBProc transform;

  @BeforeEach
  void setUp() {
    helper = new TransformMockHelper<>("Call DB procedure", DBProcMeta.class, DBProcData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    meta = new DBProcMeta();
    meta.setAutoCommit(false);
    data = new DBProcData();
    db = mock(Database.class);
    data.db = db;
    transform =
        new DBProc(helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void aCleanRunCommits() throws Exception {
    transform.dispose();

    verify(db).commit();
    verify(db, never()).rollback();
    verify(db).disconnect();
  }

  @Test
  void aFailedRunRollsBackInsteadOfCommitting() throws Exception {
    transform.setErrors(1);

    transform.dispose();

    verify(db, never()).commit();
    verify(db).rollback();
    verify(db).disconnect();
  }

  /**
   * A stop without an error, such as an Abort without "abort with error", ends the pipeline as a
   * success: what ran is committed, as Table output, Update and Delete do.
   */
  @Test
  void aStoppedRunWithoutErrorsCommits() throws Exception {
    transform.setStopped(true);

    transform.dispose();

    verify(db).commit();
    verify(db, never()).rollback();
  }

  @Test
  void aStoppedRunWithAnErrorRollsBack() throws Exception {
    transform.setStopped(true);
    transform.setErrors(1);

    transform.dispose();

    verify(db, never()).commit();
    verify(db).rollback();
  }

  @Test
  void autoCommitLeavesTheTransactionAlone() throws Exception {
    meta.setAutoCommit(true);
    transform.setErrors(1);

    transform.dispose();

    verify(db, never()).commit();
    verify(db, never()).rollback();
  }
}
