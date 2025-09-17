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

package org.apache.hop.pipeline.transforms.synchronizeaftermerge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.NoneDatabaseMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class SynchronizeAfterMergeTest {

  private static final String TRANSFORM_NAME = "Sync";

  @Disabled("This test needs to be reviewed")
  @Test
  void initWithCommitSizeVariable() {
    TransformMeta transformMeta = mock(TransformMeta.class);
    doReturn(TRANSFORM_NAME).when(transformMeta).getName();
    doReturn(1).when(transformMeta).getCopies(any(IVariables.class));

    SynchronizeAfterMergeMeta smi = mock(SynchronizeAfterMergeMeta.class);
    SynchronizeAfterMergeData sdi = mock(SynchronizeAfterMergeData.class);

    DatabaseMeta dbMeta = mock(DatabaseMeta.class);
    doReturn(mock(NoneDatabaseMeta.class)).when(dbMeta).getIDatabase();

    // doReturn(dbMeta).when(smi).getDatabaseMeta();
    doReturn("120").when(smi).getCommitSize();

    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    doReturn(transformMeta).when(pipelineMeta).findTransform(TRANSFORM_NAME);

    SynchronizeAfterMerge transform =
        new SynchronizeAfterMerge(transformMeta, smi, sdi, 0, pipelineMeta, mock(Pipeline.class));

    transform.init();

    assertEquals(120, sdi.commitSize);
  }
}
