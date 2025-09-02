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

package org.apache.hop.pipeline.transforms.loadfileinput;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;
import javax.tools.FileObject;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

/**
 * Tests for LoadFileInputMeta class
 *
 * @see LoadFileInputMeta
 */
class PDI_6976_Test {
  @Test
  void testVerifyNoPreviousTransform() {
    LoadFileInputMeta spy = spy(new LoadFileInputMeta());

    FileInputList fileInputList = mock(FileInputList.class);
    List<FileObject> files = when(mock(List.class).size()).thenReturn(1).getMock();
    doReturn(files).when(fileInputList).getFiles();
    doReturn(fileInputList).when(spy).getFiles(any(IVariables.class));

    List<ICheckResult> validationResults = mock(List.class);

    // Check we do not get validation errors
    doAnswer(
            (Answer<Object>)
                invocation -> {
                  if (((ICheckResult) invocation.getArguments()[0]).getType()
                      != ICheckResult.TYPE_RESULT_OK) {
                    fail("We've got validation error");
                  }

                  return null;
                })
        .when(validationResults)
        .add(any(ICheckResult.class));

    spy.check(
        validationResults,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        mock(IRowMeta.class),
        new String[] {},
        new String[] {"File content", "File size"},
        mock(IRowMeta.class),
        mock(IVariables.class),
        mock(IHopMetadataProvider.class));
  }
}
