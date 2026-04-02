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
package org.apache.hop.workflow.actions.unzip;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Result;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

class WorkflowActionUnZipTest extends WorkflowActionLoadSaveTestSupport<ActionUnZip> {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @Override
  protected Class<ActionUnZip> getActionClass() {
    return ActionUnZip.class;
  }

  @Override
  protected List<String> ignoreAttributes() {
    return new ArrayList<>(List.of("ifFileExist"));
  }

  @Test
  void unzipPostProcessingTest() throws Exception {

    ActionUnZip jobEntryUnZip = new ActionUnZip();

    Method unzipPostprocessingMethod =
        jobEntryUnZip
            .getClass()
            .getDeclaredMethod(
                "doUnzipPostProcessing",
                FileObject.class,
                FileObject.class,
                String.class,
                Result.class);
    unzipPostprocessingMethod.setAccessible(true);
    FileObject movetodir = Mockito.mock(FileObject.class);
    Mockito.when(movetodir.toString()).thenReturn("file:///dest");

    // delete
    FileObject sourceForDelete = Mockito.mock(FileObject.class);
    Mockito.doReturn(Mockito.mock(FileName.class)).when(sourceForDelete).getName();
    jobEntryUnZip.afterUnzip = 1;
    unzipPostprocessingMethod.invoke(jobEntryUnZip, sourceForDelete, movetodir, "", new Result());
    Mockito.verify(sourceForDelete, Mockito.times(1)).delete();

    // move (bytes written for moved archive size)
    FileObject sourceForMove = Mockito.mock(FileObject.class);
    FileName moveName = Mockito.mock(FileName.class);
    Mockito.when(moveName.getBaseName()).thenReturn("archive.zip");
    Mockito.when(sourceForMove.getName()).thenReturn(moveName);
    FileType moveType = Mockito.mock(FileType.class);
    Mockito.when(sourceForMove.getType()).thenReturn(moveType);
    Mockito.when(moveType.hasContent()).thenReturn(true);
    FileContent moveContent = Mockito.mock(FileContent.class);
    Mockito.when(sourceForMove.getContent()).thenReturn(moveContent);
    Mockito.when(moveContent.getSize()).thenReturn(77L);

    jobEntryUnZip.afterUnzip = 2;
    Result moveResult = new Result();
    unzipPostprocessingMethod.invoke(jobEntryUnZip, sourceForMove, movetodir, "", moveResult);
    Mockito.verify(sourceForMove, Mockito.times(1)).moveTo(Mockito.any());
    assertEquals(77L, moveResult.getBytesWrittenThisAction());
  }
}
