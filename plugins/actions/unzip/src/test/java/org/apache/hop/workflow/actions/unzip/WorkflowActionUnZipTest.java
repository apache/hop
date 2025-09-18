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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
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
                "doUnzipPostProcessing", FileObject.class, FileObject.class, String.class);
    unzipPostprocessingMethod.setAccessible(true);
    FileObject sourceFileObject = Mockito.mock(FileObject.class);
    Mockito.doReturn(Mockito.mock(FileName.class)).when(sourceFileObject).getName();

    // delete
    jobEntryUnZip.afterUnzip = 1;
    unzipPostprocessingMethod.invoke(
        jobEntryUnZip, sourceFileObject, Mockito.mock(FileObject.class), "");
    Mockito.verify(sourceFileObject, Mockito.times(1)).delete();

    // move
    jobEntryUnZip.afterUnzip = 2;
    unzipPostprocessingMethod.invoke(
        jobEntryUnZip, sourceFileObject, Mockito.mock(FileObject.class), "");
    Mockito.verify(sourceFileObject, Mockito.times(1)).moveTo(Mockito.any());
  }
}
