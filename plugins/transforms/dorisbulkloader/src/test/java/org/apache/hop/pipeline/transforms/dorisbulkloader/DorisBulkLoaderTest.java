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

package org.apache.hop.pipeline.transforms.dorisbulkloader;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Assert;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class DorisBulkLoaderTest {

  private static boolean canWrite = true;

  @Disabled("This test needs to be reviewed")
  @Test
  void testCallProcessStreamLoadWithOneBatch() throws Exception {
    DorisBulkLoaderMeta meta = mock(DorisBulkLoaderMeta.class);
    doReturn(40).when(meta).getBufferSize();
    doReturn(2).when(meta).getBufferCount();
    doReturn("json").when(meta).getFormat();

    DorisBulkLoaderData data = mock(DorisBulkLoaderData.class);
    IRowMeta rmi = mock(IRowMeta.class);
    data.inputRowMeta = rmi;
    data.dorisStreamLoad = null;

    DorisBulkLoader dorisBulkLoader = mock(DorisBulkLoader.class);
    doCallRealMethod().when(dorisBulkLoader).processStreamLoad(anyString(), anyBoolean());
    doReturn("xxx").when(dorisBulkLoader).resolve(anyString());

    dorisBulkLoader.processStreamLoad("{\"no\":1, \"name\":\"tom\", \"sex\":\"m\"}", true);

    Assert.assertTrue(data.dorisStreamLoad != null, "data.dorisStreamLoad initialization failure");

    data.dorisStreamLoad = mock(DorisStreamLoad.class);

    doCallRealMethod().when(dorisBulkLoader).processStreamLoad(any(), anyBoolean());
    dorisBulkLoader.processStreamLoad(null, false);

    verify(data.dorisStreamLoad, times(1)).executeDorisStreamLoad();
  }

  @Disabled("This test needs to be reviewed")
  @Test
  void testCallProcessStreamLoadWithTwoBatch() throws Exception {
    DorisBulkLoaderMeta meta = mock(DorisBulkLoaderMeta.class);
    doReturn(40).when(meta).getBufferSize();
    doReturn(2).when(meta).getBufferCount();
    doReturn("json").when(meta).getFormat();

    DorisBulkLoaderData data = mock(DorisBulkLoaderData.class);
    IRowMeta rmi = mock(IRowMeta.class);
    data.inputRowMeta = rmi;
    data.dorisStreamLoad = null;

    DorisBulkLoader dorisBulkLoader = mock(DorisBulkLoader.class);
    doCallRealMethod().when(dorisBulkLoader).processStreamLoad(anyString(), anyBoolean());
    doReturn("xxx").when(dorisBulkLoader).resolve(anyString());

    dorisBulkLoader.processStreamLoad("{\"no\":1, \"name\":\"tom\", \"sex\":\"m\"}", true);

    Assert.assertTrue(data.dorisStreamLoad != null, "data.dorisStreamLoad initialization failure");

    data.dorisStreamLoad = mock(DorisStreamLoad.class);
    when(data.dorisStreamLoad.canWrite(anyLong()))
        .thenAnswer(
            x -> {
              canWrite = !canWrite;
              return canWrite;
            });

    dorisBulkLoader.processStreamLoad("{\"no\":2, \"name\":\"jack\", \"sex\":\"m\"}", false);

    doCallRealMethod().when(dorisBulkLoader).processStreamLoad(any(), anyBoolean());
    dorisBulkLoader.processStreamLoad(null, false);

    verify(data.dorisStreamLoad, times(2)).executeDorisStreamLoad();
  }
}
