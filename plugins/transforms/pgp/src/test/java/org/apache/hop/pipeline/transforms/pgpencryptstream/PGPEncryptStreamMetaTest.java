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
package org.apache.hop.pipeline.transforms.pgpencryptstream;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PGPEncryptStreamMetaTest {
  @BeforeEach
  void setUp() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    PGPEncryptStreamMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/pgp-encrypt.xml", PGPEncryptStreamMeta.class);

    Assertions.assertEquals("/usr/bin/gpg", meta.getGpgLocation());
    Assertions.assertEquals("key-name", meta.getKeyName());
    Assertions.assertTrue(meta.isKeyNameInField());
    Assertions.assertEquals("keyNameField", meta.getKeyNameFieldName());
    Assertions.assertEquals("streamField", meta.getStreamField());
    Assertions.assertEquals("result", meta.getResultFieldName());
  }

  @Test
  void checkReportsEveryValidSettingAsOk() {
    PGPEncryptStreamMeta meta = new PGPEncryptStreamMeta();
    meta.setGpgLocation("/usr/bin/gpg");
    meta.setKeyName("key-name");
    meta.setStreamField("streamField");
    meta.setResultFieldName("result");
    List<ICheckResult> remarks = new ArrayList<>();

    meta.check(
        remarks,
        new PipelineMeta(),
        new TransformMeta("pgp", meta),
        new RowMeta(),
        new String[] {"input"},
        new String[0],
        new RowMeta(),
        new Variables(),
        null);

    // GPG location, key name, result field, stream field and the input transform
    Assertions.assertEquals(5, remarks.size());
    remarks.forEach(
        remark ->
            Assertions.assertEquals(
                ICheckResult.TYPE_RESULT_OK, remark.getType(), remark.getText()));
  }
}
