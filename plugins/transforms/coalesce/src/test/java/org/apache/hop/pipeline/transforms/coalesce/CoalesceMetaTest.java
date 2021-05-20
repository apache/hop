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

package org.apache.hop.pipeline.transforms.coalesce;

import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.injection.bean.BeanInjectionInfo;
import org.apache.hop.core.injection.bean.BeanInjector;
import org.apache.hop.core.injection.bean.BeanLevelInfo;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMetaBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CoalesceMetaTest {

  @Test
  public void testInjectionMetadata() throws Exception {

    BeanInjectionInfo<CoalesceMeta> injectionInfo = new BeanInjectionInfo<>(CoalesceMeta.class);

    BeanInjectionInfo<CoalesceMeta>.Property prop = injectionInfo.getProperties().get("NAME");
    assertNotNull(prop);

    assertEquals(3, prop.getPath().size());
    BeanLevelInfo info = prop.getPath().get(1);
    assertEquals(BeanLevelInfo.DIMENSION.LIST, info.dim);
    assertEquals(CoalesceField.class, info.leafClass);

    info = prop.getPath().get(2);
    assertEquals(String.class, info.leafClass);
  }

  @Test
  public void testInjection() throws Exception {
    BeanInjectionInfo<CoalesceMeta> injectionInfo = new BeanInjectionInfo<>(CoalesceMeta.class);
    BeanInjector<CoalesceMeta> injector = new BeanInjector<>(injectionInfo);

    IRowMeta resultMeta =
        new RowMetaBuilder()
            .addString("name")
            .addString("type")
            .addString("remove?")
            .addString("inputs")
            .build();
    List<RowMetaAndData> resultRows =
        Arrays.asList(
            new RowMetaAndData(resultMeta, "result1", "String", "false", "A,B,C"),
            new RowMetaAndData(resultMeta, "result2", "String", "true", "D,E,F"),
            new RowMetaAndData(resultMeta, "result3", "String", "false", "G,H"));

    CoalesceMeta meta = new CoalesceMeta();
    injector.setProperty(meta, "NAME", resultRows, "name");
    injector.setProperty(meta, "TYPE", resultRows, "type");
    injector.setProperty(meta, "INPUT_FIELDS", resultRows, "inputs");
    injector.setProperty(meta, "REMOVE_INPUT_FIELDS", resultRows, "remove?");

    assertEquals(3, meta.getFields().size());
    assertEquals("result1", meta.getFields().get(0).getName());
  }
}
