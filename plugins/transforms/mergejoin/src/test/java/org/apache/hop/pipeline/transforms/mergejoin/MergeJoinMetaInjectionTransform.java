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

package org.apache.hop.pipeline.transforms.mergejoin;

import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.core.injection.bean.BeanInjectionInfo;
import org.apache.hop.core.injection.bean.BeanInjector;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MergeJoinMetaInjectionTransform {

  @Test
  public void testInjection() throws Exception {
    BeanInjectionInfo<MergeJoinMeta> injectionInfo = new BeanInjectionInfo<>(MergeJoinMeta.class);
    BeanInjector<MergeJoinMeta> injector = new BeanInjector<>(injectionInfo);

    MergeJoinMeta meta = new MergeJoinMeta();

    IRowMeta metaRow =
        new RowMetaBuilder().addString("Left").addString("Right").addString("Type").build();
    List<RowMetaAndData> metaRows =
        Arrays.asList(new RowMetaAndData(metaRow, "left", "right", "INNER"));
    injector.setProperty(meta, "LEFT_TRANSFORM", metaRows, "Left");
    assertEquals("left", meta.getLeftTransformName());
    injector.setProperty(meta, "RIGHT_TRANSFORM", metaRows, "Right");
    assertEquals("right", meta.getRightTransformName());
    injector.setProperty(meta, "JOIN_TYPE", metaRows, "Type");
    assertEquals("INNER", meta.getJoinType());

    IRowMeta keyMeta = new RowMetaBuilder().addString("id").build();
    List<RowMetaAndData> key1Rows =
        Arrays.asList(new RowMetaAndData(keyMeta, "id11"), new RowMetaAndData(keyMeta, "id12"));
    List<RowMetaAndData> key2Rows =
        Arrays.asList(
            new RowMetaAndData(keyMeta, "id21"),
            new RowMetaAndData(keyMeta, "id22"),
            new RowMetaAndData(keyMeta, "id23"));

    injector.setProperty(meta, "KEY_FIELD1", key1Rows, "id");
    injector.setProperty(meta, "KEY_FIELD2", key2Rows, "id");

    assertEquals(2, meta.getKeyFields1().size());
    assertEquals("id11", meta.getKeyFields1().get(0));
    assertEquals("id12", meta.getKeyFields1().get(1));
    assertEquals(3, meta.getKeyFields2().size());
    assertEquals("id21", meta.getKeyFields2().get(0));
    assertEquals("id22", meta.getKeyFields2().get(1));
    assertEquals("id23", meta.getKeyFields2().get(2));
  }
}
