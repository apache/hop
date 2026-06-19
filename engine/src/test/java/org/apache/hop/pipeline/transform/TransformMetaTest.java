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

package org.apache.hop.pipeline.transform;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.pipeline.transform.transforms.FakeMeta;
import org.apache.hop.pipeline.transforms.missing.Missing;
import org.apache.hop.utils.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TransformMetaTest {

  private static final IVariables variables = new Variables();
  private static final Random rand = new Random();

  private static final String TRANSFORM_ID = "transform_id";

  @Test
  void cloning() throws Exception {
    TransformMeta meta = createTestMeta();
    TransformMeta clone = (TransformMeta) meta.clone();
    assertEquals(meta, clone);
  }

  @Test
  void testEqualsHashCodeConsistency() {
    TransformMeta transform = new TransformMeta();
    transform.setName("transform");
    TestUtils.checkEqualsHashCodeConsistency(transform, transform);

    TransformMeta transformSame = new TransformMeta();
    transformSame.setName("transform");
    assertEquals(transform, transformSame);
    TestUtils.checkEqualsHashCodeConsistency(transform, transformSame);

    TransformMeta transformCaps = new TransformMeta();
    transformCaps.setName("TRANSFORM");
    TestUtils.checkEqualsHashCodeConsistency(transform, transformCaps);

    TransformMeta transformOther = new TransformMeta();
    transformOther.setName("something else");
    TestUtils.checkEqualsHashCodeConsistency(transform, transformOther);
  }

  @Test
  void transformMetaXmlConsistency() throws Exception {
    TransformMeta meta = new TransformMeta("id", "name", null);
    ITransformMeta smi = new Missing(meta.getName(), meta.getTransformPluginId());
    meta.setTransform(smi);
    TransformMeta fromXml = TransformMeta.fromXml(meta.getXml());
    Assertions.assertEquals(fromXml.getXml(), meta.getXml());
  }

  private static TransformMeta createTestMeta() throws Exception {
    ITransformMeta transformMetaInterface = mock(ITransformMeta.class);
    when(transformMetaInterface.clone()).thenReturn(transformMetaInterface);

    TransformMeta meta = new TransformMeta(TRANSFORM_ID, "transformName", transformMetaInterface);
    meta.setSelected(true);
    meta.setDistributes(false);
    meta.setCopiesString("2");
    meta.setLocation(1, 2);
    meta.setDescription("description");
    meta.setTerminator(true);

    boolean shouldDistribute = rand.nextBoolean();
    meta.setDistributes(shouldDistribute);
    if (shouldDistribute) {
      meta.setRowDistribution(selectRowDistribution());
    }

    Map<String, Map<String, String>> attributes = new HashMap<>();
    Map<String, String> map1 = new HashMap<>();
    map1.put("1", "1");
    Map<String, String> map2 = new HashMap<>();
    map2.put("2", "2");

    attributes.put("qwerty", map1);
    attributes.put("asdfg", map2);
    meta.setAttributesMap(attributes);

    meta.setTransformPartitioningMeta(
        createTransformPartitioningMeta("transformMethod", "transformSchema"));
    meta.setTargetTransformPartitioningMeta(
        createTransformPartitioningMeta("targetMethod", "targetSchema"));

    return meta;
  }

  private static IRowDistribution selectRowDistribution() {
    return new FakeRowDistribution();
  }

  private static TransformPartitioningMeta createTransformPartitioningMeta(
      String method, String schemaName) throws Exception {
    TransformPartitioningMeta meta =
        new TransformPartitioningMeta(
            method, new PartitionSchema(schemaName, Collections.<String>emptyList()));
    meta.setPartitionSchema(new PartitionSchema());
    return meta;
  }

  private static void assertEquals(TransformMeta meta, TransformMeta another) {
    assertTrue(
        EqualsBuilder.reflectionEquals(
            meta,
            another,
            false,
            TransformMeta.class,
            new String[] {"location", "targetTransformPartitioningMeta"}));

    boolean manualCheck =
        new EqualsBuilder()
            .append(meta.getLocation().x, another.getLocation().x)
            .append(meta.getLocation().y, another.getLocation().y)
            .isEquals();
    assertTrue(manualCheck);
  }

  @Test
  void testSerialization() throws Exception {
    PluginRegistry registry = PluginRegistry.getInstance();
    registry.registerPluginClass(
        FakeMeta.class.getName(), TransformPluginType.class, Transform.class);

    IHopMetadataProvider metadataProvider = new MemoryMetadataProvider();
    IHopMetadataSerializer<PartitionSchema> schemaSerializer =
        metadataProvider.getSerializer(PartitionSchema.class);
    PartitionSchema four = new PartitionSchema();
    four.setName("four");
    four.setDynamicallyDefined(true);
    four.setNumberOfPartitions("4");
    schemaSerializer.save(four);

    TransformMeta source = new TransformMeta("T1", new FakeMeta());
    source.setDescription("description1");
    source.setDistributes(true);
    source.setTransformPartitioningMeta(new TransformPartitioningMeta("ModPartitioner", four));
    source.setLocation(101, 102);
    source.setCopies(3);

    // Set some extra attributes as well
    //
    Map<String, Map<String, String>> attributesMap = source.getAttributesMap();
    Map<String, String> group1Map = attributesMap.computeIfAbsent("group1", f -> new HashMap<>());
    group1Map.put("key-1-1", "value-1-1");
    group1Map.put("key-1-2", "value-1-2");
    Map<String, String> group2Map = attributesMap.computeIfAbsent("group2", f -> new HashMap<>());
    group2Map.put("key-2-1", "value-2-1");
    group2Map.put("key-2-2", "value-2-2");

    String xml = XmlMetadataUtil.serializeObjectToXml(source);
    TransformMeta copy =
        XmlMetadataUtil.deSerializeFromXml(
            XmlHandler.loadXmlString(XmlHandler.aroundTag("hop", xml), "hop"),
            TransformMeta.class,
            metadataProvider);

    Assertions.assertEquals("T1", copy.getName());
    Assertions.assertEquals("description1", copy.getDescription());
    Assertions.assertEquals("3", copy.getCopiesString());
    Assertions.assertTrue(copy.isDistributes());
    Assertions.assertEquals(101, copy.getLocation().x);
    Assertions.assertEquals(102, copy.getLocation().y);
    TransformPartitioningMeta transformPartitioningMeta = copy.getTransformPartitioningMeta();
    Assertions.assertNotNull(transformPartitioningMeta);
    Assertions.assertEquals("four", transformPartitioningMeta.getPartitionSchema().getName());

    // Validate the attributes map
    //
    Map<String, Map<String, String>> copyAttributesMap = copy.getAttributesMap();
    Assertions.assertEquals(2, copyAttributesMap.size());
    Map<String, String> group1 = copyAttributesMap.get("group1");
    Assertions.assertEquals("value-1-1", group1.get("key-1-1"));
    Assertions.assertEquals("value-1-2", group1.get("key-1-2"));
    Map<String, String> group2 = copyAttributesMap.get("group2");
    Assertions.assertEquals("value-2-1", group2.get("key-2-1"));
    Assertions.assertEquals("value-2-2", group2.get("key-2-2"));
  }
}
