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
package org.apache.hop.pipeline.transforms.filterrows;

import com.google.common.collect.ImmutableList;
import org.apache.hop.core.Condition;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ConditionLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class FilterMetaTest {

  @Test
  public void testXmlRoundTrip() throws Exception {
    FilterMeta meta1 = new FilterMeta();
    meta1.setCondition("A>100");
    meta1.setTrueTransformName("true");
    meta1.setFalseTransformName("false");

    String xml1 = meta1.getXml();

    FilterMeta meta2 = new FilterMeta();
    meta2.loadXml(XmlHandler.wrapLoadXmlString(xml1), null);

    assertEquals(meta1.getCondition(), meta2.getCondition());
    assertEquals(meta1.getTrueTransformName(), meta2.getTrueTransformName());
    assertEquals(meta1.getFalseTransformName(), meta2.getFalseTransformName());

    assertEquals(meta1.getXml(), meta2.getXml());
  }

  @Test
  public void testClone() {
    FilterMeta filterMeta = new FilterMeta();
    filterMeta.setCondition("A>100");
    filterMeta.setTrueTransformName("true");
    filterMeta.setFalseTransformName("false");

    FilterMeta clone = filterMeta.clone();
    assertEquals("A>100", clone.getCondition());
    assertEquals("true", clone.getTrueTransformName());
    assertEquals("false", clone.getFalseTransformName());
  }
}
