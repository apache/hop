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

package org.apache.hop.pipeline.transforms.blockuntiltransformsfinish;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BlockUntilTransformsFinishMetaTest {

  @Test
  public void testXmlRoundTrip() throws Exception {
    BlockUntilTransformsFinishMeta meta = new BlockUntilTransformsFinishMeta();
    meta.getBlockingTransforms().add(new BlockingTransform("A", "0"));
    meta.getBlockingTransforms().add(new BlockingTransform("B", "1"));
    meta.getBlockingTransforms().add(new BlockingTransform("C", "2"));
    String xml = meta.getXml();

    String transformXml =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + xml
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    BlockUntilTransformsFinishMeta meta2 = new BlockUntilTransformsFinishMeta();
    meta2.loadXml(XmlHandler.loadXmlString(transformXml, TransformMeta.XML_TAG), null);
  }
}
