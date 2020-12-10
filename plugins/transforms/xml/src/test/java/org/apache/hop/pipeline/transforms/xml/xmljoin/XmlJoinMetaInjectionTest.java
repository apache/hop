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

package org.apache.hop.pipeline.transforms.xml.xmljoin;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by ecuellar on 3/3/2016.
 */
public class XmlJoinMetaInjectionTest extends BaseMetadataInjectionTest<XmlJoinMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() {

    try{
      setup( new XmlJoinMeta() );
    }catch(Exception e){

    }
  }

  @Test
  @Ignore
  public void test() throws Exception {

    check( "COMPLEX_JOIN", () -> meta.isComplexJoin() );

    check( "TARGET_XML_TRANSFORM", () -> meta.getTargetXmlTransform() );

    check( "TARGET_XML_FIELD", () -> meta.getTargetXmlField() );

    check( "SOURCE_XML_FIELD", () -> meta.getSourceXmlField() );

    check( "VALUE_XML_FIELD", () -> meta.getValueXmlField() );

    check( "TARGET_XPATH", () -> meta.getTargetXPath() );

    check( "SOURCE_XML_TRANSFORM", () -> meta.getSourceXmlTransform() );

    check( "JOIN_COMPARE_FIELD", () -> meta.getJoinCompareField() );

    check( "ENCODING", () -> meta.getEncoding() );

    check( "OMIT_XML_HEADER", () -> meta.isOmitXmlHeader() );

    check( "OMIT_NULL_VALUES", () -> meta.isOmitNullValues() );
  }
}
