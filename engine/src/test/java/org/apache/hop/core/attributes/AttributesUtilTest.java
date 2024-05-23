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

package org.apache.hop.core.attributes;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;

import java.util.HashMap;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.w3c.dom.Node;

public class AttributesUtilTest {

  private static MockedStatic<AttributesUtil> mockedAttributesUtil;

  private static final String CUSTOM_TAG = "customTag";
  private static final String A_KEY = "aKEY";
  private static final String A_VALUE = "aVALUE";
  private static final String A_GROUP = "attributesGroup";

  @BeforeClass
  public static void setUpStaticMocks() {
    mockedAttributesUtil = Mockito.mockStatic(AttributesUtil.class);
  }

  @AfterClass
  public static void tearDownStaticMocks() {
    mockedAttributesUtil.close();
  }

  @Test
  public void testGetAttributesXml_DefaultTag() {

    mockedAttributesUtil
        .when(() -> AttributesUtil.getAttributesXml(any(Map.class)))
        .thenCallRealMethod();
    mockedAttributesUtil
        .when(() -> AttributesUtil.getAttributesXml(any(Map.class), anyString()))
        .thenCallRealMethod();

    Map<String, String> attributesGroup = new HashMap<>();
    Map<String, Map<String, String>> attributesMap = new HashMap<>();
    attributesGroup.put(A_KEY, A_VALUE);
    attributesMap.put(A_GROUP, attributesGroup);

    String attributesXml = AttributesUtil.getAttributesXml(attributesMap);

    assertNotNull(attributesXml);

    // The default tag was used
    assertTrue(attributesXml.contains(AttributesUtil.XML_TAG));

    // The group is present
    assertTrue(attributesXml.contains(A_GROUP));

    // Both Key and Value are present
    assertTrue(attributesXml.contains(A_KEY));
    assertTrue(attributesXml.contains(A_VALUE));

    // Verify that getAttributesXml was invoked once (and with the right parameters)
    AttributesUtil.getAttributesXml(attributesMap, AttributesUtil.XML_TAG);
  }

  @Test
  public void testGetAttributesXml_CustomTag() {

    mockedAttributesUtil
        .when(() -> AttributesUtil.getAttributesXml(any(Map.class), anyString()))
        .thenCallRealMethod();

    Map<String, String> attributesGroup = new HashMap<>();
    Map<String, Map<String, String>> attributesMap = new HashMap<>();
    attributesGroup.put(A_KEY, A_VALUE);
    attributesMap.put(A_GROUP, attributesGroup);

    String attributesXml = AttributesUtil.getAttributesXml(attributesMap, CUSTOM_TAG);

    assertNotNull(attributesXml);

    // The custom tag was used
    assertTrue(attributesXml.contains(CUSTOM_TAG));

    // The group is present
    assertTrue(attributesXml.contains(A_GROUP));

    // Both Key and Value are present
    assertTrue(attributesXml.contains(A_KEY));
    assertTrue(attributesXml.contains(A_VALUE));
  }

  @Test
  public void testGetAttributesXml_DefaultTag_NullParameter() {

    mockedAttributesUtil
        .when(() -> AttributesUtil.getAttributesXml(nullable(Map.class)))
        .thenCallRealMethod();
    mockedAttributesUtil
        .when(() -> AttributesUtil.getAttributesXml(nullable(Map.class), nullable(String.class)))
        .thenCallRealMethod();

    String attributesXml = AttributesUtil.getAttributesXml(null);

    assertNotNull(attributesXml);

    // Check that it's not an empty XML fragment
    assertTrue(attributesXml.contains(AttributesUtil.XML_TAG));
  }

  @Test
  public void testGetAttributesXml_CustomTag_NullParameter() {

    mockedAttributesUtil
        .when(() -> AttributesUtil.getAttributesXml(nullable(Map.class), nullable(String.class)))
        .thenCallRealMethod();

    String attributesXml = AttributesUtil.getAttributesXml(null, CUSTOM_TAG);

    assertNotNull(attributesXml);

    // Check that it's not an empty XML fragment
    assertTrue(attributesXml.contains(CUSTOM_TAG));
  }

  @Test
  public void testGetAttributesXml_DefaultTag_EmptyMap() {

    mockedAttributesUtil
        .when(() -> AttributesUtil.getAttributesXml(any(Map.class)))
        .thenCallRealMethod();
    mockedAttributesUtil
        .when(() -> AttributesUtil.getAttributesXml(any(Map.class), anyString()))
        .thenCallRealMethod();

    Map<String, Map<String, String>> attributesMap = new HashMap<>();

    String attributesXml = AttributesUtil.getAttributesXml(attributesMap);

    assertNotNull(attributesXml);

    // Check that it's not an empty XML fragment
    assertTrue(attributesXml.contains(AttributesUtil.XML_TAG));
  }

  @Test
  public void testGetAttributesXml_CustomTag_EmptyMap() {

    mockedAttributesUtil
        .when(() -> AttributesUtil.getAttributesXml(any(Map.class), anyString()))
        .thenCallRealMethod();

    Map<String, Map<String, String>> attributesMap = new HashMap<>();

    String attributesXml = AttributesUtil.getAttributesXml(attributesMap, CUSTOM_TAG);

    assertNotNull(attributesXml);

    // Check that it's not an empty XML fragment
    assertTrue(attributesXml.contains(CUSTOM_TAG));
  }

  @Test
  public void testLoadAttributes_NullParameter() {

    mockedAttributesUtil
        .when(() -> AttributesUtil.loadAttributes(any(Node.class)))
        .thenCallRealMethod();

    Map<String, Map<String, String>> attributesMap = AttributesUtil.loadAttributes(null);

    assertNotNull(attributesMap);
    assertTrue(attributesMap.isEmpty());
  }
}
