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

package org.apache.hop.core.compress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.junit.Test;

public class CompressionPluginTypeTest {

  @Test
  public void testGetInstance() {
    CompressionPluginType instance = CompressionPluginType.getInstance();
    CompressionPluginType instance2 = CompressionPluginType.getInstance();
    assertSame(instance, instance2);
    assertNotNull(instance);
    CompressionPluginType.pluginType = null;
    CompressionPluginType instance3 = CompressionPluginType.getInstance();
    assertNotSame(instance, instance3);
  }

  @Test
  public void testGetPluginInfo() {
    CompressionPluginType instance = CompressionPluginType.getInstance();
    CompressionPlugin a = new FakePlugin().getClass().getAnnotation(CompressionPlugin.class);
    assertNotNull(a);
    assertEquals("", instance.extractCategory(a));
    assertEquals("Fake", instance.extractID(a));
    assertEquals("FakePlugin", instance.extractName(a));
    assertEquals("", instance.extractCasesUrl(a));
    assertEquals("Compression Plugin", instance.extractDesc(a));
    assertEquals("", instance.extractDocumentationUrl(a));
    assertEquals("", instance.extractForumUrl(a));
    assertNull(instance.extractImageFile(a));
    assertFalse(instance.extractSeparateClassLoader(a));
  }

  @CompressionPlugin(id = "Fake", name = "FakePlugin")
  private class FakePlugin {}
}
