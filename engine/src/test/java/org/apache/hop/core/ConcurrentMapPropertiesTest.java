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
package org.apache.hop.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class ConcurrentMapPropertiesTest {

  @Test
  void runMapTests() throws IOException {
    Properties p = new Properties();
    ConcurrentMapProperties c = new ConcurrentMapProperties();

    for (int i = 0; i < 10000; i++) {
      String unique = UUID.randomUUID().toString();
      p.put(unique, unique);
      c.put(unique, unique);
    }

    assertEquals(p, c);
    assertEquals(p.size(), c.size());

    List<String> removeKeys = new ArrayList<>(c.size());

    for (Object key : c.keySet()) {
      if (Math.random() > 0.2) {
        removeKeys.add((String) key);
      }
    }

    for (String rmKey : removeKeys) {
      c.remove(rmKey);
      p.remove(rmKey);
    }

    assertEquals(p.size(), c.size());
    assertEquals(p, c);

    p.clear();
    c.clear();

    assertEquals(p, c);
    assertEquals(0, p.size());
    assertEquals(0, c.size());

    Map<String, String> addKeys = removeKeys.stream().collect(Collectors.toMap(x -> x, x -> x));
    p.putAll(addKeys);
    c.putAll(addKeys);

    assertEquals(p, c);

    for (String property : removeKeys) {
      assertEquals(p.getProperty(property), c.getProperty(property));
    }

    Path tempFile = Files.createTempFile("propstest", "props");

    c.store(new FileOutputStream(tempFile.toFile()), "No Comments");
    c.clear();

    assertTrue(c.isEmpty());

    c.load(new FileInputStream(tempFile.toFile()));

    assertEquals(c, p);
  }
}
