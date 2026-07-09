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
package org.apache.hop.pipeline.transforms.fake;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Optional, human-curated titles, descriptions and parameter labels layered on top of the
 * reflection-driven {@link FakerCatalog}.
 *
 * <p>DataFaker exposes no descriptions, titles or parameter names at runtime - the jar is not even
 * built with {@code -parameters}. This overlay lets us add friendly text for <em>any</em> generator
 * without giving up the automatic catalog: a generator with an entry uses it, a generator without
 * one falls back to what reflection gives us (a prettified function name and the parameter types).
 *
 * <p>Entries live in {@code generators.properties} next to this class and use these keys:
 *
 * <pre>
 *   &lt;category&gt;.&lt;function&gt;.title                          = friendly name
 *   &lt;category&gt;.&lt;function&gt;.description                    = one-line explanation
 *   &lt;category&gt;.&lt;function&gt;.&lt;paramCount&gt;.param.&lt;index&gt; = label for one parameter
 * </pre>
 *
 * The {@code paramCount} segment disambiguates overloaded methods. The file is free to grow to
 * cover everything; nothing has to be filled in for the catalog to work.
 */
public final class FakerDescriptors {

  private static final String RESOURCE = "generators.properties";

  private FakerDescriptors() {
    // utility class
  }

  /** Initialization-on-demand holder: the properties file is read once, on first access. */
  private static final class Holder {
    static final Properties PROPERTIES = load();
  }

  private static Properties load() {
    Properties properties = new Properties();
    try (InputStream in = FakerDescriptors.class.getResourceAsStream(RESOURCE)) {
      if (in != null) {
        properties.load(in);
      }
    } catch (IOException e) {
      // The overlay is entirely optional - fall back to reflection-derived information.
    }
    return properties;
  }

  /**
   * @param key a descriptor key (see the class javadoc for the format)
   * @return the curated value, or {@code null} when nothing is defined for that key
   */
  public static String get(String key) {
    String value = Holder.PROPERTIES.getProperty(key);
    return value == null || value.isBlank() ? null : value.trim();
  }
}
