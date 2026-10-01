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
package org.apache.hop.pipeline.transforms.plugincatalog;

import java.util.ArrayList;
import java.util.List;

/** One catalogued plugin (transform, action or metadata type) with its resolved labels. */
public class PluginRecord {
  public String pluginId = "";

  /** {@code transform}, {@code action} or {@code metadata}. */
  public String pluginType = "";

  public String name = "";
  public String description = "";
  public String category = "";
  public String keywords = "";
  public String className = "";

  /** Locale-independent search aliases (English name, category and keywords). */
  public String englishAliases = "";

  /** BCP-47 tag of the Hop locale the labels above were resolved against. */
  public String locale = "";

  public List<PropertyRecord> properties = new ArrayList<>();
}
