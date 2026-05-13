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

package org.apache.hop.lineage.model;

/**
 * Describes how a {@link FileIoTabularColumn}'s {@code sourcePath} should be interpreted when
 * building structure trees or mapping to external catalogs.
 */
public enum FileIoPathSyntax {
  /** Not applicable (tabular file / no locator). */
  NONE,
  /** JSONPath-style locator (Hop Json Input). */
  JSON_PATH,
  /** YAML path (Hop YAML Input). */
  YAML_PATH,
  /** XPath (Hop Get data from XML, etc.). */
  XPATH,
  /** Delimited column by position/name only (CSV and similar). */
  DELIMITED
}
