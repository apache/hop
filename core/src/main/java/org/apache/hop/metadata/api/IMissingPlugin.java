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

package org.apache.hop.metadata.api;

import java.util.List;

/**
 * Implemented by the placeholder objects which stand in for a plugin that isn't installed: a
 * transform in a pipeline or an action in a workflow.
 *
 * <p>Since the plugin isn't available there is no class to de-serialize its settings into. To make
 * sure that simply opening and saving the file doesn't destroy the configuration of that plugin,
 * the placeholder keeps the XML of the original object untouched and writes it back out again on
 * save.
 */
public interface IMissingPlugin {

  /** The id of the plugin which couldn't be found. */
  String getMissingPluginId();

  /**
   * The XML of the child elements of the object this placeholder replaces, verbatim, in the order
   * in which they were read.
   */
  List<String> getPreservedXml();

  /**
   * Remember the XML of the object this placeholder replaces so it can be written back out on save.
   *
   * @param preservedXml the verbatim XML of the original child elements
   */
  void setPreservedXml(List<String> preservedXml);
}
