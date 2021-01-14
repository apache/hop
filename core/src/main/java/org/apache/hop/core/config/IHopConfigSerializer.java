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

package org.apache.hop.core.config;

import org.apache.hop.core.exception.HopException;

import java.util.Map;

public interface IHopConfigSerializer {

  /**
   * Write the hop configuration map to the provided file
   * @param filename The name of the file to write to
   * @param configMap The configuration options to write
   * @throws HopException In case something goes wrong
   */
  void writeToFile(String filename, Map<String, Object> configMap) throws HopException;

  /**
   * Read the configurations map from a file
   * @param filename The name of the config file
   * @return The options map
   * @throws HopException In case something goes wrong.
   */
  Map<String, Object> readFromFile( String filename) throws HopException;
}
