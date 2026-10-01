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

package org.apache.hop.core.diagram;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

/** Options bean controlling diagram export parameters. */
@Getter
@Setter
public class DiagramExportOptions {
  private String targetFilename;
  private String format;
  private float magnification = 1.0f;
  private boolean includeNotes = true;
  private String theme;
  private String direction;
  private Map<String, String> extraOptions = new HashMap<>();

  public DiagramExportOptions() {}

  public DiagramExportOptions(String targetFilename, String format) {
    this.targetFilename = targetFilename;
    this.format = format;
  }
}
