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

package org.apache.hop.pipeline.transforms.javascript;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class ScriptValuesScript {
  public static final int NORMAL_SCRIPT = -1;
  public static final int TRANSFORM_SCRIPT = 0;
  public static final int START_SCRIPT = 1;
  public static final int END_SCRIPT = 2;

  @HopMetadataProperty(
      key = "jsScript_type",
      injectionKey = "SCRIPT_TYPE",
      injectionKeyDescription = "ScriptValuesMod.Injection.SCRIPT_TYPE")
  private int type;

  @HopMetadataProperty(
      key = "jsScript_name",
      injectionKey = "SCRIPT_NAME",
      injectionKeyDescription = "ScriptValuesMod.Injection.SCRIPT_NAME")
  private String name;

  @HopMetadataProperty(
      key = "jsScript_script",
      injectionKey = "SCRIPT",
      injectionKeyDescription = "ScriptValuesMod.Injection.SCRIPT")
  private String script;

  public ScriptValuesScript() {}

  public ScriptValuesScript(ScriptValuesScript s) {
    super();
    this.type = s.type;
    this.name = s.name;
    this.script = s.script;
  }

  public ScriptValuesScript(int type, String name, String script) {
    this();
    this.type = type;
    this.name = name;
    this.script = script;
  }

  public boolean isTransformScript() {
    return this.type == TRANSFORM_SCRIPT;
  }

  public boolean isStartScript() {
    return this.type == START_SCRIPT;
  }

  public boolean isEndScript() {
    return this.type == END_SCRIPT;
  }

  public String toString() {
    return String.format("ScriptValuesScript: (%d, %s, %s)", type, name, script);
  }
}
