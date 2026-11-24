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

package org.apache.hop.fatjar;

import java.util.Locale;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/** The engine a Hop fat jar is generated for: it decides which jars of the installation go in. */
public enum FatJarEngine implements IEnumHasCodeAndDescription {

  /**
   * The native Spark engine: Hop and its plugins without Spark, Scala and the Hadoop client, which
   * the cluster provides, and without the Beam engine.
   */
  SPARK("SparkPipelineEngine"),

  /** The Apache Beam engines (Direct, Flink, Dataflow, Spark runner), with their SDKs. */
  BEAM("BeamDirectPipelineEngine");

  /** A pipeline engine plugin id that this kind of fat jar needs installed. */
  private final String enginePluginId;

  FatJarEngine(String enginePluginId) {
    this.enginePluginId = enginePluginId;
  }

  public String getEnginePluginId() {
    return enginePluginId;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getDescription() {
    return BaseMessages.getString(FatJarEngine.class, "FatJarEngine." + name());
  }

  /**
   * @param value a code, case insensitive ({@code spark}, {@code beam})
   * @return the engine, or null when the value names none
   */
  public static FatJarEngine lookup(String value) {
    if (value == null) {
      return null;
    }
    String code = value.trim().toUpperCase(Locale.ROOT);
    for (FatJarEngine engine : values()) {
      if (engine.name().equals(code)) {
        return engine;
      }
    }
    return null;
  }
}
