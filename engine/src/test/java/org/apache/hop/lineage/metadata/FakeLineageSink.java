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

package org.apache.hop.lineage.metadata;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.plugin.LineageSinkPlugin;
import org.apache.hop.lineage.spi.ILineageSink;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/** A lineage sink for tests that keeps what it was given. */
@LineageSinkPlugin(id = "fake-sink", name = "Fake sink")
public class FakeLineageSink implements ILineageSink {

  /** The configuration of the fake sink: a URL and a token, both resolved. */
  @Getter
  @Setter
  public static class Configuration implements ILineageSinkConfiguration {
    private String pluginId;
    private String pluginName;

    @HopMetadataProperty private String url;

    @HopMetadataProperty(password = true)
    private String token;

    @Override
    public Map<String, String> toSettings(
        IVariables variables, IHopMetadataProvider metadataProvider) throws HopException {
      String resolved = variables.resolve(url);
      if (resolved == null || resolved.isEmpty()) {
        throw new HopException("No URL");
      }
      Map<String, String> settings = new LinkedHashMap<>();
      settings.put("FAKE_URL", resolved);
      settings.put("FAKE_TOKEN", variables.resolve(token));
      return settings;
    }

    @Override
    public Configuration clone() {
      try {
        return (Configuration) super.clone();
      } catch (CloneNotSupportedException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  @Getter private String url;
  @Getter private final List<LineageEvent> events = new ArrayList<>();
  @Getter private volatile boolean shutdown;

  @Override
  public ILineageSinkConfiguration newConfiguration() {
    return new Configuration();
  }

  @Override
  public void init(IVariables variables, ILogChannel log) {
    url = variables.getVariable("FAKE_URL");
  }

  @Override
  public synchronized void accept(List<LineageEvent> batch) {
    events.addAll(batch);
  }

  @Override
  public void shutdown() {
    shutdown = true;
  }

  /** A configured sink called {@code name} with this URL. */
  public static LineageSink named(String name, String url) {
    Configuration configuration = new Configuration();
    configuration.setPluginId("fake-sink");
    configuration.setPluginName("Fake sink");
    configuration.setUrl(url);
    configuration.setToken("s3cr3t-token");
    return new LineageSink(name, "A fake sink", configuration);
  }
}
