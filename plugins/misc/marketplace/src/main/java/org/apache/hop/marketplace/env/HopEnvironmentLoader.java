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

package org.apache.hop.marketplace.env;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/** Loads {@link HopEnvironmentSpec} from YAML or JSON. */
public final class HopEnvironmentLoader {

  private HopEnvironmentLoader() {}

  public static HopEnvironmentSpec load(Path file) throws HopException {
    if (file == null || !Files.isRegularFile(file)) {
      throw new HopException("Environment file not found: " + file);
    }
    String name = file.getFileName().toString().toLowerCase(Locale.ROOT);
    try (InputStream in = Files.newInputStream(file)) {
      if (name.endsWith(".json")) {
        return HopJson.newMapper().readValue(in, HopEnvironmentSpec.class);
      }
      // default: YAML (.yaml / .yml / anything else)
      Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
      Object loaded = yaml.load(in);
      if (loaded == null) {
        return new HopEnvironmentSpec();
      }
      if (!(loaded instanceof Map)) {
        throw new HopException("Environment file root must be a YAML mapping: " + file);
      }
      return HopJson.newMapper().convertValue(loaded, HopEnvironmentSpec.class);
    } catch (IOException e) {
      throw new HopException("Unable to read environment file: " + file, e);
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to parse environment file: " + file, e);
    }
  }
}
