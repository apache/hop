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

package org.apache.hop.spark.engines;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiTableColumn;
import org.apache.hop.core.gui.plugin.GuiTableColumnType;
import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * One row of the path scheme map: the Spark File and Lake Table transforms rewrite a path of the
 * scheme {@code from} to {@code to} before Spark reads or writes it, for example minio to s3a.
 */
@Getter
@Setter
public class PathSchemeMapping {

  @HopMetadataProperty
  @GuiTableColumn(
      order = "10",
      type = GuiTableColumnType.TEXT,
      label = "i18n::SparkEngine.PathScheme.From.Label",
      toolTip = "i18n::SparkEngine.PathScheme.From.ToolTip")
  private String from;

  @HopMetadataProperty
  @GuiTableColumn(
      order = "20",
      type = GuiTableColumnType.TEXT,
      label = "i18n::SparkEngine.PathScheme.To.Label",
      toolTip = "i18n::SparkEngine.PathScheme.To.ToolTip")
  private String to;

  public PathSchemeMapping() {}

  public PathSchemeMapping(String from, String to) {
    this.from = from;
    this.to = to;
  }

  /** The rows of {@code from=to} lines, as run configurations stored them before the table. */
  public static List<PathSchemeMapping> parse(String lines) {
    List<PathSchemeMapping> mappings = new ArrayList<>();
    for (SparkSetting line : SparkSetting.parse(lines)) {
      mappings.add(new PathSchemeMapping(line.getKey(), line.getValue()));
    }
    return mappings;
  }

  /** The rows as {@code from=to} lines; rows without a scheme to map are left out. */
  public static String toLines(List<PathSchemeMapping> mappings) {
    List<SparkSetting> lines = new ArrayList<>();
    if (mappings != null) {
      for (PathSchemeMapping mapping : mappings) {
        lines.add(new SparkSetting(mapping.getFrom(), mapping.getTo()));
      }
    }
    return SparkSetting.toLines(lines);
  }
}
