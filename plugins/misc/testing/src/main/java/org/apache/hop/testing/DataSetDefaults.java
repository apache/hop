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

package org.apache.hop.testing;

import java.util.regex.Pattern;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;

/**
 * Suggestions for a new data set.
 *
 * <p>Name pattern: {@code ds-<pipeline file>-<transform>}. The pipeline part is the file name
 * without its directory or extension, and the transform part is omitted when unknown. The folder is
 * the datasets-folder variable expression when that variable is set, otherwise the pipeline
 * directory. A value that is already set is left alone, except the constructor placeholder base
 * file name, which is replaced when a name is known. A second call does not replace suggestions.
 */
public final class DataSetDefaults {

  private static final Pattern UNSAFE = Pattern.compile("[\\\\/:*?\"<>|\\p{Cntrl}]+");
  private static final Pattern REPEATED_DASH = Pattern.compile("-{2,}");
  private static final Pattern EDGE_DASH = Pattern.compile("^[\\s-]+|[\\s-]+$");

  private DataSetDefaults() {}

  public static void apply(
      DataSet dataSet,
      String pipelineFilename,
      String transformName,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (dataSet == null) {
      return;
    }

    if (StringUtils.isEmpty(dataSet.getName())) {
      String generated = buildName(pipelineFilename, transformName);
      if (generated != null) {
        dataSet.setName(uniqueName(generated, metadataProvider));
      }
    }

    if (StringUtils.isEmpty(dataSet.getFolderName())) {
      String folder = defaultFolder(variables, pipelineFilename);
      if (StringUtils.isNotEmpty(folder)) {
        dataSet.setFolderName(folder);
      }
    }

    if (isPlaceholderFilename(dataSet) && StringUtils.isNotEmpty(dataSet.getName())) {
      dataSet.setBaseFilename(dataSet.getName() + ".csv");
    }
  }

  static String buildName(String pipelineFilename, String transformName) {
    String pipeline = sanitizeToken(pipelineStem(pipelineFilename));
    String transform = sanitizeToken(transformName);
    if (pipeline == null && transform == null) {
      return null;
    }
    StringBuilder name = new StringBuilder("ds");
    if (pipeline != null) {
      name.append('-').append(pipeline);
    }
    if (transform != null) {
      name.append('-').append(transform);
    }
    return name.toString();
  }

  static String pipelineStem(String pipelineFilename) {
    String file = filenameOnly(pipelineFilename);
    if (file == null) {
      return null;
    }
    String stem = FilenameUtils.getBaseName(file);
    return StringUtils.isBlank(stem) ? null : stem;
  }

  static String pipelineDirectory(String pipelineFilename) {
    if (StringUtils.isBlank(pipelineFilename)) {
      return null;
    }
    String normalized = pipelineFilename.trim().replace('\\', '/');
    int slash = normalized.lastIndexOf('/');
    if (slash < 0) {
      return null;
    }
    if (slash == 0) {
      return "/";
    }
    return normalized.substring(0, slash);
  }

  static String defaultFolder(IVariables variables, String pipelineFilename) {
    String configured = datasetsFolderExpression(variables);
    if (configured != null) {
      return configured;
    }
    return pipelineDirectory(pipelineFilename);
  }

  /**
   * The datasets-folder variable expression when that variable is set on {@code variables} or a
   * parent, otherwise null.
   */
  public static String datasetsFolderExpression(IVariables variables) {
    if (!hasDatasetsFolder(variables)) {
      return null;
    }
    return "${" + DataSet.VARIABLE_HOP_DATASETS_FOLDER + "}";
  }

  private static boolean isPlaceholderFilename(DataSet dataSet) {
    return StringUtils.isEmpty(dataSet.getBaseFilename())
        || DataSet.DEFAULT_BASE_FILENAME.equals(dataSet.getBaseFilename());
  }

  private static String filenameOnly(String pipelineFilename) {
    if (StringUtils.isBlank(pipelineFilename)) {
      return null;
    }
    String normalized = pipelineFilename.trim().replace('\\', '/');
    int slash = normalized.lastIndexOf('/');
    String file = slash >= 0 ? normalized.substring(slash + 1) : normalized;
    return StringUtils.isBlank(file) ? null : file;
  }

  static String sanitizeToken(String value) {
    if (value == null) {
      return null;
    }
    String cleaned = UNSAFE.matcher(value.trim()).replaceAll("-");
    cleaned = REPEATED_DASH.matcher(cleaned).replaceAll("-");
    cleaned = EDGE_DASH.matcher(cleaned).replaceAll("");
    return cleaned.isEmpty() ? null : cleaned;
  }

  private static boolean hasDatasetsFolder(IVariables variables) {
    IVariables current = variables;
    while (current != null) {
      String value = current.getVariable(DataSet.VARIABLE_HOP_DATASETS_FOLDER);
      if (StringUtils.isNotBlank(value)) {
        return true;
      }
      current = current.getParentVariables();
    }
    return false;
  }

  private static String uniqueName(String base, IHopMetadataProvider metadataProvider) {
    if (metadataProvider == null) {
      return base;
    }
    try {
      IHopMetadataSerializer<DataSet> serializer = metadataProvider.getSerializer(DataSet.class);
      if (!serializer.exists(base)) {
        return base;
      }
      int suffix = 2;
      String candidate = base + " " + suffix;
      while (serializer.exists(candidate) && suffix < 10000) {
        suffix++;
        candidate = base + " " + suffix;
      }
      return candidate;
    } catch (Exception e) {
      return base;
    }
  }
}
