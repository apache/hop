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
 *
 */

package org.apache.hop.neo4j.transforms.gencsv;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.util.Map;

public class GenerateCsvData extends BaseTransformData implements ITransformData {

  public IRowMeta outputRowMeta;
  public String importFolder;
  public int graphFieldIndex;
  public IndexedGraphData indexedGraphData;
  public String baseFolder;

  public String filesPrefix;
  public String filenameField;
  public String fileTypeField;

  public Map<String, CsvFile> fileMap;

  public static String getPropertySetKey(
      String sourcePipeline, String sourceTransform, String propertySetId) {
    StringBuilder key = new StringBuilder();
    if (StringUtils.isNotEmpty(sourcePipeline)) {
      key.append(sourcePipeline);
    }
    if (StringUtils.isNotEmpty(sourceTransform)) {
      if (key.length() > 0) {
        key.append("-");
      }
      key.append(sourceTransform);
    }
    if (StringUtils.isNotEmpty(propertySetId)) {
      if (key.length() > 0) {
        key.append("-");
      }
      key.append(propertySetId);
    }

    // Replace troublesome characters from the key for filename purposes
    //
    String setKey = key.toString();
    setKey = setKey.replace("*", "");
    setKey = setKey.replace(":", "");
    setKey = setKey.replace(";", "");
    setKey = setKey.replace("[", "");
    setKey = setKey.replace("]", "");
    setKey = setKey.replace("$", "");
    setKey = setKey.replace("/", "");
    setKey = setKey.replace("{", "");
    setKey = setKey.replace("}", "");

    return setKey;
  }
}
