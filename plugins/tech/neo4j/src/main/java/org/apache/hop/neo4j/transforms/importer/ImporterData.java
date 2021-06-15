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

package org.apache.hop.neo4j.transforms.importer;

import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.util.List;

public class ImporterData extends BaseTransformData implements ITransformData {

  public List<String> nodesFiles;
  public List<String> relsFiles;

  public String importFolder;
  public String adminCommand;
  public String databaseFilename;
  public String baseFolder;
  public String reportFile;

  public int filenameFieldIndex;
  public int fileTypeFieldIndex;
  public String badTolerance;
  public String readBufferSize;
  public String maxMemory;
  public String processors;
}
