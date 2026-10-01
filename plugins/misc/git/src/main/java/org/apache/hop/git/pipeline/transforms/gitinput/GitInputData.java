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

package org.apache.hop.git.pipeline.transforms.gitinput;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.git.provider.GitConnection;
import org.apache.hop.git.provider.GitResourceReader;
import org.apache.hop.git.provider.GitResourceType;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

public class GitInputData extends BaseTransformData implements ITransformData {

  public GitConnection gitConnection;
  public IRowMeta outputRowMeta;
  public GitResourceReader reader;
  public GitInputSource source;
  public GitResourceType resourceType;
  public String owner;
  public String repository;
  public int pageSize;
  public int maxPages;
  public boolean summaryLogged;
}
