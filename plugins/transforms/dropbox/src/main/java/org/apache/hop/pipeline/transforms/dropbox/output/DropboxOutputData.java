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

package org.apache.hop.pipeline.transforms.dropbox.output;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

public class DropboxOutputData extends BaseTransformData implements ITransformData {

  int accessTokenIdx;
  int sourceFileIdx;
  int targetFilesIdx;

  IRowMeta outputRowMeta;
  public IRowSet successfulRowSet;
  public IRowSet failedRowSet;
  public boolean chosesTargetTransforms;

  // Large files should be uploaded in chunks for optimization.
  static final long CHUNKED_UPLOAD_CHUNK_SIZE = 8L << 20; // 8MiB
  static final int CHUNKED_UPLOAD_MAX_ATTEMPTS = 5;

  /**
   *
   */
  public DropboxOutputData() {
    super();
  }
}
