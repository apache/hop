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
package org.apache.hop.pgvector.transforms.search;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pgvector.util.PgVectorSearchFilter;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

public class PgVectorSearchData extends BaseTransformData implements ITransformData {

  public IRowMeta inputRowMeta;
  public IRowMeta outputRowMeta;
  public org.apache.hop.core.database.Database database;
  public PreparedStatement searchStatement;
  public int embeddingFieldIndex = -1;

  /** Output field indices, resolved once so the result loop never scans the row metadata. */
  public int resultIdFieldIndex = -1;

  public int resultDocumentIdFieldIndex = -1;
  public int resultChunkIndexFieldIndex = -1;
  public int resultContentFieldIndex = -1;
  public int resultScoreFieldIndex = -1;
  public List<FilterBinding> filterBindings = new ArrayList<>();

  public static final class FilterBinding {
    public final int streamFieldIndex;
    public final PgVectorSearchFilter filter;

    public FilterBinding(int streamFieldIndex, PgVectorSearchFilter filter) {
      this.streamFieldIndex = streamFieldIndex;
      this.filter = filter;
    }
  }

  public PgVectorSearchData() {
    super();
  }
}
