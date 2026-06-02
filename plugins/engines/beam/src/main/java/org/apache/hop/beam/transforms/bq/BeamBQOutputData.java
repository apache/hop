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

package org.apache.hop.beam.transforms.bq;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.TableId;
import java.util.List;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

public class BeamBQOutputData extends BaseTransformData implements ITransformData {
  public BigQuery bigquery;
  public TableId tableId;

  /** Cached on first row so {@link BeamBQOutput#prepareTable} can derive the schema. */
  public IRowMeta inputRowMeta;

  /** Set once we've checked / created / truncated the target table — kept off the hot path. */
  public boolean tableReady;

  /** Streaming-insert buffer. Flushed when full and on dispose. */
  public List<InsertAllRequest.RowToInsert> batch;

  /** Streaming-insert chunk size. 500 is the common safe ceiling for BigQuery's REST API. */
  public int batchSize;
}
