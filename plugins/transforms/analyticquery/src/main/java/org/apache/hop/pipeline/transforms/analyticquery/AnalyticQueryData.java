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

package org.apache.hop.pipeline.transforms.analyticquery;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author ngoodman
 * @since 27-jan-2009
 */
public class AnalyticQueryData extends BaseTransformData implements ITransformData {
  // Grouped Field Indexes (faster than looking up by strings)
  public int[] groupnrs;

  public IRowMeta inputRowMeta;
  public IRowMeta outputRowMeta;

  // Two Integers for our processing
  // Window Size (the largest N we need to skip forward/back)
  public int window_size;
  // Queue Size (the size of our data queue (Window Size * 2 ) + 1)
  public int queue_size;
  // / Queue Cursor (the current processing location in the queue) reset with every group
  public int queue_cursor;

  // Queue for keeping the data. We will push data onto the queue
  // and pop it off as we process rows. The queue of data is required
  // to get the second previous row and the second ahead row, etc.
  public ConcurrentLinkedQueue<Object[]> data;

  public Object[] previous;

  public AnalyticQueryData() {
    super();

  }

}
