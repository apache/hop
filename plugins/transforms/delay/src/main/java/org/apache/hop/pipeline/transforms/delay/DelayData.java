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

package org.apache.hop.pipeline.transforms.delay;

import org.apache.hop.pipeline.transform.BaseTransformData;

@SuppressWarnings("java:S1104")
public class DelayData extends BaseTransformData {
  public long multiple;
  public long timeout;
  public long staticTimeout;
  public int staticScaleTimeCode;
  public int timeoutFieldIndex;
  public int scaleTimeFieldIndex;
  public org.apache.hop.core.row.IValueMeta timeoutValueMeta;
  public org.apache.hop.core.row.IValueMeta scaleTimeValueMeta;

  public DelayData() {
    super();
    multiple = 1000L;
    timeout = 0L;
    staticTimeout = 0L;
    staticScaleTimeCode = 1;
    timeoutFieldIndex = -1;
    scaleTimeFieldIndex = -1;
    timeoutValueMeta = null;
    scaleTimeValueMeta = null;
  }
}
