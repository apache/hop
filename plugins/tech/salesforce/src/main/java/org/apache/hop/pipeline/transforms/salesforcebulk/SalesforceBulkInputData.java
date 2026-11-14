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

package org.apache.hop.pipeline.transforms.salesforcebulk;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

@SuppressWarnings("java:S1104")
public class SalesforceBulkInputData extends BaseTransformData implements ITransformData {
  public SalesforceBulkClient client;
  public IRowMeta outputRowMeta;
  public List<IValueMeta> targets = new ArrayList<>();
  public List<String> columnNames = new ArrayList<>();
  public int jobIdIndex = -1;
  public String jobId;
  public String openJobId;
  public String locator;
  public boolean started;
  public List<Object[]> page = new ArrayList<>();
  public int pageIndex;
  public long timeoutMillis = 3_600_000L;
  public long pollMillis = 10_000L;
  public long maxRows;
  public String operationCode;
  public String soql;
}
