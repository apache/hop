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

package org.apache.hop.pipeline.transforms.salesforceupsert;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceTransformData;

import com.sforce.soap.partner.UpsertResult;
import com.sforce.soap.partner.sobject.SObject;

import java.util.HashMap;
import java.util.Map;

/*
 * @author Samatar
 * @since 10-06-2007
 */
public class SalesforceUpsertData extends SalesforceTransformData {
  public IRowMeta inputRowMeta;
  public IRowMeta outputRowMeta;

  public int nrFields;
  public int[] fieldnrs;

  public UpsertResult[] upsertResult;

  public SObject[] sfBuffer;
  public Object[][] outputBuffer;
  public int iBufferPos;

  public String realSalesforceFieldName;
  public Map<String, String> dataTypeMap;
  public boolean mapData;

  public SalesforceUpsertData() {
    super();

    nrFields = 0;

    upsertResult = null;
    realSalesforceFieldName = null;
    iBufferPos = 0;
    dataTypeMap = new HashMap<>();
  }
}
