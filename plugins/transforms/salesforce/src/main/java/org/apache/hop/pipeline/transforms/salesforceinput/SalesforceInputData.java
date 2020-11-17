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

package org.apache.hop.pipeline.transforms.salesforceinput;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceTransformData;

import java.util.GregorianCalendar;

/*
 * @author Samatar
 * @since 10-06-2007
 */
public class SalesforceInputData extends SalesforceTransformData {
  public int nr_repeats;
  public long rownr;
  public Object[] previousRow;
  public IRowMeta inputRowMeta;
  public IRowMeta outputRowMeta;
  public IRowMeta convertRowMeta;
  public int recordcount;
  public int nrfields;
  public boolean limitReached;
  public long limit;
  // available before we call query more if needed
  public int nrRecords;
  // We use this variable to query more
  // we initialize it each time we call query more
  public int recordIndex;
  public GregorianCalendar startCal;
  public GregorianCalendar endCal;
  public boolean finishedRecord;

  public SalesforceInputData() {
    super();

    nr_repeats = 0;
    nrfields = 0;
    recordcount = 0;
    limitReached = false;
    limit = 0;
    nrRecords = 0;
    recordIndex = 0;
    rownr = 0;

    startCal = null;
    endCal = null;
  }
}
