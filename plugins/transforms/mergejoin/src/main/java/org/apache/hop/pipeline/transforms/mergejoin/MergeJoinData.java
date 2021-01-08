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

package org.apache.hop.pipeline.transforms.mergejoin;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import javax.sql.RowSet;
import java.util.List;

/**
 * @author Biswapesh
 * @since 24-nov-2005
 */

public class MergeJoinData extends BaseTransformData implements ITransformData {
  public Object[] one, two;
  public IRowMeta oneMeta, twoMeta;
  public IRowMeta outputRowMeta; // just for speed: oneMeta+twoMeta
  public Object[] one_dummy, two_dummy;
  public List<Object[]> ones, twos;
  public Object[] one_next, two_next;
  public boolean one_optional, two_optional;
  public int[] keyNrs1;
  public int[] keyNrs2;

  public IRowSet oneRowSet;
  public IRowSet twoRowSet;

  /**
   * Default initializer
   */
  public MergeJoinData() {
    super();
    ones = null;
    twos = null;
    one_next = null;
    two_next = null;
    one_dummy = null;
    two_dummy = null;
    one_optional = false;
    two_optional = false;
    keyNrs1 = null;
    keyNrs2 = null;
  }

}
