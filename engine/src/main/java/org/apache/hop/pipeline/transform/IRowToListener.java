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

package org.apache.hop.pipeline.transform;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;

/**
 * Listener for rows written to a specific destination rowset (target hops).
 *
 * <p>Unlike {@link IRowListener}, this interface includes the destination {@link IRowSet} so
 * listeners can keep rows with different layouts apart (e.g. Filter true/false branches,
 * Switch/Case targets). Used by GUI hop sampling and other destination-aware consumers.
 *
 * @see IRowListener
 * @see BaseTransform#putRowTo(IRowMeta, Object[], IRowSet)
 */
public interface IRowToListener {

  /**
   * Called when a row is written to a specific destination rowset (typically via {@code putRowTo} /
   * {@code handlePutRowTo}).
   *
   * @param rowMeta the metadata of the row
   * @param row the data of the row
   * @param rowSet the destination rowset the row was written to
   * @throws HopTransformException an exception that can be thrown to hard stop the transform
   */
  void rowWrittenTo(IRowMeta rowMeta, Object[] row, IRowSet rowSet) throws HopTransformException;
}
