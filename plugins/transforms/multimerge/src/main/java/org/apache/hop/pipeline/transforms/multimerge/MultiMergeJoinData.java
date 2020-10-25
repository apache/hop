/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.transforms.multimerge;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * @author Biswapesh
 * @since 24-nov-2005
 */

public class MultiMergeJoinData extends BaseTransformData implements ITransformData {
  public static class QueueEntry {
    public Object[] row;
    public int index;
  }

  public static class QueueComparator implements Comparator<QueueEntry> {
    MultiMergeJoinData data;

    QueueComparator( MultiMergeJoinData data ) {
      this.data = data;
    }

    @Override
    public int compare( QueueEntry a, QueueEntry b ) {
      try {
        int cmp =
          data.metas[ a.index ].compare(
            a.row, data.metas[ b.index ], b.row, data.keyNrs[ a.index ], data.keyNrs[ b.index ] );
        return cmp > 0 ? 1 : cmp < 0 ? -1 : 0;
      } catch ( HopException e ) {
        throw new RuntimeException( e.getMessage() );
      }
    }
  }

  public Object[][] rows;
  public IRowMeta[] metas;
  public IRowMeta outputRowMeta; // just for speed: oneMeta+twoMeta
  public Object[][] dummy;
  public List<List<Object[]>> results;
  public PriorityQueue<QueueEntry> queue;
  public boolean optional;
  public int[][] keyNrs;
  public int[] drainIndices;

  public IRowSet[] rowSets;
  public QueueEntry[] queueEntries;
  public int[] rowLengths;

  /**
   * Default initializer
   */
  public MultiMergeJoinData() {
    super();
    rows = null;
    metas = null;
    dummy = null;
    optional = false;
    keyNrs = null;
  }

}
