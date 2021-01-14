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

package org.apache.hop.pipeline.transforms.mapping;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.RowProducer;

/**
 * This class renamed fields in rows before passing them to the row producer specified
 *
 * @author matt
 */
public class RowDataInputMapper {
  private final RowProducer rowProducer;
  private final MappingIODefinition inputDefinition;

  private boolean first = true;
  private IRowMeta renamedRowMeta;

  public RowDataInputMapper( MappingIODefinition inputDefinition, RowProducer rowProducer ) {
    this.inputDefinition = inputDefinition;
    this.rowProducer = rowProducer;
  }

  /**
   * Attempts to put the <code>row</code> onto the underlying <code>rowProducer</code> during its timeout period.
   * Returns <code>true</code> if the operation completed successfully and <code>false</code> otherwise.
   *
   * @param rowMeta input row's meta data
   * @param row     input row
   * @return <code>true</code> if the <code>row</code> was put successfully
   */
  public boolean putRow( IRowMeta rowMeta, Object[] row ) {
    if ( first ) {
      first = false;
      renamedRowMeta = rowMeta.clone();

      for ( MappingValueRename valueRename : inputDefinition.getValueRenames() ) {
        IValueMeta valueMeta = renamedRowMeta.searchValueMeta( valueRename.getSourceValueName() );
        if ( valueMeta != null ) {
          valueMeta.setName( valueRename.getTargetValueName() );
        }
      }
    }
    return rowProducer.putRow( renamedRowMeta, row, false );
  }

  public void finished() {
    rowProducer.finished();
  }
}
