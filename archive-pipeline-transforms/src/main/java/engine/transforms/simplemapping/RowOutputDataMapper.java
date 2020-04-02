/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.simplemapping;

import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transforms.mapping.MappingIODefinition;
import org.apache.hop.pipeline.transforms.mapping.MappingValueRename;

/**
 * This class takes care of mapping output data from the mapping transform back to the parent pipeline, renaming
 * columns mainly.
 *
 * @author matt
 */
public class RowOutputDataMapper extends RowAdapter {

  private MappingIODefinition inputDefinition;
  private MappingIODefinition outputDefinition;
  private boolean first = true;
  private IRowMeta renamedRowMeta;
  private PutRowInterface putRowInterface;

  public RowOutputDataMapper( MappingIODefinition inputDefinition, MappingIODefinition outputDefinition,
                              PutRowInterface putRowInterface ) {
    this.inputDefinition = inputDefinition;
    this.outputDefinition = outputDefinition;
    this.putRowInterface = putRowInterface;
  }

  @Override
  public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {

    if ( first ) {
      first = false;
      renamedRowMeta = rowMeta.clone();

      if ( inputDefinition.isRenamingOnOutput() ) {
        for ( MappingValueRename valueRename : inputDefinition.getValueRenames() ) {
          IValueMeta valueMeta = renamedRowMeta.searchValueMeta( valueRename.getTargetValueName() );
          if ( valueMeta != null ) {
            valueMeta.setName( valueRename.getSourceValueName() );
          }
        }
      }
      for ( MappingValueRename valueRename : outputDefinition.getValueRenames() ) {
        IValueMeta valueMeta = renamedRowMeta.searchValueMeta( valueRename.getSourceValueName() );
        if ( valueMeta != null ) {
          valueMeta.setName( valueRename.getTargetValueName() );
        }
      }
    }

    putRowInterface.putRow( renamedRowMeta, row );
  }
}
