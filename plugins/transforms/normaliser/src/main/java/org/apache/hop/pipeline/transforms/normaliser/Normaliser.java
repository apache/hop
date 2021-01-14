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

package org.apache.hop.pipeline.transforms.normaliser;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Normalise de-normalised input data.
 *
 * @author Matt
 * @since 5-apr-2003
 */
public class Normaliser extends BaseTransform<NormaliserMeta,NormaliserData> implements ITransform<NormaliserMeta,NormaliserData> {
  private static final Class<?> PKG = NormaliserMeta.class; // For Translator

  public Normaliser( TransformMeta transformMeta, NormaliserMeta meta, NormaliserData data, int copyNr, PipelineMeta pipelineMeta,
                     Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {

    Object[] r = getRow(); // get row from rowset, wait for our turn, indicate busy!
    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    List<Integer> normFieldList;
    int i, e;

    if ( first ) { // INITIALISE

      first = false;

      data.inputRowMeta = getInputRowMeta();
      data.outputRowMeta = data.inputRowMeta.clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );
      int normFieldsLength = meta.getNormaliserFields().length;
      data.typeToFieldIndex = new HashMap<>();
      String typeValue;
      int dataFieldNr;

      // Get a unique list of occurrences...
      //
      data.type_occ = new ArrayList<>();
      data.maxlen = 0;

      for ( i = 0; i < normFieldsLength; i++ ) {
        typeValue = meta.getNormaliserFields()[ i ].getValue();
        if ( !data.type_occ.contains( typeValue ) ) {
          data.type_occ.add( typeValue );
        }
        if ( typeValue.length() > data.maxlen ) {
          data.maxlen = typeValue.length();
        }

        // This next section creates a map of arraylist objects. The key is the Type in the Normaliser
        // and the ArrayList is the list of indexes on the row of all fields that get normalized under that Type.
        // This eliminates the inner loop that iterated over all the fields finding the fields associated with the Type.
        // On a test data set with 2500 fields and about 36000 input rows (outputting over 22m rows), the time went from
        // 12min to about 1min 35sec.
        dataFieldNr = data.inputRowMeta.indexOfValue( meta.getNormaliserFields()[ i ].getName() );
        if ( dataFieldNr < 0 ) {
          logError( BaseMessages.getString( PKG, "Normaliser.Log.CouldNotFindFieldInRow", meta.getNormaliserFields()[ i ].getName() ) );
          setErrors( 1 );
          stopAll();
          return false;
        }
        normFieldList = data.typeToFieldIndex.get( typeValue );
        if ( normFieldList == null ) {
          normFieldList = new ArrayList<>();
          data.typeToFieldIndex.put( typeValue, normFieldList );
        }
        normFieldList.add( dataFieldNr );

      }

      // Which fields are not impacted? We can just copy these, leave them alone.
      //
      data.copyFieldnrs = new ArrayList<>();

      Set<String> normaliserFields = meta.getFieldNames();
      int irmSize = data.inputRowMeta.size();

      for ( i = 0; i < irmSize; i++ ) {
        IValueMeta v = data.inputRowMeta.getValueMeta( Integer.valueOf( i ) );
        // Backwards compatibility - old loop called Const.indexofstring which uses equalsIgnoreCase
        if ( !normaliserFields.contains( v.getName().toLowerCase() ) ) {
          data.copyFieldnrs.add( Integer.valueOf( i ) );
        }
      }

    }

    // Modest performance improvement over millions of rows - don't recalculate on each loop iteration something that doesn't change
    int typeOccSize = data.type_occ.size();
    int copyFldNrsSz = data.copyFieldnrs.size();
    int rowMetaSz = data.outputRowMeta.size();

    // Modest performance improvement (large memory improvement) - re-use temporary objects instead of re-creating them - better for GC over time
    String typeValue;
    Object[] outputRowData;
    int outputIndex, nr, normFieldListSz;
    Object value;

    // Now do the normalization
    // Loop over the unique occurrences of the different types.
    //
    for ( e = 0; e < typeOccSize; e++ ) {
      typeValue = data.type_occ.get( e );

      // Create an output row per type
      //
      outputRowData = new Object[ rowMetaSz ];
      outputIndex = 0;

      // Copy the input row data, excluding the fields that are normalized...
      //
      for ( i = 0; i < copyFldNrsSz; i++ ) {
        nr = data.copyFieldnrs.get( i );
        outputRowData[ outputIndex++ ] = r[ nr ];
      }

      // Add the typefield_value
      //
      outputRowData[ outputIndex++ ] = typeValue;

      // Then add the normalized fields...
      //
      normFieldList = data.typeToFieldIndex.get( typeValue );
      normFieldListSz = normFieldList.size();
      for ( i = 0; i < normFieldListSz; i++ ) {
        value = r[ normFieldList.get( i ) ];
        outputRowData[ outputIndex++ ] = value;
      }

      // The row is constructed, now give it to the next transform(s)...
      //
      putRow( data.outputRowMeta, outputRowData );
    }

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "Normaliser.Log.LineNumber" ) + getLinesRead() );
      }
    }

    return true;
  }

  public boolean init() {
    if ( super.init() ) {
      // Add init code here.
      return true;
    }
    return false;
  }

}
