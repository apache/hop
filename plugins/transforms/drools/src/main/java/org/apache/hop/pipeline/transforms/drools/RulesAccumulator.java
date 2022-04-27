
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.drools;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class RulesAccumulator  extends BaseTransform<RulesAccumulatorMeta, RulesAccumulatorData> {
  // private static Class<?> PKG = Rules.class; // for i18n purposes

  public RulesAccumulator(
          TransformMeta transformMeta,
          RulesAccumulatorMeta meta,
          RulesAccumulatorData data,
          int copyNr,
          PipelineMeta pipelineMeta,
          Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  public boolean init() {

    if ( super.init() ) {
      return true;
    }
    return false;
  }

  public boolean runtimeInit() throws HopTransformException {
    try {
      data.setOutputRowMeta( getInputRowMeta().clone() );
      meta.setKeepInputFields( false );
      meta.getFields( data.getOutputRowMeta(), getTransformName(), null, null, this, null );

      data.setRuleFilePath( meta.getRuleFile() );
      data.setRuleString( meta.getRuleDefinition() );

      data.initializeRules();
      data.initializeInput( getInputRowMeta() );

      return true;
    } catch ( Exception e ) {
      throw new HopTransformException( e );
    }
  }

  public void dispose( ) {
     super.dispose();
  }

  public boolean processRow() throws HopException {
    try {
      Object[] r = getRow(); // get row, set busy!

      if ( r == null ) { // no more input to be expected...

        data.execute();

        Object[] outputRow;

        String[] expectedResults = meta.getExpectedResultList();

        for ( Rules.Row resultRow : data.getResultRows() ) {
          outputRow = new Object[expectedResults.length];
          for ( String columnName : expectedResults ) {
            outputRow[data.getOutputRowMeta().indexOfValue( columnName )] = resultRow.getColumn().get( columnName );
          }
          putRow( data.getOutputRowMeta(), outputRow );
        }

        data.shutdown();
        setOutputDone();
        return false;
      }

      if ( first ) {
        if ( !runtimeInit() ) {
          return false;
        }

        first = false;
      }

      // Store the row for processing
      data.loadRow( r );

      return true;
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }
}
