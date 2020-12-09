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

package org.apache.hop.lineage;

import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class will help calculate and contain the data lineage for all values in the pipeline.<br>
 * What we will get is a List of ValueLineage objects for all the values transforms in the pipeline.<br>
 * Each of these ValueLineage objects contains a list of all the transforms it passed through.<br>
 * As such, it's a hierarchical view of the pipeline.<br>
 * <p>
 * This view will allow us to see immediately where a certain value is being manipulated.<br>
 *
 * @author matt
 */
public class PipelineDataLineage {
  private PipelineMeta pipelineMeta;

  private List<ValueLineage> valueLineages;

  private Map<IValueMeta, List<TransformMeta>> fieldTransformsMap;

  public PipelineDataLineage( PipelineMeta pipelineMeta ) {
    this.pipelineMeta = pipelineMeta;
    this.valueLineages = new ArrayList<>();
  }

  public PipelineMeta getPipelineMeta() {
    return pipelineMeta;
  }

  public void setPipelineMeta( PipelineMeta pipelineMeta ) {
    this.pipelineMeta = pipelineMeta;
  }

  /**
   * @return the valueLineages
   */
  public List<ValueLineage> getValueLineages() {
    return valueLineages;
  }

  /**
   * @param valueLineages the valueLineages to set
   */
  public void setValueLineages( List<ValueLineage> valueLineages ) {
    this.valueLineages = valueLineages;
  }

  /**
   * Using the pipeline, we will calculate the data lineage for each field in each transform.
   *
   * @throws HopTransformException In case there is an exception calculating the lineage. This is usually caused by unavailable data sources
   *                          etc.
   */
  public void calculateLineage(IVariables variables) throws HopTransformException {

    // After sorting the transforms we get a map of all the previous transforms of a certain transform.
    //
    final Map<TransformMeta, Map<TransformMeta, Boolean>> transformMap = pipelineMeta.sortTransformsNatural();

    // However, the we need a sorted list of previous transforms per transform, not a map.
    // So lets sort the maps, turn them into lists...
    //
    Map<TransformMeta, List<TransformMeta>> previousTransformListMap = new HashMap<>();
    for ( TransformMeta transformMeta : transformMap.keySet() ) {
      List<TransformMeta> previousTransforms = new ArrayList<>();
      previousTransformListMap.put( transformMeta, previousTransforms );

      previousTransforms.addAll( transformMap.get( transformMeta ).keySet() );

      // Sort this list...
      //
      Collections.sort( previousTransforms, ( o1, o2 ) -> {

        Map<TransformMeta, Boolean> beforeMap = transformMap.get( o1 );
        if ( beforeMap != null ) {
          if ( beforeMap.get( o2 ) == null ) {
            return -1;
          } else {
            return 1;
          }
        } else {
          return o1.getName().compareToIgnoreCase( o2.getName() );
        }
      } );
    }

    fieldTransformsMap = new HashMap<>();

    List<TransformMeta> usedTransforms = pipelineMeta.getUsedTransforms();
    for ( TransformMeta transformMeta : usedTransforms ) {
      calculateLineage( variables, transformMeta );
    }
  }

  /**
   * Calculate the lineage for the specified transform only...
   *
   * @param transformMeta The transform to calculate the lineage for.
   * @throws HopTransformException In case there is an exception calculating the lineage. This is usually caused by unavailable data sources
   *                          etc.
   */
  private void calculateLineage( IVariables variables, TransformMeta transformMeta ) throws HopTransformException {
    IRowMeta outputMeta = pipelineMeta.getTransformFields( variables, transformMeta );

    // The lineage is basically a calculation of origin for each output of a certain transform.
    //
    for ( IValueMeta valueMeta : outputMeta.getValueMetaList() ) {

      TransformMeta originTransformMeta = pipelineMeta.findTransform( valueMeta.getOrigin(), transformMeta );
      if ( originTransformMeta != null ) {
        /* List<TransformMeta> list = */
        fieldTransformsMap.get( originTransformMeta );
      }
    }
  }

}
