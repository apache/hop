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

package org.apache.hop.lineage;

import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.StepMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class will help calculate and contain the data lineage for all values in the pipeline.<br>
 * What we will get is a List of ValueLineage objects for all the values steps in the pipeline.<br>
 * Each of these ValueLineage objects contains a list of all the steps it passed through.<br>
 * As such, it's a hierarchical view of the pipeline.<br>
 * <p>
 * This view will allow us to see immediately where a certain value is being manipulated.<br>
 *
 * @author matt
 */
public class PipelineDataLineage {
  private PipelineMeta pipelineMeta;

  private List<ValueLineage> valueLineages;

  private Map<ValueMetaInterface, List<StepMeta>> fieldStepsMap;

  public PipelineDataLineage( PipelineMeta pipelineMeta ) {
    this.pipelineMeta = pipelineMeta;
    this.valueLineages = new ArrayList<ValueLineage>();
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
   * Using the pipeline, we will calculate the data lineage for each field in each step.
   *
   * @throws HopStepException In case there is an exception calculating the lineage. This is usually caused by unavailable data sources
   *                          etc.
   */
  public void calculateLineage() throws HopStepException {

    // After sorting the steps we get a map of all the previous steps of a certain step.
    //
    final Map<StepMeta, Map<StepMeta, Boolean>> stepMap = pipelineMeta.sortStepsNatural();

    // However, the we need a sorted list of previous steps per step, not a map.
    // So lets sort the maps, turn them into lists...
    //
    Map<StepMeta, List<StepMeta>> previousStepListMap = new HashMap<StepMeta, List<StepMeta>>();
    for ( StepMeta stepMeta : stepMap.keySet() ) {
      List<StepMeta> previousSteps = new ArrayList<StepMeta>();
      previousStepListMap.put( stepMeta, previousSteps );

      previousSteps.addAll( stepMap.get( stepMeta ).keySet() );

      // Sort this list...
      //
      Collections.sort( previousSteps, new Comparator<StepMeta>() {

        public int compare( StepMeta o1, StepMeta o2 ) {

          Map<StepMeta, Boolean> beforeMap = stepMap.get( o1 );
          if ( beforeMap != null ) {
            if ( beforeMap.get( o2 ) == null ) {
              return -1;
            } else {
              return 1;
            }
          } else {
            return o1.getName().compareToIgnoreCase( o2.getName() );
          }
        }
      } );
    }

    fieldStepsMap = new HashMap<ValueMetaInterface, List<StepMeta>>();

    List<StepMeta> usedSteps = pipelineMeta.getUsedSteps();
    for ( StepMeta stepMeta : usedSteps ) {
      calculateLineage( stepMeta );
    }
  }

  /**
   * Calculate the lineage for the specified step only...
   *
   * @param stepMeta The step to calculate the lineage for.
   * @throws HopStepException In case there is an exception calculating the lineage. This is usually caused by unavailable data sources
   *                          etc.
   */
  private void calculateLineage( StepMeta stepMeta ) throws HopStepException {
    RowMetaInterface outputMeta = pipelineMeta.getStepFields( stepMeta );

    // The lineage is basically a calculation of origin for each output of a certain step.
    //
    for ( ValueMetaInterface valueMeta : outputMeta.getValueMetaList() ) {

      StepMeta originStepMeta = pipelineMeta.findStep( valueMeta.getOrigin(), stepMeta );
      if ( originStepMeta != null ) {
        /* List<StepMeta> list = */
        fieldStepsMap.get( originStepMeta );
      }
    }
  }

}
