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

package org.apache.hop.beam.core.transform;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.GroupByFn;
import org.apache.hop.beam.core.fn.HopKeyValueFn;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class GroupByTransform extends PTransform<PCollection<HopRow>, PCollection<HopRow>> {

  // The non-transient methods are serializing
  // Keep them simple to stay out of trouble.
  //
  private String transformName;
  private String rowMetaJson;   // The input row
  private String[] groupFields;  // The fields to group over
  private String[] subjects; // The subjects to aggregate on
  private String[] aggregations; // The aggregation types
  private String[] resultFields; // The result fields
  private List<String> transformPluginClasses;
  private List<String> xpPluginClasses;

  private static final Logger LOG = LoggerFactory.getLogger( GroupByTransform.class );
  private final Counter numErrors = Metrics.counter( "main", "GroupByTransformErrors" );

  private transient IRowMeta inputRowMeta;
  private transient IRowMeta groupRowMeta;
  private transient IRowMeta subjectRowMeta;

  public GroupByTransform() {
  }

  public GroupByTransform( String transformName, String rowMetaJson, List<String> transformPluginClasses, List<String> xpPluginClasses, String[] groupFields, String[] subjects, String[] aggregations, String[] resultFields) {
    this.transformName = transformName;
    this.rowMetaJson = rowMetaJson;
    this.transformPluginClasses = transformPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
    this.groupFields = groupFields;
    this.subjects = subjects;
    this.aggregations = aggregations;
    this.resultFields = resultFields;
  }

  @Override public PCollection<HopRow> expand( PCollection<HopRow> input ) {
    try {
      if ( inputRowMeta == null ) {
        BeamHop.init(transformPluginClasses, xpPluginClasses);

        inputRowMeta = JsonRowMeta.fromJson( rowMetaJson );

        groupRowMeta = new RowMeta();
        for (int i=0;i<groupFields.length;i++) {
          groupRowMeta.addValueMeta( inputRowMeta.searchValueMeta( groupFields[i] ) );
        }
        subjectRowMeta = new RowMeta();
        for (int i=0;i<subjects.length;i++) {
          subjectRowMeta.addValueMeta( inputRowMeta.searchValueMeta( subjects[i] ) );
        }
      }

      // Split the HopRow into GroupFields-HopRow and SubjectFields-HopRow
      //
      PCollection<KV<HopRow, HopRow>> groupSubjects = input.apply( ParDo.of(
        new HopKeyValueFn( rowMetaJson, transformPluginClasses, xpPluginClasses, groupFields, subjects, transformName )
      ) );

      // Now we need to aggregate the groups with a Combine
      GroupByKey<HopRow, HopRow> byKey = GroupByKey.<HopRow, HopRow>create();
      PCollection<KV<HopRow, Iterable<HopRow>>> grouped = groupSubjects.apply( byKey );

      // Aggregate the rows in the grouped PCollection
      //   Input: KV<HopRow>, Iterable<HopRow>>
      //   This means that The group rows is in HopRow.  For every one of these, you get a list of subject rows.
      //   We need to calculate the aggregation of these subject lists
      //   Then we output group values with result values behind it.
      //
      String counterName = transformName+" AGG";
      PCollection<HopRow> output = grouped.apply( ParDo.of(
        new GroupByFn(counterName, JsonRowMeta.toJson(groupRowMeta), transformPluginClasses, xpPluginClasses,
          JsonRowMeta.toJson(subjectRowMeta), aggregations ) ) );

      return output;
    } catch(Exception e) {
      numErrors.inc();
      LOG.error( "Error in group by transform", e );
      throw new RuntimeException( "Error in group by transform", e );
    }
  }


  /**
   * Gets inputRowMetaJson
   *
   * @return value of inputRowMetaJson
   */
  public String getRowMetaJson() {
    return rowMetaJson;
  }

  /**
   * @param rowMetaJson The inputRowMetaJson to set
   */
  public void setRowMetaJson( String rowMetaJson ) {
    this.rowMetaJson = rowMetaJson;
  }

  /**
   * Gets groupFields
   *
   * @return value of groupFields
   */
  public String[] getGroupFields() {
    return groupFields;
  }

  /**
   * @param groupFields The groupFields to set
   */
  public void setGroupFields( String[] groupFields ) {
    this.groupFields = groupFields;
  }

  /**
   * Gets subjects
   *
   * @return value of subjects
   */
  public String[] getSubjects() {
    return subjects;
  }

  /**
   * @param subjects The subjects to set
   */
  public void setSubjects( String[] subjects ) {
    this.subjects = subjects;
  }

  /**
   * Gets aggregations
   *
   * @return value of aggregations
   */
  public String[] getAggregations() {
    return aggregations;
  }

  /**
   * @param aggregations The aggregations to set
   */
  public void setAggregations( String[] aggregations ) {
    this.aggregations = aggregations;
  }

  /**
   * Gets resultFields
   *
   * @return value of resultFields
   */
  public String[] getResultFields() {
    return resultFields;
  }

  /**
   * @param resultFields The resultFields to set
   */
  public void setResultFields( String[] resultFields ) {
    this.resultFields = resultFields;
  }
}
