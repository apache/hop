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

import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.StringToHopFn;
import org.apache.hop.core.row.IRowMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeamHiveMetastoreInputTransform extends PTransform<PBegin, PCollection<HopRow>> {

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger(BeamHiveMetastoreInputTransform.class);
  private static final Counter numErrors = Metrics.counter("main", "BeamBQInputError");
  // These non-transient privates get serialized to spread across nodes
  //
  private String transformName;
  private String hiveMetastoreUris;
  private String hiveMetastoreDatabase;
  private String hiveMetastoreTable;
  private String rowMetaJson;
  private transient IRowMeta rowMeta;

  public BeamHiveMetastoreInputTransform() {}

  public BeamHiveMetastoreInputTransform(
      @Nullable String name,
      String transformName,
      String hiveMetastoreUris,
      String hiveMetastoreDatabase,
      String hiveMetastoreTable,
      String rowMetaJson) {
    super(name);
    this.transformName = transformName;
    this.hiveMetastoreUris = hiveMetastoreUris;
    this.hiveMetastoreDatabase = hiveMetastoreDatabase;
    this.hiveMetastoreTable = hiveMetastoreTable;
    this.rowMetaJson = rowMetaJson;
  }

  @Override
  public PCollection<HopRow> expand(PBegin input) {
    try {
      // Only initialize once on this node/vm
      //
      BeamHop.init();

      Map<String, String> configProperties = new HashMap<>();
      configProperties.put("hive.metastore.uris", hiveMetastoreUris);

      PCollection<HopRow> output;

      PCollection<String> tempOutput =
          input
              .apply(
                  HCatalogIO.read()
                      .withConfigProperties(configProperties)
                      .withDatabase(hiveMetastoreDatabase)
                      .withTable(hiveMetastoreTable))
              .apply(
                  ParDo.of(
                      new DoFn<HCatRecord, String>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                          String outputStr = "";
                          for (int i = 0; i < c.element().size(); i++) {
                            if (i < c.element().size() - 1) {
                              var element = Objects.requireNonNull(c.element()).get(i);
                              if (element != null) {
                                outputStr += element.toString() + ";";
                              } else {
                                outputStr += ";";
                              }
                            } else {
                              var element = Objects.requireNonNull(c.element()).get(i);
                              if (element != null) {
                                outputStr += element.toString();
                              }
                            }
                          }
                          c.output(outputStr);
                        }
                      }));

      output = tempOutput.apply(ParDo.of(new StringToHopFn(transformName, rowMetaJson, ",")));

      return output;

    } catch (Exception e) {
      numErrors.inc();
      LOG.error("Error in beam input transform", e);
      throw new RuntimeException("Error in beam input transform", e);
    }
  }
}
