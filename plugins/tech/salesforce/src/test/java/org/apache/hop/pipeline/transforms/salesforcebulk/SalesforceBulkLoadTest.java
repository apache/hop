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

package org.apache.hop.pipeline.transforms.salesforcebulk;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SalesforceBulkLoadTest {

  @BeforeAll
  static void setUpHop() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void returnsRowsInInputOrderWhenSalesforceReversesTheFile() throws Exception {
    SalesforceBulkLoadMeta meta = new SalesforceBulkLoadMeta();
    meta.setSalesforceObject("Account");
    meta.setOperation(SalesforceBulkOperation.INSERT);
    meta.setFields(List.of(new SalesforceBulkField("Name", "Name")));
    meta.setMaxBytes("1000000");
    SalesforceBulkLoadData data = new SalesforceBulkLoadData();
    data.client = new EchoClient();
    TransformMeta transformMeta = new TransformMeta("load", meta);
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("bulk");
    pipelineMeta.addTransform(transformMeta);
    Harness transform = new Harness(transformMeta, meta, data, pipelineMeta);
    while (transform.processRow()) {
      // Drain the three input rows and the final flush.
    }
    assertEquals(3, transform.output.size());
    assertEquals("001000000000001AAA", transform.output.get(0)[1]);
    assertEquals(true, transform.output.get(0)[3]);
    assertEquals("001000000000002AAA", transform.output.get(1)[1]);
    assertEquals(false, transform.output.get(2)[3]);
    assertEquals("simulated failure", transform.output.get(2)[4]);
    transform.dispose();
  }

  private static final class Harness extends SalesforceBulkLoad {
    private final Queue<Object[]> rows = new ArrayDeque<>();
    private final IRowMeta input;
    private final List<Object[]> output = new ArrayList<>();

    private Harness(
        TransformMeta transformMeta,
        SalesforceBulkLoadMeta meta,
        SalesforceBulkLoadData data,
        PipelineMeta pipelineMeta) {
      super(transformMeta, meta, data, 0, pipelineMeta, null);
      input = new RowMeta();
      input.addValueMeta(new ValueMetaString("Name"));
      rows.add(new Object[] {"Alice"});
      rows.add(new Object[] {"Bob"});
      rows.add(new Object[] {"FAIL"});
    }

    @Override
    public Object[] getRow() {
      return rows.poll();
    }

    @Override
    public IRowMeta getInputRowMeta() {
      return input;
    }

    @Override
    public void putRow(IRowMeta rowMeta, Object[] row) {
      output.add(row);
    }
  }

  private static final class EchoClient implements SalesforceBulkClient {
    private byte[] uploaded;

    @Override
    public SalesforceBulkJob createIngestJob(SalesforceBulkIngestRequest request) {
      return new SalesforceBulkJob("7501", "Open", null, 0, 0);
    }

    @Override
    public void uploadIngestData(String jobId, byte[] csv) {
      uploaded = csv;
    }

    @Override
    public SalesforceBulkJob completeIngestUpload(String jobId) {
      return new SalesforceBulkJob(jobId, "UploadComplete", null, 0, 0);
    }

    @Override
    public SalesforceBulkJob getIngestJob(String jobId) {
      return new SalesforceBulkJob(jobId, "JobComplete", null, 3, 1);
    }

    @Override
    public String ingestResults(String jobId, boolean failed) throws HopException {
      List<List<String>> table =
          SalesforceBulkCsv.parse(new String(uploaded, StandardCharsets.UTF_8));
      StringBuilder csv = new StringBuilder();
      csv.append(failed ? "Name,sf__Id,sf__Error\n" : "Name,sf__Id,sf__Created\n");
      List<List<String>> data = new ArrayList<>(table.subList(1, table.size()));
      java.util.Collections.reverse(data);
      for (List<String> row : data) {
        boolean fail = "FAIL".equals(row.get(0));
        if (failed != fail) {
          continue;
        }
        if (fail) {
          csv.append("FAIL,,simulated failure\n");
        } else if ("Alice".equals(row.get(0))) {
          csv.append("Alice,001000000000001AAA,true\n");
        } else {
          csv.append("Bob,001000000000002AAA,true\n");
        }
      }
      return csv.toString();
    }

    @Override
    public void abortIngestJob(String jobId) {}

    @Override
    public SalesforceBulkJob createQueryJob(String operation, String soql) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SalesforceBulkJob getQueryJob(String jobId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SalesforceBulkQueryPage queryResults(String jobId, String locator) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void abortQueryJob(String jobId) {}

    @Override
    public void close() {}
  }
}
