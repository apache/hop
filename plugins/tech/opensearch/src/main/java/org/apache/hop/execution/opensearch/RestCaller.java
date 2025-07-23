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
 *
 */

package org.apache.hop.execution.opensearch;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.anon.AnonymousPipelineResults;
import org.apache.hop.pipeline.anon.AnonymousPipelineRunner;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.datagrid.DataGridDataMeta;
import org.apache.hop.pipeline.transforms.datagrid.DataGridFieldMeta;
import org.apache.hop.pipeline.transforms.datagrid.DataGridMeta;
import org.apache.hop.pipeline.transforms.rest.RestMeta;
import org.apache.hop.pipeline.transforms.rest.fields.ResultField;

/** Let's call a REST service using another Hop pipeline */
@Getter
public class RestCaller {
  public static final String BODY_FIELD_NAME = "body";
  public static final String RESPONSE_FIELD_NAME = "response";
  public static final String STATUSCODE_FIELD_NAME = "code";

  public static final String INPUT_TRANSFORM_NAME = "INPUT";
  public static final String REST_TRANSFORM_NAME = "REST";

  private final IHopMetadataProvider metadataProvider;
  private final String url;
  private final String username;
  private final String password;
  private final String method; // POST, PUT, GET, ...
  private final String body;
  private final Map<String, String> headers;
  private final boolean ignoreSsl;

  private String response;
  private Long statusCode;
  private String loggingText;
  private Result result;

  private IRowMeta restRowMeta;
  private Object[] restRowData;

  public RestCaller(
      IHopMetadataProvider metadataProvider,
      String url,
      String username,
      String password,
      String method,
      String body,
      boolean ignoreSsl,
      Map<String, String> headers) {
    this.metadataProvider = metadataProvider;
    this.url = url;
    this.username = username;
    this.password = password;
    this.method = method;
    this.body = body;
    this.ignoreSsl = ignoreSsl;
    this.headers = headers;
  }

  public String execute() throws HopException {
    try {
      IVariables variables = Variables.getADefaultVariableSpace();

      // Let's build a new pipeline with a Data Grid + REST transform
      //
      PipelineMeta pipelineMeta = new PipelineMeta();
      pipelineMeta.setName("RestCaller-execution");
      pipelineMeta.setDescription("OpenSearch Rest Caller");

      // The data grid
      //
      DataGridMeta gridMeta = new DataGridMeta();
      gridMeta.setDataGridFields(
          List.of(new DataGridFieldMeta("", "", "", BODY_FIELD_NAME, "String", "", -1, -1, false)));
      gridMeta.setDataLines(List.of(new DataGridDataMeta(Collections.singletonList(body))));
      TransformMeta gridTransformMeta = new TransformMeta(INPUT_TRANSFORM_NAME, gridMeta);
      gridTransformMeta.setLocation(50, 100);
      pipelineMeta.addTransform(gridTransformMeta);

      // The REST transform
      //
      RestMeta restMeta = new RestMeta();
      restMeta.setUrl(url);
      restMeta.setMethod(method);
      restMeta.setHttpLogin(username);
      restMeta.setHttpPassword(password);
      restMeta.setIgnoreSsl(ignoreSsl);
      restMeta.setBodyField(BODY_FIELD_NAME);
      restMeta.setApplicationType("JSON");
      restMeta.setResultField(
          new ResultField(RESPONSE_FIELD_NAME, STATUSCODE_FIELD_NAME, null, null));
      TransformMeta restTransformMeta = new TransformMeta(REST_TRANSFORM_NAME, restMeta);
      restTransformMeta.setLocation(350, 100);
      pipelineMeta.addTransform(restTransformMeta);

      // Add a hop between them.
      //
      pipelineMeta.addPipelineHop(new PipelineHopMeta(gridTransformMeta, restTransformMeta));

      AnonymousPipelineResults results =
          AnonymousPipelineRunner.executePipeline(
              pipelineMeta, variables, metadataProvider, REST_TRANSFORM_NAME);
      restRowMeta = results.getResultRowMeta();
      restRowData = results.getFirstResultRow();

      if (restRowMeta != null) {
        // Now we can retrieve the result and status code
        //
        response = restRowMeta.getString(restRowData, RESPONSE_FIELD_NAME, null);
        statusCode = restRowMeta.getInteger(restRowData, STATUSCODE_FIELD_NAME, null);
      }
      result = results.getResult();
      loggingText =
          HopLogStore.getAppender()
              .getBuffer(results.getPipeline().getLogChannelId(), false)
              .toString();

      return response;
    } catch (Exception e) {
      throw new HopException("Error executing REST call to " + url, e);
    }
  }
}
