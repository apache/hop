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

package org.apache.hop.rest.v1.resources;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.rest.Hop;
import org.apache.hop.rest.v1.resources.location.ListExecutionsRequest;

/** This resource has services exposing {@link org.apache.hop.execution.IExecutionInfoLocation} */
@Path("/location")
public class LocationResource extends BaseResource {
  private final Hop hop = Hop.getInstance();

  /**
   * Register a new execution
   *
   * @param locationName The name of the location to register with
   * @param execution the execution to register
   */
  @POST
  @Path("/executions/{locationName}/")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response registerExecution(
      @PathParam("locationName") String locationName, Execution execution) {
    try {
      IExecutionInfoLocation location = getInitLocation(locationName);

      try {
        location.registerExecution(execution);
      } finally {
        location.close();
      }

      return Response.ok("execution registered successfully").build();
    } catch (Exception e) {
      return getServerError("Error registering execution for location " + locationName, e, true);
    }
  }

  /**
   * List the execution IDs in an execution information location.
   *
   * @param locationName The execution information location to query
   * @param request The request containing the query parameters
   */
  @GET
  @Path("/executions/{locationName}/")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getExecutionIds(
      @PathParam("locationName") String locationName, ListExecutionsRequest request) {
    try {
      IExecutionInfoLocation location = getInitLocation(locationName);
      List<String> ids = new ArrayList<>();
      try {
        // Query the location for the execution IDs.
        //
        ids.addAll(location.getExecutionIds(request.isIncludeChildren(), request.getLimit()));
      } finally {
        location.close();
      }

      return Response.ok(ids).build();
    } catch (Exception e) {
      return getServerError("Error registering execution for location " + locationName, e, true);
    }
  }

  /**
   * Get the execution for a given ID in an execution information location.
   *
   * @param locationName The execution information location to query
   * @param executionId The execution ID to get the state for
   */
  @GET
  @Path("/executions/{locationName}/{executionId}/")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getExecution(
      @PathParam("locationName") String locationName,
      @PathParam("executionId") String executionId) {
    try {
      IExecutionInfoLocation location = getInitLocation(locationName);
      List<String> ids = new ArrayList<>();
      Execution execution;
      try {
        // Get the execution state for the ID.
        //
        execution = location.getExecution(executionId);
        if (execution == null) {
          throw new HopException(
              "Unable to find execution for ID " + executionId + " in location " + locationName);
        }
      } finally {
        location.close();
      }
      return Response.ok(execution).build();
    } catch (Exception e) {
      return getServerError(
          "Error getting execution for location " + locationName + " and ID " + executionId,
          e,
          true);
    }
  }

  /**
   * Get the execution state for a given ID in an execution information location.
   *
   * @param locationName The execution information location to query
   * @param executionId The execution ID to get the state for
   */
  @GET
  @Path("/state/{locationName}/{executionId}/")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getExecutionState(
      @PathParam("locationName") String locationName,
      @PathParam("executionId") String executionId) {
    try {
      IExecutionInfoLocation location = getInitLocation(locationName);
      List<String> ids = new ArrayList<>();
      ExecutionState executionState;
      try {
        // Get the execution state for the ID.
        //
        executionState = location.getExecutionState(executionId);
        if (executionState == null) {
          throw new HopException(
              "Unable to find execution state for ID "
                  + executionId
                  + " in location "
                  + locationName);
        }
      } finally {
        location.close();
      }
      return Response.ok(executionState).build();
    } catch (Exception e) {
      return getServerError(
          "Error getting execution state for location " + locationName + " and ID " + executionId,
          e,
          true);
    }
  }

  private IExecutionInfoLocation getInitLocation(String locationName) throws HopException {
    if (StringUtils.isEmpty(locationName)) {
      throw new HopException("Please specify a location name");
    }
    IVariables variables = hop.getVariables();
    MultiMetadataProvider metadataProvider = hop.getMetadataProvider();
    IHopMetadataSerializer<ExecutionInfoLocation> serializer =
        metadataProvider.getSerializer(ExecutionInfoLocation.class);
    ExecutionInfoLocation location = serializer.load(locationName);
    if (location == null) {
      throw new HopException("Unable to find location " + locationName);
    }
    IExecutionInfoLocation iLocation = location.getExecutionInfoLocation();

    // Initialize the location
    iLocation.initialize(variables, metadataProvider);

    return location.getExecutionInfoLocation();
  }
}
