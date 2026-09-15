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

package org.apache.hop.www.api.v1.resources;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionData;
import org.apache.hop.execution.ExecutionInfoLocations;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.www.api.HopApiBadRequestException;
import org.apache.hop.www.api.HopApiNotFoundException;

/**
 * Exposes the {@link org.apache.hop.execution.IExecutionInfoLocation} operations as JSON.
 *
 * <p>Every method goes through {@link ExecutionInfoLocations#withLocation}, the same helper the
 * {@code /hop/getExecInfo} and {@code /hop/registerExecInfo} servlets use, so a location is always
 * initialized and closed exactly once.
 */
@Path("/location")
public class LocationResource extends BaseApiResource {

  /** Register a new execution. */
  @POST
  @Path("/{locationName}/executions")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response registerExecution(
      @PathParam("locationName") String locationName, Execution execution) throws HopException {
    withLocation(
        locationName,
        location -> {
          location.registerExecution(execution);
          return null;
        });
    return Response.ok().entity("execution registered successfully").build();
  }

  /** List the execution IDs in an execution information location. */
  @GET
  @Path("/{locationName}/executions")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getExecutionIds(
      @PathParam("locationName") String locationName,
      @QueryParam("children") @DefaultValue("false") boolean includeChildren,
      @QueryParam("limit") @DefaultValue("100") int limit)
      throws HopException {
    return Response.ok(
            withLocation(
                locationName, location -> location.getExecutionIds(includeChildren, limit)))
        .build();
  }

  /** Find the last execution of a given type and name. */
  @GET
  @Path("/{locationName}/executions/last")
  @Produces(MediaType.APPLICATION_JSON)
  public Response findLastExecution(
      @PathParam("locationName") String locationName,
      @QueryParam("execType") String executionTypeString,
      @QueryParam("name") String name)
      throws HopException {
    ExecutionType executionType = parseExecutionType(executionTypeString);
    requireParameter(name, "name");
    Execution execution =
        withLocation(locationName, location -> location.findLastExecution(executionType, name));
    if (execution == null) {
      throw new HopApiNotFoundException(
          "Unable to find the last " + executionType + " execution named " + name);
    }
    return Response.ok(execution).build();
  }

  /** Get the execution for a given ID. */
  @GET
  @Path("/{locationName}/executions/{executionId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getExecution(
      @PathParam("locationName") String locationName, @PathParam("executionId") String executionId)
      throws HopException {
    Execution execution =
        withLocation(locationName, location -> location.getExecution(executionId));
    if (execution == null) {
      throw new HopApiNotFoundException(
          "Unable to find execution for ID " + executionId + " in location " + locationName);
    }
    return Response.ok(execution).build();
  }

  /** Delete the execution for a given ID. */
  @DELETE
  @Path("/{locationName}/executions/{executionId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteExecution(
      @PathParam("locationName") String locationName, @PathParam("executionId") String executionId)
      throws HopException {
    return Response.ok(
            withLocation(locationName, location -> location.deleteExecution(executionId)))
        .build();
  }

  /** Get the execution state for a given ID. */
  @GET
  @Path("/{locationName}/executions/{executionId}/state")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getExecutionState(
      @PathParam("locationName") String locationName, @PathParam("executionId") String executionId)
      throws HopException {
    ExecutionState state =
        withLocation(locationName, location -> location.getExecutionState(executionId));
    if (state == null) {
      throw new HopApiNotFoundException(
          "Unable to find execution state for ID " + executionId + " in location " + locationName);
    }
    return Response.ok(state).build();
  }

  /** Update the execution state for a given ID. */
  @PUT
  @Path("/{locationName}/executions/{executionId}/state")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response updateExecutionState(
      @PathParam("locationName") String locationName,
      @PathParam("executionId") String executionId,
      ExecutionState state)
      throws HopException {
    withLocation(
        locationName,
        location -> {
          location.updateExecutionState(state);
          return null;
        });
    return Response.ok().entity("execution state updated successfully").build();
  }

  /** Get the logging text of the execution state for a given ID. */
  @GET
  @Path("/{locationName}/executions/{executionId}/state/logging")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getExecutionStateLoggingText(
      @PathParam("locationName") String locationName,
      @PathParam("executionId") String executionId,
      @QueryParam("limit") @DefaultValue("-1") int limit)
      throws HopException {
    return Response.ok(
            withLocation(
                locationName,
                location -> location.getExecutionStateLoggingText(executionId, limit)))
        .build();
  }

  /** Find the child executions of a parent execution. */
  @GET
  @Path("/{locationName}/executions/{executionId}/children")
  @Produces(MediaType.APPLICATION_JSON)
  public Response findChildExecutions(
      @PathParam("locationName") String locationName, @PathParam("executionId") String executionId)
      throws HopException {
    return Response.ok(withLocation(locationName, location -> location.findExecutions(executionId)))
        .build();
  }

  /** Find the child execution IDs of a parent execution of a given type. */
  @GET
  @Path("/{locationName}/executions/{executionId}/child-ids")
  @Produces(MediaType.APPLICATION_JSON)
  public Response findChildIds(
      @PathParam("locationName") String locationName,
      @PathParam("executionId") String executionId,
      @QueryParam("execType") String executionTypeString)
      throws HopException {
    ExecutionType executionType = parseExecutionType(executionTypeString);
    return Response.ok(
            withLocation(
                locationName, location -> location.findChildIds(executionType, executionId)))
        .build();
  }

  /** Find the parent execution ID of an execution. */
  @GET
  @Path("/{locationName}/executions/{executionId}/parent")
  @Produces(MediaType.APPLICATION_JSON)
  public Response findParentId(
      @PathParam("locationName") String locationName, @PathParam("executionId") String executionId)
      throws HopException {
    return Response.ok(withLocation(locationName, location -> location.findParentId(executionId)))
        .build();
  }

  /** Get the execution data of an execution. */
  @GET
  @Path("/{locationName}/executions/{executionId}/data")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getExecutionData(
      @PathParam("locationName") String locationName,
      @PathParam("executionId") String executionId,
      @QueryParam("parentId") String parentId)
      throws HopException {
    String parent = StringUtils.isEmpty(parentId) ? executionId : parentId;
    ExecutionData data =
        withLocation(locationName, location -> location.getExecutionData(parent, executionId));
    if (data == null) {
      throw new HopApiNotFoundException(
          "Unable to find execution data for ID " + executionId + " in location " + locationName);
    }
    return Response.ok(data).build();
  }

  /** Register execution data. */
  @POST
  @Path("/{locationName}/executions/{executionId}/data")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response registerData(
      @PathParam("locationName") String locationName,
      @PathParam("executionId") String executionId,
      ExecutionData data)
      throws HopException {
    withLocation(
        locationName,
        location -> {
          location.registerData(data);
          return null;
        });
    return Response.ok().entity("execution data registered successfully").build();
  }

  private <T> T withLocation(String locationName, ExecutionInfoLocations.ILocationAction<T> action)
      throws HopException {
    return ExecutionInfoLocations.withLocation(
        locationName,
        context.getVariables(),
        context.getMetadataProvider(),
        context.getLog(),
        action);
  }

  private static void requireParameter(String value, String name) throws HopException {
    if (StringUtils.isEmpty(value)) {
      throw new HopApiBadRequestException("Please specify parameter '" + name + "'");
    }
  }

  private static ExecutionType parseExecutionType(String executionTypeString) throws HopException {
    requireParameter(executionTypeString, "execType");
    try {
      return ExecutionType.valueOf(executionTypeString);
    } catch (IllegalArgumentException e) {
      throw new HopApiBadRequestException("Unknown execution type: " + executionTypeString);
    }
  }
}
