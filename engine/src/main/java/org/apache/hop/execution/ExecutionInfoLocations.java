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

package org.apache.hop.execution;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;

/**
 * Looks up an {@link ExecutionInfoLocation} by name and runs an action against the initialized
 * {@link IExecutionInfoLocation} it wraps, closing it again afterwards.
 *
 * <p>No caching of the location state is done: every call loads the metadata element afresh and
 * every {@code initialize()} is paired with exactly one {@code close()}.
 */
public final class ExecutionInfoLocations {

  private ExecutionInfoLocations() {
    // Utility class, do not instantiate.
  }

  /** An action to perform against an initialized execution information location. */
  @FunctionalInterface
  public interface ILocationAction<T> {
    T apply(IExecutionInfoLocation location) throws HopException;
  }

  /** Thrown when no execution information location with the requested name exists. */
  public static class LocationNotFoundException extends HopException {
    public LocationNotFoundException(String message) {
      super(message);
    }
  }

  /**
   * Load the named execution information location, initialize it, run the given action against it
   * and close it again.
   *
   * <p>The location is initialized <i>inside</i> the try block, so a location which fails part-way
   * through {@code initialize()} is still closed. A failure to close is logged but never allowed to
   * mask the error which caused it.
   *
   * @param locationName the name of the execution information location metadata element
   * @param variables the variables to initialize the location with
   * @param metadataProvider the provider to load the location from and to initialize it with
   * @param log the log channel to report a failing close on, or null to stay silent
   * @param action the action to run against the initialized location
   * @return whatever the action returns
   * @throws HopException if the location can not be found or the action fails
   */
  public static <T> T withLocation(
      String locationName,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      ILogChannel log,
      ILocationAction<T> action)
      throws HopException {

    if (StringUtils.isEmpty(locationName)) {
      throw new HopException("Please specify the name of the execution information location");
    }

    IHopMetadataSerializer<ExecutionInfoLocation> serializer =
        metadataProvider.getSerializer(ExecutionInfoLocation.class);
    ExecutionInfoLocation location = serializer.load(locationName);
    if (location == null) {
      throw new LocationNotFoundException(
          "Unable to find execution information location " + locationName);
    }

    IExecutionInfoLocation iLocation = location.getExecutionInfoLocation();
    T result;
    try {
      iLocation.initialize(variables, metadataProvider);
      result = action.apply(iLocation);
    } catch (Exception actionError) {
      // We are already unwinding. Close, but never let a failing close hide the real error.
      try {
        iLocation.close();
      } catch (Exception closeError) {
        if (log != null) {
          log.logError("Error closing execution information location " + locationName, closeError);
        }
      }
      throw actionError instanceof HopException hopException
          ? hopException
          : new HopException(
              "Error using execution information location " + locationName, actionError);
    }

    // The action succeeded, so a failing close IS the failure: caching locations persist their
    // writes here, and reporting success while the data was never stored would lose it silently.
    iLocation.close();
    return result;
  }
}
