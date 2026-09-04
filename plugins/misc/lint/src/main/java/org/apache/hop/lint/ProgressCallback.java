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
package org.apache.hop.lint;

/** Callback interface for reporting progress during linting operations */
public interface ProgressCallback {

  /**
   * Report progress with a message and completion percentage
   *
   * @param message Current operation message
   * @param completed Number of items completed
   * @param total Total number of items to process
   */
  void updateProgress(String message, int completed, int total);

  /**
   * Check if the operation has been cancelled by the user
   *
   * @return true if cancelled, false otherwise
   */
  boolean isCancelled();

  /**
   * Report that the operation is complete
   *
   * @param message Final completion message
   */
  void setComplete(String message);
}
