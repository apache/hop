// CHECKSTYLE:EmptyBlock:OFF
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

package org.apache.hop.core;

public class ProgressNullMonitorListener implements IProgressMonitor {

  @Override
  public void beginTask(String message, int nrWorks) {
    // Do nothing because of beginTask( "", 0 ).
  }

  @Override
  public void subTask(String message) {
    // Do nothing because of subTask( "" ).
  }

  @Override
  public boolean isCanceled() {
    return false;
  }

  @Override
  public void worked(int nrWorks) {
    // Do nothing because of worked( 0 ).
  }

  @Override
  public void done() {
    // Do nothing because of done().
  }

  @Override
  public void setTaskName(String taskName) {
    // Do nothing because of setTaskName( "" ).
  }
}
