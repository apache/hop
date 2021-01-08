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

public class ProgressMonitorAdapter implements IProgressMonitor {
  private org.eclipse.core.runtime.IProgressMonitor monitor;

  public ProgressMonitorAdapter( org.eclipse.core.runtime.IProgressMonitor monitor ) {
    this.monitor = monitor;
  }

  public void beginTask( String message, int nrWorks ) {
    monitor.beginTask( message, nrWorks );
  }

  public void done() {
    monitor.done();
  }

  public boolean isCanceled() {
    return monitor.isCanceled();
  }

  public void subTask( String message ) {
    monitor.subTask( message );
  }

  public void worked( int nrWorks ) {
    monitor.worked( nrWorks );
  }

  public void setTaskName( String taskName ) {
    monitor.setTaskName( taskName );
  }

}
