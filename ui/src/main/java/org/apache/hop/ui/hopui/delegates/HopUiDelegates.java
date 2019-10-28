/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.ui.hopui.delegates;

import org.apache.hop.job.JobMeta;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.ui.hopui.InstanceCreationException;
import org.apache.hop.ui.hopui.HopUi;

public class HopUiDelegates {
  public HopUiJobDelegate jobs;

  public HopUiTabsDelegate tabs;

  public HopUiTransformationDelegate trans;

  public HopUiSlaveDelegate slaves;

  public HopUiTreeDelegate tree;

  public HopUiStepsDelegate steps;

  public HopUiDBDelegate db;

  public HopUiClustersDelegate clusters;

  public HopUiPartitionsDelegate partitions;

  public HopUiDelegates( HopUi hopUi ) {
    tabs = new HopUiTabsDelegate( hopUi );
    tree = new HopUiTreeDelegate( hopUi );
    slaves = new HopUiSlaveDelegate( hopUi );
    steps = new HopUiStepsDelegate( hopUi );
    db = new HopUiDBDelegate( hopUi );
    clusters = new HopUiClustersDelegate( hopUi );
    partitions = new HopUiPartitionsDelegate( hopUi );
    update( hopUi );
  }

  public void update( HopUi hopUi ) {
    HopUiJobDelegate origJobs = jobs;
    try {
      jobs = (HopUiJobDelegate) HopUiDelegateRegistry.getInstance().constructSpoonJobDelegate( hopUi );
    } catch ( InstanceCreationException e ) {
      jobs = new HopUiJobDelegate( hopUi );
    }
    if ( origJobs != null ) {
      // preserve open jobs
      for ( JobMeta jobMeta : origJobs.getLoadedJobs() ) {
        jobs.addJob( jobMeta );
      }
    }
    HopUiTransformationDelegate origTrans = trans;
    try {
      trans =
        (HopUiTransformationDelegate) HopUiDelegateRegistry.getInstance().constructSpoonTransDelegate( hopUi );
    } catch ( InstanceCreationException e ) {
      trans = new HopUiTransformationDelegate( hopUi );
    }
    if ( origTrans != null ) {
      // preseve open trans
      for ( TransMeta transMeta : origTrans.getLoadedTransformations() ) {
        trans.addTransformation( transMeta );
      }
    }
  }
}
