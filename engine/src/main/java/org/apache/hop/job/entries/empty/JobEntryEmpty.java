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

package org.apache.hop.job.entries.empty;

import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryBase;
import org.apache.hop.job.entry.IJobEntry;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

public class JobEntryEmpty extends JobEntryBase implements IJobEntry {
  public Result execute( Result prev_result, int nr ) throws HopException {
    return null;
  }

  public void loadXML( Node entrynode,
                       IMetaStore metaStore ) throws HopXMLException {

  }

  public void check( List<ICheckResult> remarks, JobMeta jobMeta, IVariables variables,
                     IMetaStore metaStore ) {

  }

}
