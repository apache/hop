/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.job.entries.abort;

import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.JobEntry;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryBase;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.job.entry.validator.JobEntryValidatorUtils;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Job entry type to abort a job.
 *
 * @author Samatar
 * @since 12-02-2007
 */
@JobEntry( id = "ABORT",
  i18nPackageName = "org.apache.hop.job.entries.abort",
  name = "JobEntryAbort.Name",
  description = "JobEntryAbort.Description",
  categoryDescription = "i18n:org.apache.hop.job:JobCategory.Category.Utility" )
public class JobEntryAbort extends JobEntryBase implements Cloneable, JobEntryInterface {
  private static Class<?> PKG = JobEntryAbort.class; // for i18n purposes, needed by Translator!!

  private String messageAbort;

  public JobEntryAbort( String n, String scr ) {
    super( n, "" );
    messageAbort = null;
  }

  public JobEntryAbort() {
    this( "", "" );
  }

  public Object clone() {
    JobEntryAbort je = (JobEntryAbort) super.clone();
    return je;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( super.getXML() );
    retval.append( "      " ).append( XMLHandler.addTagValue( "message", messageAbort ) );

    return retval.toString();
  }

  public void loadXML( Node entrynode, IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode );
      messageAbort = XMLHandler.getTagValue( entrynode, "message" );
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString( PKG, "JobEntryAbort.UnableToLoadFromXml.Label" ), e );
    }
  }

  public boolean evaluate( Result result ) {
    String Returnmessage = null;
    String RealMessageabort = environmentSubstitute( getMessageabort() );

    try {
      // Return False
      if ( RealMessageabort == null ) {
        Returnmessage = BaseMessages.getString( PKG, "JobEntryAbort.Meta.CheckResult.Label" );
      } else {
        Returnmessage = RealMessageabort;

      }
      logError( Returnmessage );
      result.setNrErrors( 1 );
      return false;
    } catch ( Exception e ) {
      result.setNrErrors( 1 );
      logError( BaseMessages.getString( PKG, "JobEntryAbort.Meta.CheckResult.CouldNotExecute" ) + e.toString() );
      return false;
    }
  }

  /**
   * Execute this job entry and return the result. In this case it means, just set the result boolean in the Result
   * class.
   *
   * @param previousResult The result of the previous execution
   * @return The Result of the execution.
   */
  public Result execute( Result previousResult, int nr ) {
    previousResult.setResult( evaluate( previousResult ) );
    // we fail so stop
    // job execution
    parentJob.stopAll();
    return previousResult;
  }

  public boolean resetErrorsBeforeExecution() {
    // we should be able to evaluate the errors in
    // the previous jobentry.
    return false;
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }

  public void setMessageabort( String messageabort ) {
    this.messageAbort = messageabort;
  }

  public String getMessageabort() {
    return messageAbort;
  }

  public void check( List<CheckResultInterface> remarks, JobMeta jobMeta, VariableSpace space,
                     IMetaStore metaStore ) {
    JobEntryValidatorUtils.addOkRemark( this, "messageabort", remarks );
  }
}
