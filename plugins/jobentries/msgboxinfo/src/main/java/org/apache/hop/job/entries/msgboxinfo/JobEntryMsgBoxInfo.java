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

package org.apache.hop.job.entries.msgboxinfo;

import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.JobEntry;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.gui.GUIFactory;
import org.apache.hop.core.gui.ThreadDialogs;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryBase;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.job.entry.validator.JobEntryValidatorUtils;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Job entry type to display a message box.
 *
 * @author Samatar
 * @since 12-02-2007
 */

@JobEntry(
  id = "MSGBOX_INFO",
  i18nPackageName = "org.apache.hop.job.entries.msgboxinfo",
  name = "JobEntryMsgBoxInfo.Name",
  description = "JobEntryMsgBoxInfo.Description",
  image = "MsgBoxInfo.svg",
  categoryDescription = "i18n:org.apache.hop.job:JobCategory.Category.Utility"
)
public class JobEntryMsgBoxInfo extends JobEntryBase implements Cloneable, JobEntryInterface {
  private String bodymessage;
  private String titremessage;

  public JobEntryMsgBoxInfo( String n, String scr ) {
    super( n, "" );
    bodymessage = null;
    titremessage = null;
  }

  public JobEntryMsgBoxInfo() {
    this( "", "" );
  }

  public Object clone() {
    JobEntryMsgBoxInfo je = (JobEntryMsgBoxInfo) super.clone();
    return je;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 50 );

    retval.append( super.getXML() );
    retval.append( "      " ).append( XMLHandler.addTagValue( "bodymessage", bodymessage ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "titremessage", titremessage ) );

    return retval.toString();
  }

  public void loadXML( Node entrynode,
                       IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode );
      bodymessage = XMLHandler.getTagValue( entrynode, "bodymessage" );
      titremessage = XMLHandler.getTagValue( entrynode, "titremessage" );
    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load job entry of type 'Msgbox Info' from XML node", e );
    }
  }

  /**
   * Display the Message Box.
   */
  public boolean evaluate( Result result ) {
    try {
      // default to ok

      // Try to display MSGBOX
      boolean response = true;

      ThreadDialogs dialogs = GUIFactory.getThreadDialogs();
      if ( dialogs != null ) {
        response =
          dialogs.threadMessageBox( getRealBodyMessage() + Const.CR, getRealTitleMessage(), true, Const.INFO );
      }

      return response;

    } catch ( Exception e ) {
      result.setNrErrors( 1 );
      logError( "Couldn't display message box: " + e.toString() );
      return false;
    }

  }

  /**
   * Execute this job entry and return the result. In this case it means, just set the result boolean in the Result
   * class.
   *
   * @param prev_result The result of the previous execution
   * @return The Result of the execution.
   */
  public Result execute( Result prev_result, int nr ) {
    prev_result.setResult( evaluate( prev_result ) );
    return prev_result;
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

  public String getRealTitleMessage() {
    return environmentSubstitute( getTitleMessage() );
  }

  public String getRealBodyMessage() {
    return environmentSubstitute( getBodyMessage() );
  }

  public String getTitleMessage() {
    if ( titremessage == null ) {
      titremessage = "";
    }
    return titremessage;
  }

  public String getBodyMessage() {
    if ( bodymessage == null ) {
      bodymessage = "";
    }
    return bodymessage;

  }

  public void setBodyMessage( String s ) {

    bodymessage = s;

  }

  public void setTitleMessage( String s ) {

    titremessage = s;

  }

  @Override
  public void check( List<CheckResultInterface> remarks, JobMeta jobMeta, VariableSpace space,
                     IMetaStore metaStore ) {
    JobEntryValidatorUtils.addOkRemark( this, "bodyMessage", remarks );
    JobEntryValidatorUtils.addOkRemark( this, "titleMessage", remarks );
  }

}
