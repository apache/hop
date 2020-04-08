/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.workflow.actions.pgpverify;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.actions.pgpencryptfiles.GPG;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.util.List;
import java.util.Map;

/**
 * This defines a PGP verify action.
 *
 * @author Samatar
 * @since 25-02-2011
 */

@Action(
  id = "PGP_VERIFY_FILES",
  i18nPackageName = "org.apache.hop.workflow.actions.pgpverify",
  name = "ActionPGPVerify.Name",
  description = "ActionPGPVerify.Description",
  image = "PGPVerify.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileEncryption"
)
public class ActionPGPVerify extends ActionBase implements Cloneable, IAction {
  private static Class<?> PKG = ActionPGPVerify.class; // for i18n purposes, needed by Translator!!

  private String gpglocation;
  private String filename;
  private String detachedfilename;
  private boolean useDetachedSignature;

  public ActionPGPVerify( String n ) {
    super( n, "" );
    gpglocation = null;
    filename = null;
    detachedfilename = null;
    useDetachedSignature = false;
  }

  public ActionPGPVerify() {
    this( "" );
  }

  public Object clone() {
    ActionPGPVerify je = (ActionPGPVerify) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 100 );

    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "gpglocation", gpglocation ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "filename", filename ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "detachedfilename", detachedfilename ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "useDetachedSignature", useDetachedSignature ) );
    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IMetaStore metaStore ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      gpglocation = XmlHandler.getTagValue( entrynode, "gpglocation" );
      filename = XmlHandler.getTagValue( entrynode, "filename" );
      detachedfilename = XmlHandler.getTagValue( entrynode, "detachedfilename" );
      useDetachedSignature = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "useDetachedSignature" ) );

    } catch ( HopXmlException xe ) {
      throw new HopXmlException( BaseMessages.getString(
        PKG, "ActionPGPVerify.ERROR_0001_Cannot_Load_Job_Entry_From_Xml_Node" ), xe );
    }
  }

  public void setGPGLocation( String gpglocation ) {
    this.gpglocation = gpglocation;
  }

  public String getGPGLocation() {
    return gpglocation;
  }

  public void setFilename( String filename ) {
    this.filename = filename;
  }

  public String getFilename() {
    return filename;
  }

  public void setDetachedfilename( String detachedfilename ) {
    this.detachedfilename = detachedfilename;
  }

  public String getDetachedfilename() {
    return detachedfilename;
  }

  public void setUseDetachedfilename( boolean useDetachedSignature ) {
    this.useDetachedSignature = useDetachedSignature;
  }

  public boolean useDetachedfilename() {
    return useDetachedSignature;
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setResult( false );
    result.setNrErrors( 1 );

    FileObject file = null;
    FileObject detachedSignature = null;
    try {

      String realFilename = environmentSubstitute( getFilename() );
      if ( Utils.isEmpty( realFilename ) ) {
        logError( BaseMessages.getString( PKG, "JobPGPVerify.FilenameMissing" ) );
        return result;
      }
      file = HopVFS.getFileObject( realFilename );

      GPG gpg = new GPG( environmentSubstitute( getGPGLocation() ), log );

      if ( useDetachedfilename() ) {
        String signature = environmentSubstitute( getDetachedfilename() );

        if ( Utils.isEmpty( signature ) ) {
          logError( BaseMessages.getString( PKG, "JobPGPVerify.DetachedSignatureMissing" ) );
          return result;
        }
        detachedSignature = HopVFS.getFileObject( signature );

        gpg.verifyDetachedSignature( detachedSignature, file );
      } else {
        gpg.verifySignature( file );
      }

      result.setNrErrors( 0 );
      result.setResult( true );

    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "JobPGPVerify.Error" ), e );
    } finally {
      try {
        if ( file != null ) {
          file.close();
        }
        if ( detachedSignature != null ) {
          detachedSignature.close();
        }
      } catch ( Exception e ) { /* Ignore */
      }
    }

    return result;
  }

  public boolean evaluates() {
    return true;
  }

  public List<ResourceReference> getResourceDependencies( WorkflowMeta workflowMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( workflowMeta );
    if ( !Utils.isEmpty( gpglocation ) ) {
      String realFileName = workflowMeta.environmentSubstitute( gpglocation );
      ResourceReference reference = new ResourceReference( this );
      reference.getEntries().add( new ResourceEntry( realFileName, ResourceType.FILE ) );
      references.add( reference );
    }
    return references;
  }

  @Override
  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IMetaStore metaStore ) {
    ActionValidatorUtils.andValidator().validate( this, "gpglocation", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
  }

  /**
   * Exports the object to a flat-file system, adding content with filename keys to a set of definitions. The supplied
   * resource naming interface allows the object to name appropriately without worrying about those parts of the
   * implementation specific details.
   *
   * @param variables           The variable space to resolve (environment) variables with.
   * @param definitions     The map containing the filenames and content
   * @param namingInterface The resource naming interface allows the object to be named appropriately
   * @param metaStore       the metaStore to load external metadata from
   * @return The filename for this object. (also contained in the definitions map)
   * @throws HopException in case something goes wrong during the export
   */
  public String exportResources( IVariables variables, Map<String, ResourceDefinition> definitions,
                                 IResourceNaming namingInterface, IMetaStore metaStore ) throws HopException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the gpglocation from relative to absolute by grabbing the file object...
      // In case the name of the file comes from previous transforms, forget about this!
      //
      if ( !Utils.isEmpty( gpglocation ) ) {
        // From : ${FOLDER}/../foo/bar.csv
        // To : /home/matt/test/files/foo/bar.csv
        //
        FileObject fileObject = HopVFS.getFileObject( variables.environmentSubstitute( gpglocation ), variables );

        // If the file doesn't exist, forget about this effort too!
        //
        if ( fileObject.exists() ) {
          // Convert to an absolute path...
          //
          gpglocation = namingInterface.nameResource( fileObject, variables, true );

          return gpglocation;
        }
      }
      return null;
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }
}
