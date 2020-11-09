/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.workflow.actions.xml.xslt;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.*;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.w3c.dom.Node;

import javax.xml.transform.Source;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Properties;

import static org.apache.hop.workflow.action.validator.AbstractFileValidator.putVariableSpace;
import static org.apache.hop.workflow.action.validator.ActionValidatorUtils.*;
import static org.apache.hop.workflow.action.validator.AndValidator.putValidators;

/**
 * This defines a 'xslt' job entry.
 * 
 * @author Samatar Hassan
 * @since 02-03-2007
 * 
 */
@Action( id = "XSLT",
        i18nPackageName = "org.apache.hop.workflow.actions.xml.xslt",
        name = "XSLT.Name",
        description = "XSLT.Description",
        image = "XSLT.svg",
        categoryDescription = "XSLT.Category",               
        documentationUrl = "https://www.project-hop.org/manual/latest/plugins/actions/xslt.html"
)
public class Xslt extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = Xslt.class; // for i18n purposes, needed by Translator2!!

  public static String FACTORY_JAXP = "JAXP";
  public static String FACTORY_SAXON = "SAXON";

  private String xmlfilename;
  private String xslfilename;
  private String outputfilename;
  public int ifFileExists;
  private boolean addfiletoresult;
  private String xsltfactory;
  private boolean filenamesfromprevious;

  /** output property name */
  private String[] outputPropertyName;

  /** output property value */
  private String[] outputPropertyValue;

  /** parameter name */
  private String[] parameterName;

  /** parameter field */
  private String[] parameterField;

  private int nrParams;
  private String[] nameOfParams;
  private String[] valueOfParams;
  private boolean useParameters;

  private Properties outputProperties;
  private boolean setOutputProperties;

  public Xslt(String n ) {
    super( n, "" );
    xmlfilename = null;
    xslfilename = null;
    outputfilename = null;
    ifFileExists = 1;
    addfiletoresult = false;
    filenamesfromprevious = false;
    xsltfactory = FACTORY_JAXP;
    int nrparams = 0;
    int nroutputproperties = 0;
    allocate( nrparams, nroutputproperties );

    for ( int i = 0; i < nrparams; i++ ) {
      parameterField[i] = "param" + i;
      parameterName[i] = "param";
    }
    for ( int i = 0; i < nroutputproperties; i++ ) {
      outputPropertyName[i] = "outputprop" + i;
      outputPropertyValue[i] = "outputprop";
    }
  }

  public void allocate( int nrParameters, int outputProps ) {
    parameterName = new String[nrParameters];
    parameterField = new String[nrParameters];

    outputPropertyName = new String[outputProps];
    outputPropertyValue = new String[outputProps];
  }

  public Xslt() {
    this( "" );
  }

  public Object clone() {
    Xslt je = (Xslt) super.clone();
    int nrparams = parameterName.length;
    int nroutputprops = outputPropertyName.length;
    je.allocate( nrparams, nroutputprops );

    for ( int i = 0; i < nrparams; i++ ) {
      je.parameterName[i] = parameterName[i];
      je.parameterField[i] = parameterField[i];
    }
    for ( int i = 0; i < nroutputprops; i++ ) {
      je.outputPropertyName[i] = outputPropertyName[i];
      je.outputPropertyValue[i] = outputPropertyValue[i];
    }

    return je;
  }

  public String getXML() {
    StringBuffer retval = new StringBuffer( 50 );

    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "xmlfilename", xmlfilename ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "xslfilename", xslfilename ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "outputfilename", outputfilename ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "iffileexists", ifFileExists ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "addfiletoresult", addfiletoresult ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "filenamesfromprevious", filenamesfromprevious ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "xsltfactory", xsltfactory ) );

    retval.append( "    <parameters>" ).append( Const.CR );

    for ( int i = 0; i < parameterName.length; i++ ) {
      retval.append( "      <parameter>" ).append( Const.CR );
      retval.append( "        " ).append( XmlHandler.addTagValue( "field", parameterField[i] ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( "name", parameterName[i] ) );
      retval.append( "      </parameter>" ).append( Const.CR );
    }

    retval.append( "    </parameters>" ).append( Const.CR );
    retval.append( "    <outputproperties>" ).append( Const.CR );

    for ( int i = 0; i < outputPropertyName.length; i++ ) {
      retval.append( "      <outputproperty>" ).append( Const.CR );
      retval.append( "        " ).append( XmlHandler.addTagValue( "name", outputPropertyName[i] ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( "value", outputPropertyValue[i] ) );
      retval.append( "      </outputproperty>" ).append( Const.CR );
    }

    retval.append( "    </outputproperties>" ).append( Const.CR );
    return retval.toString();
  }

  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      xmlfilename = XmlHandler.getTagValue( entrynode, "xmlfilename" );
      xslfilename = XmlHandler.getTagValue( entrynode, "xslfilename" );
      outputfilename = XmlHandler.getTagValue( entrynode, "outputfilename" );
      ifFileExists = Const.toInt( XmlHandler.getTagValue( entrynode, "iffileexists" ), -1 );
      addfiletoresult = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "addfiletoresult" ) );
      filenamesfromprevious = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "filenamesfromprevious" ) );
      xsltfactory = XmlHandler.getTagValue( entrynode, "xsltfactory" );
      if ( xsltfactory == null ) {
        xsltfactory = FACTORY_JAXP;
      }
      Node parametersNode = XmlHandler.getSubNode( entrynode, "parameters" );
      int nrparams = XmlHandler.countNodes( parametersNode, "parameter" );

      Node parametersOutputProps = XmlHandler.getSubNode( entrynode, "outputproperties" );
      int nroutputprops = XmlHandler.countNodes( parametersOutputProps, "outputproperty" );
      allocate( nrparams, nroutputprops );

      for ( int i = 0; i < nrparams; i++ ) {
        Node anode = XmlHandler.getSubNodeByNr( parametersNode, "parameter", i );
        parameterField[i] = XmlHandler.getTagValue( anode, "field" );
        parameterName[i] = XmlHandler.getTagValue( anode, "name" );
      }
      for ( int i = 0; i < nroutputprops; i++ ) {
        Node anode = XmlHandler.getSubNodeByNr( parametersOutputProps, "outputproperty", i );
        outputPropertyName[i] = XmlHandler.getTagValue( anode, "name" );
        outputPropertyValue[i] = XmlHandler.getTagValue( anode, "value" );
      }

    } catch ( HopXmlException xe ) {
      throw new HopXmlException( "Unable to load job entry of type 'xslt' from XML node", xe );
    }
  }
  
  public String getXSLTFactory() {
    return xsltfactory;
  }

  public void setXSLTFactory( String xsltfactoryin ) {
    xsltfactory = xsltfactoryin;
  }

  public String getRealxmlfilename() {
    return environmentSubstitute( getxmlFilename() );
  }

  public String getoutputfilename() {
    return environmentSubstitute( getoutputFilename() );
  }

  public boolean isFilenamesFromPrevious() {
    return filenamesfromprevious;
  }

  public void setFilenamesFromPrevious( boolean filenamesfromprevious ) {
    this.filenamesfromprevious = filenamesfromprevious;
  }

  public String getRealxslfilename() {
    return environmentSubstitute( getxslFilename() );
  }

  public Result execute(Result previousResult, int nr ) throws HopException {
    Result result = previousResult;
    int nrErrors = 0;
    int nrSuccess = 0;

    // Check output parameters
    int nrOutputProps = getOutputPropertyName() == null ? 0 : getOutputPropertyName().length;
    if ( nrOutputProps > 0 ) {
      outputProperties = new Properties();
      for ( int i = 0; i < nrOutputProps; i++ ) {
        outputProperties.put( getOutputPropertyName()[i], environmentSubstitute( getOutputPropertyValue()[i] ) );
      }
      setOutputProperties = true;
    }

    // Check parameters
    nrParams = getParameterField() == null ? 0 : getParameterField().length;
    if ( nrParams > 0 ) {
      nameOfParams = new String[nrParams];
      valueOfParams = new String[nrParams];
      for ( int i = 0; i < nrParams; i++ ) {
        String name = environmentSubstitute( getParameterName()[i] );
        String value = environmentSubstitute( getParameterField()[i] );
        if ( Utils.isEmpty( value ) ) {
          throw new HopTransformException( BaseMessages.getString( PKG, "Xslt.Exception.ParameterFieldMissing", name, i ) );
        }
        nameOfParams[i] = name;
        valueOfParams[i] = value;
      }
      useParameters = true;
    }

    List<RowMetaAndData> rows = result.getRows();
    if ( isFilenamesFromPrevious() ) {
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobEntryXSLT.Log.ArgFromPrevious.Found", ( rows != null ? rows
            .size() : 0 )
            + "" ) );
      }
    }

    if ( isFilenamesFromPrevious() && rows != null ) {
      // Copy the input row to the (command line) arguments
      RowMetaAndData resultRow = null;
      for ( int iteration = 0; iteration < rows.size() && !parentWorkflow.isStopped(); iteration++ ) {
        resultRow = rows.get( iteration );

        // Get filenames (xml, xsl, output filename)
        String xmlfilenamePrevious = resultRow.getString( 0, null );
        String xslfilenamePrevious = resultRow.getString( 1, null );
        String ouputfilenamePrevious = resultRow.getString( 2, null );

        if ( !Utils.isEmpty( xmlfilenamePrevious ) && !Utils.isEmpty( xslfilenamePrevious )
            && !Utils.isEmpty( ouputfilenamePrevious ) ) {
          if ( processOneXMLFile( xmlfilenamePrevious, xslfilenamePrevious, ouputfilenamePrevious, result, parentWorkflow ) ) {
            nrSuccess++;
          } else {
            nrErrors++;
          }
        } else {
          // We failed!
          logError( BaseMessages.getString( PKG, "JobEntryXSLT.AllFilesNotNull.Label" ) );
          nrErrors++;
        }

      }
    } else {
      String realxmlfilename = getRealxmlfilename();
      String realxslfilename = getRealxslfilename();
      String realoutputfilename = getoutputfilename();
      if ( !Utils.isEmpty( realxmlfilename ) && !Utils.isEmpty( realxslfilename )
          && !Utils.isEmpty( realoutputfilename ) ) {
        if ( processOneXMLFile( realxmlfilename, realxslfilename, realoutputfilename, result, parentWorkflow ) ) {
          nrSuccess++;
        } else {
          nrErrors++;
        }
      } else {
        // We failed!
        logError( BaseMessages.getString( PKG, "JobEntryXSLT.AllFilesNotNull.Label" ) );
        nrErrors++;
      }
    }

    result.setResult( nrErrors == 0 );
    result.setNrErrors( nrErrors );
    result.setNrLinesWritten( nrSuccess );

    return result;
  }

  private boolean processOneXMLFile( String xmlfilename, String xslfilename, String outputfilename, Result result,
      IWorkflowEngine parentWorkflow ) {
    boolean retval = false;
    FileObject xmlfile = null;
    FileObject xslfile = null;
    FileObject outputfile = null;

    try {
      xmlfile = HopVfs.getFileObject( xmlfilename );
      xslfile = HopVfs.getFileObject( xslfilename );
      outputfile = HopVfs.getFileObject( outputfilename );

      if ( xmlfile.exists() && xslfile.exists() ) {
        if ( outputfile.exists() && ifFileExists == 2 ) {
          // Output file exists
          // User want to fail
          logError( BaseMessages.getString( PKG, "JobEntryXSLT.OuputFileExists1.Label" ) + outputfilename
              + BaseMessages.getString( PKG, "JobEntryXSLT.OuputFileExists2.Label" ) );
          return retval;

        } else if ( outputfile.exists() && ifFileExists == 1 ) {
          // Do nothing
          if ( log.isDebug() ) {
            logDebug( BaseMessages.getString( PKG, "JobEntryXSLT.OuputFileExists1.Label" ) + outputfilename
                + BaseMessages.getString( PKG, "JobEntryXSLT.OuputFileExists2.Label" ) );
          }
          retval = true;
          return retval;

        } else {
          if ( outputfile.exists() && ifFileExists == 0 ) {
            // the output file exists and user want to create new one with unique name
            // Format Date

            // Try to clean filename (without wildcard)
            String wildcard = outputfilename.substring( outputfilename.length() - 4, outputfilename.length() );
            if ( wildcard.substring( 0, 1 ).equals( "." ) ) {
              // Find wildcard
              outputfilename =
                  outputfilename.substring( 0, outputfilename.length() - 4 ) + "_"
                      + StringUtil.getFormattedDateTimeNow( true ) + wildcard;
            } else {
              // did not find wildcard
              outputfilename = outputfilename + "_" + StringUtil.getFormattedDateTimeNow( true );
            }
            if ( log.isDebug() ) {
              logDebug( BaseMessages.getString( PKG, "JobEntryXSLT.OuputFileExists1.Label" ) + outputfilename
                  + BaseMessages.getString( PKG, "JobEntryXSLT.OuputFileExists2.Label" ) );
              logDebug( BaseMessages.getString( PKG, "JobEntryXSLT.OuputFileNameChange1.Label" ) + outputfilename
                  + BaseMessages.getString( PKG, "JobEntryXSLT.OuputFileNameChange2.Label" ) );
            }
          }

          // Create transformer factory
          TransformerFactory factory = TransformerFactory.newInstance();

          if ( xsltfactory.equals( FACTORY_SAXON ) ) {
            // Set the TransformerFactory to the SAXON implementation.
            factory = new net.sf.saxon.TransformerFactoryImpl();
          }

          if ( log.isDetailed() ) {
            log.logDetailed( BaseMessages.getString( PKG, "JobEntryXSL.Log.TransformerFactoryInfos" ), BaseMessages
                .getString( PKG, "JobEntryXSL.Log.TransformerFactory", factory.getClass().getName() ) );
          }

          InputStream xslInputStream = HopVfs.getInputStream( xslfile );
          InputStream xmlInputStream = HopVfs.getInputStream( xmlfile );
          OutputStream os = null;
          try {
            // Use the factory to create a template containing the xsl file
            Templates template = factory.newTemplates( new StreamSource( xslInputStream ) );

            // Use the template to create a transformer
            Transformer xformer = template.newTransformer();

            if ( log.isDetailed() ) {
              log.logDetailed( BaseMessages.getString( PKG, "JobEntryXSL.Log.TransformerClassInfos" ), BaseMessages
                  .getString( PKG, "JobEntryXSL.Log.TransformerClass", xformer.getClass().getName() ) );
            }

            // Do we need to set output properties?
            if ( setOutputProperties ) {
              xformer.setOutputProperties( outputProperties );
            }

            // Do we need to pass parameters?
            if ( useParameters ) {
              for ( int i = 0; i < nrParams; i++ ) {
                xformer.setParameter( nameOfParams[i], valueOfParams[i] );
              }
            }

            // Prepare the input and output files
            Source source = new StreamSource( xmlInputStream );
            os = HopVfs.getOutputStream( outputfile, false );
            StreamResult resultat = new StreamResult( os );

            // Apply the xsl file to the source file and write the result to the output file
            xformer.transform( source, resultat );

            if ( isAddFileToResult() ) {
              // Add output filename to output files
              ResultFile resultFile =
                  new ResultFile( ResultFile.FILE_TYPE_GENERAL, HopVfs.getFileObject( outputfilename ),
                      parentWorkflow.getWorkflowName(), toString() );
              result.getResultFiles().put( resultFile.getFile().toString(), resultFile );
            }

            // Everything is OK
            retval = true;
          } finally {
            try {
              xslInputStream.close();
            } catch ( IOException ignored ) {
              // ignore IO Exception on close
            }
            try {
              xmlInputStream.close();
            } catch ( IOException ignored ) {
              // ignore IO Exception on close
            }
            try {
              if ( os != null ) {
                os.close();
              }
            } catch ( IOException ignored ) {
              // ignore IO Exception on close
            }
          }
        }
      } else {

        if ( !xmlfile.exists() ) {
          logError( BaseMessages.getString( PKG, "JobEntryXSLT.FileDoesNotExist1.Label" ) + xmlfilename
              + BaseMessages.getString( PKG, "JobEntryXSLT.FileDoesNotExist2.Label" ) );
        }
        if ( !xslfile.exists() ) {
          logError( BaseMessages.getString( PKG, "JobEntryXSLT.FileDoesNotExist1.Label" ) + xmlfilename
              + BaseMessages.getString( PKG, "JobEntryXSLT.FileDoesNotExist2.Label" ) );
        }
      }
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "JobEntryXSLT.ErrorXLST.Label" )
          + BaseMessages.getString( PKG, "JobEntryXSLT.ErrorXLSTXML1.Label" ) + xmlfilename
          + BaseMessages.getString( PKG, "JobEntryXSLT.ErrorXLSTXML2.Label" )
          + BaseMessages.getString( PKG, "JobEntryXSLT.ErrorXLSTXSL1.Label" ) + xslfilename
          + BaseMessages.getString( PKG, "JobEntryXSLT.ErrorXLSTXSL2.Label" ) + e.getMessage() );
    } finally {
      try {
        if ( xmlfile != null ) {
          xmlfile.close();
        }

        if ( xslfile != null ) {
          xslfile.close();
        }
        if ( outputfile != null ) {
          outputfile.close();
        }
      } catch ( IOException e ) {
        logError( "Unable to close file", e );
      }
    }

    return retval;
  }

  public boolean evaluates() {
    return true;
  }

  public void setxmlFilename( String filename ) {
    this.xmlfilename = filename;
  }

  public String getxmlFilename() {
    return xmlfilename;
  }

  public String getoutputFilename() {
    return outputfilename;
  }

  public void setoutputFilename( String outputfilename ) {
    this.outputfilename = outputfilename;
  }

  public void setxslFilename( String filename ) {
    this.xslfilename = filename;
  }

  public String getxslFilename() {
    return xslfilename;
  }

  public void setAddFileToResult( boolean addfiletoresultin ) {
    this.addfiletoresult = addfiletoresultin;
  }

  public boolean isAddFileToResult() {
    return addfiletoresult;
  }

  public List<ResourceReference> getResourceDependencies(WorkflowMeta jobMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( jobMeta );
    if ( ( !Utils.isEmpty( xslfilename ) ) && ( !Utils.isEmpty( xmlfilename ) ) ) {
      String realXmlFileName = jobMeta.environmentSubstitute( xmlfilename );
      String realXslFileName = jobMeta.environmentSubstitute( xslfilename );
      ResourceReference reference = new ResourceReference( this );
      reference.getEntries().add( new ResourceEntry( realXmlFileName, ResourceEntry.ResourceType.FILE ) );
      reference.getEntries().add( new ResourceEntry( realXslFileName, ResourceEntry.ResourceType.FILE ) );
      references.add( reference );
    }
    return references;
  }

  @Override
  public void check(List<ICheckResult> remarks, WorkflowMeta jobMeta, IVariables space, IHopMetadataProvider metadataProvider ) {
    ValidatorContext ctx = new ValidatorContext();
    putVariableSpace( ctx, getVariables() );
    putValidators( ctx, notBlankValidator(), fileExistsValidator() );
    andValidator().validate( this, "xmlFilename", remarks, ctx );
    andValidator().validate( this, "xslFilename", remarks, ctx );

    andValidator().validate( this, "outputFilename", remarks, putValidators( notBlankValidator() ) );
  }

  /**
   * @return Returns the OutputPropertyName.
   */
  public String[] getOutputPropertyName() {
    return outputPropertyName;
  }

  /**
   * @param argumentDirection
   *          The OutputPropertyName to set.
   */
  public void setOutputPropertyName( String[] argumentDirection ) {
    this.outputPropertyName = argumentDirection;
  }

  /**
   * @return Returns the OutputPropertyField.
   */
  public String[] getOutputPropertyValue() {
    return outputPropertyValue;
  }

  /**
   * @param argumentDirection
   *          The outputPropertyValue to set.
   */
  public void setOutputPropertyValue( String[] argumentDirection ) {
    this.outputPropertyValue = argumentDirection;
  }

  /**
   * @return Returns the parameterName.
   */
  public String[] getParameterName() {
    return parameterName;
  }

  /**
   * @param argumentDirection
   *          The parameterName to set.
   */
  public void setParameterName( String[] argumentDirection ) {
    this.parameterName = argumentDirection;
  }

  /**
   * @return Returns the parameterField.
   */
  public String[] getParameterField() {
    return parameterField;
  }
}
