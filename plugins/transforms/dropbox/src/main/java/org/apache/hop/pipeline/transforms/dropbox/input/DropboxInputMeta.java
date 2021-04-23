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

package org.apache.hop.pipeline.transforms.dropbox.input;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.*;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.w3c.dom.Node;

import java.util.List;
import java.util.Objects;

@Transform(
        id = "DropboxInput",
        image = "dropboxinput.svg",
        name = "i18n::BaseTransform.TypeLongDesc.DropboxInput",
        description = "i18n::BaseTransform.TypeTooltipDesc.DropboxInput",
        categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
        documentationUrl =
                "https://hop.apache.org/manual/latest/pipeline/transforms/dropboxinput.html"
)
public class DropboxInputMeta extends BaseTransformMeta implements ITransformMeta<DropboxInput, DropboxInputData> {

    private static Class<?> PKG = DropboxInput.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

    public void setSuccessfulTransformName(String successfulTransformName ) {
        getTransformIOMeta().getTargetStreams().get( 0 ).setSubject( successfulTransformName );
    }

    public String getSuccessfulTransformName() {
        return getTargetTransformName( 0 );
    }

    public void setFailedTransformName( String failedTransformName ) {
        getTransformIOMeta().getTargetStreams().get( 1 ).setSubject( failedTransformName );
    }

    public String getFailedTransformName() {
        return getTargetTransformName( 1 );
    }

    private String getTargetTransformName( int streamIndex ) {
        IStream stream = getTransformIOMeta().getTargetStreams().get( streamIndex );
        return java.util.stream.Stream.of( stream.getTransformName(), stream.getSubject() )
                .filter( Objects::nonNull )
                .findFirst().map( Object::toString ).orElse( null );
    }

    public String getAccessTokenField() {
        return accessTokenField;
    }

    public void setAccessTokenField( String accessTokenField ) {
        this.accessTokenField = accessTokenField;
    }

    public String getSourceFilesField() {
        return sourceFilesField;
    }

    public void setSourceFilesField( String sourceFilesField ) {
        this.sourceFilesField = sourceFilesField;
    }

    public String getTargetFilesField() {
        return targetFilesField;
    }

    public void setTargetFilesField( String targetFilesField ) {
        this.targetFilesField = targetFilesField;
    }

    private String accessTokenField;
    private String sourceFilesField;
    private String targetFilesField;

    public DropboxInputMeta() {
        super(); // allocate BaseTransformMeta
    }

    public void loadXML(Node transformNodeode, List<DatabaseMeta> databases, IHopMetadataProvider metadataProvider ) throws HopXmlException {
        readData( transformNodeode );
    }

    public Object clone() {
        Object retval = super.clone();
        return retval;
    }

    private void readData( Node transformNode ) {
        setSuccessfulTransformName( XmlHandler.getTagValue( transformNode, "sendSuccessfulTo" ) );
        setFailedTransformName( XmlHandler.getTagValue( transformNode, "sendFailedTo" ) );
        accessTokenField = XmlHandler.getTagValue( transformNode, "accessTokenField" );
        sourceFilesField = XmlHandler.getTagValue( transformNode, "sourceFilesField" );
        targetFilesField = XmlHandler.getTagValue( transformNode, "targetFilesField" );
    }

    public void setDefault() {
    }

    @Override
    public String getXml() {
        StringBuilder retval = new StringBuilder();
        retval.append( "    " + XmlHandler.addTagValue( "sendSuccessfulTo", getSuccessfulTransformName() ) );
        retval.append( "    " + XmlHandler.addTagValue( "sendFailedTo", getFailedTransformName() ) );
        retval.append( "    " + XmlHandler.addTagValue( "accessTokenField", accessTokenField ) );
        retval.append( "    " + XmlHandler.addTagValue( "sourceFilesField", sourceFilesField ) );
        retval.append( "    " + XmlHandler.addTagValue( "targetFilesField", targetFilesField ) );
        return retval.toString();
    }

    public void getFields(IRowMeta rowMeta, String origin, IRowMeta[] info, TransformMeta nextTransform,
                          IVariables space, IHopMetadataProvider metadataProvider) throws HopTransformException {

    }

    public void check(List<ICheckResult> remarks, PipelineMeta pipelineMeta,
                      TransformMeta transformMeta, IRowMeta prev, String[] input, String[] output,
                      IRowMeta info, IVariables space, IHopMetadataProvider metadataProvider ) {
        CheckResult cr;
        if ( prev == null || prev.size() == 0 ) {
            cr = new CheckResult( ICheckResult.TYPE_RESULT_WARNING, BaseMessages.getString( PKG, "DropboxInputMeta.CheckResult.NotReceivingFields" ), transformMeta );
            remarks.add( cr );
        } else {
            cr = new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString( PKG, "DropboxInputMeta.CheckResult.TransformRecevingData", prev.size() + "" ), transformMeta );
            remarks.add( cr );
        }

        // See if we have input streams leading to this transform!
        if ( input.length > 0 ) {
            cr = new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString( PKG, "DropboxInputMeta.CheckResult.TransformRecevingData2" ), transformMeta );
            remarks.add( cr );
        } else {
            cr = new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString( PKG, "DropboxInputMeta.CheckResult.NoInputReceivedFromOtherTransforms" ), transformMeta );
            remarks.add( cr );
        }
    }

    @Override
    public ITransform createTransform(TransformMeta transformMeta, DropboxInputData data, int cnr, PipelineMeta tr, Pipeline pipeline ) {
        return new DropboxInput( transformMeta, this, data, cnr, tr, pipeline );
    }

    public DropboxInputData getTransformData() {
        return new DropboxInputData();
    }

    @Override
    public boolean excludeFromCopyDistributeVerification() {
        return true;
    }

    @Override
    public void searchInfoAndTargetTransforms( List<TransformMeta> transforms ) {
        List<IStream> targetStreams = getTransformIOMeta().getTargetStreams();
        for ( IStream stream : targetStreams ) {
            stream.setTransformMeta( TransformMeta.findTransform( transforms, (String) stream.getSubject() ) );
        }
    }

    /**
     * Returns the Input/Output metadata for this transform.
     */
    public ITransformIOMeta getTransformIOMeta() {
        ITransformIOMeta ioMeta = super.getTransformIOMeta( false );
        if ( ioMeta == null ) {

            ioMeta = new TransformIOMeta( true, true, false, false, false, false );

            ioMeta.addStream( new Stream( IStream.StreamType.TARGET, null, BaseMessages.getString(
                    PKG, "DropboxInputMeta.InfoStream.Successful.Description" ), StreamIcon.TRUE, null ) );
            ioMeta.addStream( new Stream( IStream.StreamType.TARGET, null, BaseMessages.getString(
                    PKG, "DropboxInputMeta.InfoStream.Failed.Description" ), StreamIcon.FALSE, null ) );
            setTransformIOMeta( ioMeta );
        }

        return ioMeta;
    }

    @Override
    public void resetTransformIoMeta() {
    }

    /**
     * When an optional stream is selected, this method is called to handled the ETL metadata implications of that.
     *
     * @param stream
     *          The optional stream to handle.
     */
    public void handleStreamSelection( IStream stream ) {
        // This transform targets another transform.
        // Make sure that we don't specify the same transform for true and false...
        // If the user requests false, we blank out true and vice versa
        //
        List<IStream> targets = getTransformIOMeta().getTargetStreams();
        int index = targets.indexOf( stream );
        if ( index == 0 ) {
            // True
            //
            TransformMeta failedTransform = targets.get( 1 ).getTransformMeta();
            if ( failedTransform != null && failedTransform.equals( stream.getTransformMeta() ) ) {
                targets.get( 1 ).setTransformMeta( null );
            }
        }
        if ( index == 1 ) {
            // False
            //
            TransformMeta succesfulTransform = targets.get( 0 ).getTransformMeta();
            if ( succesfulTransform != null && succesfulTransform.equals( stream.getTransformMeta() ) ) {
                targets.get( 0 ).setTransformMeta( null );
            }
        }
    }
}
