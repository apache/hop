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

package org.apache.hop.pipeline.transforms.googleanalytics;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.AfterInjection;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Transform(
        id = "GoogleAnalytics",
        image = "googleanalytics.svg",
        name = "i18n::BaseTransform.TypeLongDesc.GoogleAnalytics",
        description = "i18n::BaseTransform.TypeTooltipDesc.GoogleAnalytics",
        categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
        documentationUrl = "https://hop.apache.org/manual/latest/pipeline/transforms/googleanalytics.html"
)
@InjectionSupported(
        localizationPrefix = "GoogleAnalytics.Injection.",
        groups = {"OUTPUT_FIELDS"})
public class GoogleAnalyticsMeta extends BaseTransformMeta implements ITransformMeta<GoogleAnalytics, GoogleAnalyticsData> {

    private static Class<?> PKG = GoogleAnalyticsMeta.class; // for i18n purposes

    public static final String FIELD_TYPE_DIMENSION = "Dimension";
    public static final String FIELD_TYPE_METRIC = "Metric";
    public static final String FIELD_TYPE_DATA_SOURCE_PROPERTY = "Data Source Property";
    public static final String FIELD_TYPE_DATA_SOURCE_FIELD = "Data Source Field";
    public static final String FIELD_DATA_SOURCE_TABLE_ID = "dxp:tableId";
    public static final String FIELD_DATA_SOURCE_TABLE_NAME = "dxp:tableName";
    public static final String PROPERTY_DATA_SOURCE_PROFILE_ID = "ga:profileId";
    public static final String PROPERTY_DATA_SOURCE_WEBPROP_ID = "ga:webPropertyId";
    public static final String PROPERTY_DATA_SOURCE_ACCOUNT_NAME = "ga:accountName";
    public static final String DEFAULT_GA_APPLICATION_NAME = "hop-google-analytics-app";

    // The following is deprecated and removed by Google, and remains here only to allow old transformations to load
    // successfully in Spoon.
    public static final String DEPRECATED_FIELD_TYPE_CONFIDENCE_INTERVAL = "Confidence Interval for Metric";

    @Injection( name = "OAUTH_SERVICE_EMAIL" )
    private String oauthServiceAccount;
    @Injection( name = "OAUTH_KEYFILE" )
    private String oauthKeyFile;

    @Injection( name = "APPLICATION_NAME" )
    private String gaAppName;
    @Injection( name = "PROFILE_TABLE" )
    private String gaProfileTableId;
    @Injection( name = "PROFILE_NAME" )
    private String gaProfileName;
    @Injection( name = "USE_CUSTOM_TABLE_ID" )
    private boolean useCustomTableId;
    @Injection( name = "CUSTOM_TABLE_ID" )
    private String gaCustomTableId;
    @Injection( name = "START_DATE" )
    private String startDate;
    @Injection( name = "END_DATE" )
    private String endDate;
    @Injection( name = "DIMENSIONS" )
    private String dimensions;
    @Injection( name = "METRICS" )
    private String metrics;
    @Injection( name = "FILTERS" )
    private String filters;
    @Injection( name = "SORT" )
    private String sort;
    @Injection( name = "USE_SEGMENT" )
    private boolean useSegment;

    @Injection( name = "USE_CUSTOM_SEGMENT" )
    private boolean useCustomSegment;
    @Injection( name = "ROW_LIMIT" )
    private int rowLimit;

    @Injection( name = "CUSTOM_SEGMENT" )
    private String customSegment;
    @Injection( name = "SEGMENT_NAME" )
    private String segmentName;
    @Injection( name = "SEGMENT_ID" )
    private String segmentId;

    private String samplingLevel;
    public static final String[] TYPE_SAMPLING_LEVEL_CODE = new String[] { "DEFAULT", "FASTER", "HIGHER_PRECISION" };

    @Injection( name = "FEED_FIELD", group = "OUTPUT_FIELDS" )
    private String[] feedField;
    @Injection( name = "FEED_FIELD_TYPE", group = "OUTPUT_FIELDS" )
    private String[] feedFieldType;
    @Injection( name = "OUTPUT_FIELD", group = "OUTPUT_FIELDS" )
    private String[] outputField;
    @Injection( name = "OUTPUT_TYPE", group = "OUTPUT_FIELDS", converter = OutputTypeConverter.class )
    private int[] outputType;
    @Injection( name = "CONVERSION_MASK", group = "OUTPUT_FIELDS" )
    private String[] conversionMask;

    public GoogleAnalyticsMeta() {
        super();
    }

    public int getRowLimit() {
        return rowLimit;
    }

    public void setRowLimit( int rowLimit ) {
        if ( rowLimit < 0 ) {
            rowLimit = 0;
        }
        this.rowLimit = rowLimit;
    }

    public String[] getConversionMask() {
        return conversionMask;
    }

    public void setConversionMask( String[] conversionMask ) {
        this.conversionMask = conversionMask;
    }

    public String getGaAppName() {
        return gaAppName;
    }

    public void setGaAppName( String gaAppName ) {
        this.gaAppName = gaAppName;
    }

    public boolean isUseCustomTableId() {
        return useCustomTableId;
    }

    public void setUseCustomTableId( boolean useCustomTableId ) {
        this.useCustomTableId = useCustomTableId;
    }

    public String getGaCustomTableId() {
        return gaCustomTableId;
    }

    public void setGaCustomTableId( String gaCustomTableId ) {
        this.gaCustomTableId = gaCustomTableId;
    }

    public boolean isUseSegment() {
        return useSegment;
    }

    public void setUseSegment( boolean useSegment ) {
        this.useSegment = useSegment;
    }

    public String getSegmentName() {
        return segmentName;
    }

    public void setSegmentName( String segmentName ) {
        this.segmentName = segmentName;
    }

    public String getSegmentId() {
        return segmentId;
    }

    public void setSegmentId( String segmentId ) {
        this.segmentId = segmentId;
    }

    public boolean isUseCustomSegment() {
        return useCustomSegment;
    }

    public void setUseCustomSegment( boolean useCustomSegment ) {
        this.useCustomSegment = useCustomSegment;
    }

    public String getCustomSegment() {
        return customSegment;
    }

    public void setCustomSegment( String customSegment ) {
        this.customSegment = customSegment;
    }

    public String getDimensions() {
        return dimensions;
    }

    public void setDimensions( String dimensions ) {
        this.dimensions = dimensions;
    }

    public String getMetrics() {
        return metrics;
    }

    public void setMetrics( String metrics ) {
        this.metrics = metrics;
    }

    public String getFilters() {
        return filters;
    }

    public void setFilters( String filters ) {
        this.filters = filters;
    }

    public String getSort() {
        return sort;
    }

    public void setSort( String sort ) {
        this.sort = sort;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate( String startDate ) {
        this.startDate = startDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate( String endDate ) {
        this.endDate = endDate;
    }

    public String getGaProfileTableId() {
        return gaProfileTableId;
    }

    public void setGaProfileTableId( String gaProfile ) {
        this.gaProfileTableId = gaProfile;
    }

    public String getGaProfileName() {
        return gaProfileName;
    }

    public void setGaProfileName( String gaProfileName ) {
        this.gaProfileName = gaProfileName;
    }

    public String getSamplingLevel() {
        return samplingLevel;
    }

    public void setSamplingLevel( String samplingLevel ) {
        this.samplingLevel = samplingLevel;
    }

    public String[] getFeedFieldType() {
        return feedFieldType;
    }

    public void setFeedFieldType( String[] feedFieldType ) {
        this.feedFieldType = feedFieldType;
    }

    public String[] getFeedField() {
        return feedField;
    }

    public void setFeedField( String[] feedField ) {
        this.feedField = feedField;
    }

    public String[] getOutputField() {
        return outputField;
    }

    public void setOutputField( String[] outputField ) {
        this.outputField = outputField;
    }

    public int[] getOutputType() {
        return outputType;
    }

    public void setOutputType( int[] outputType ) {
        this.outputType = outputType;
    }

    public int getFieldsCount() {
        int count = Math.min( getFeedField().length, getFeedFieldType().length );
        count = Math.min( count, getOutputField().length );
        count = Math.min( count, getOutputType().length );
        count = Math.min( count, getConversionMask().length );
        return count;
    }

    // set sensible defaults for a new transform
    @Override
    public void setDefault() {
        oauthServiceAccount = "service.account@developer.gserviceaccount.com";
        oauthKeyFile = "";
        useSegment = true;
        segmentId = "gaid::-1";
        segmentName = "All Visits";
        dimensions = "ga:browser";
        metrics = "ga:visits";
        startDate = new SimpleDateFormat( "yyyy-MM-dd" ).format( new Date() );
        endDate = new String( startDate );
        sort = "-ga:visits";
        gaAppName = DEFAULT_GA_APPLICATION_NAME;
        rowLimit = 0;
        samplingLevel = TYPE_SAMPLING_LEVEL_CODE[0];

        // default is to have no key lookup settings
        allocate( 0 );

    }

    // helper method to allocate the arrays
    public void allocate( int nrkeys ) {

        feedField = new String[ nrkeys ];
        outputField = new String[ nrkeys ];
        outputType = new int[ nrkeys ];
        feedFieldType = new String[ nrkeys ];
        conversionMask = new String[ nrkeys ];
    }

    @Override
    public void getFields(IRowMeta r, String origin, IRowMeta[] info, TransformMeta nextTransform,
                          IVariables variables, IHopMetadataProvider metadataProvider) {

        // clear the output
        r.clear();
        // append the outputFields to the output
        for ( int i = 0; i < outputField.length; i++ ) {
            IValueMeta v;
            try {
                v = ValueMetaFactory.createValueMeta( outputField[ i ], outputType[ i ] );
            } catch ( HopPluginException e ) {
                v = new ValueMetaString( outputField[ i ] );
            }
            // that would influence the output
            // v.setConversionMask(conversionMask[i]);
            v.setOrigin( origin );
            r.addValueMeta( v );
        }

    }

    @Override
    public Object clone() {

        // field by field copy is default
        GoogleAnalyticsMeta retval = (GoogleAnalyticsMeta) super.clone();

        // add proper deep copy for the collections
        int nrKeys = feedField.length;

        retval.allocate( nrKeys );

        for ( int i = 0; i < nrKeys; i++ ) {
            retval.feedField[ i ] = feedField[ i ];
            retval.outputField[ i ] = outputField[ i ];
            retval.outputType[ i ] = outputType[ i ];
            retval.feedFieldType[ i ] = feedFieldType[ i ];
            retval.conversionMask[ i ] = conversionMask[ i ];
        }

        return retval;
    }

    private boolean getBooleanAttributeFromNode(Node node, String tag ) {
        String sValue = XmlHandler.getTagValue( node, tag );
        return ( sValue != null && sValue.equalsIgnoreCase( "Y" ) );

    }

    @Override
    public String getXml() throws HopValueException {

        StringBuilder retval = new StringBuilder( 800 );
        retval.append( "    " ).append( XmlHandler.addTagValue( "oauthServiceAccount", oauthServiceAccount ) );
        retval.append( "    " ).append( XmlHandler.addTagValue( "appName", gaAppName ) );
        retval.append( "    " ).append( XmlHandler.addTagValue( "oauthKeyFile", oauthKeyFile ) );
        retval.append( "    " ).append( XmlHandler.addTagValue( "profileName", gaProfileName ) );
        retval.append( "    " ).append( XmlHandler.addTagValue( "profileTableId", gaProfileTableId ) );
        retval.append( "    " ).append( XmlHandler.addTagValue( "customTableId", gaCustomTableId ) );
        retval.append( "    " ).append( XmlHandler.addTagValue( "useCustomTableId", useCustomTableId ) );
        retval.append( "    " ).append( XmlHandler.addTagValue( "startDate", startDate ) );
        retval.append( "    " ).append( XmlHandler.addTagValue( "endDate", endDate ) );
        retval.append( "    " ).append( XmlHandler.addTagValue( "dimensions", dimensions ) );
        retval.append( "    " ).append( XmlHandler.addTagValue( "metrics", metrics ) );
        retval.append( "    " ).append( XmlHandler.addTagValue( "filters", filters ) );
        retval.append( "    " ).append( XmlHandler.addTagValue( "sort", sort ) );
        retval.append( "    " ).append( XmlHandler.addTagValue( "useSegment", useSegment ) );
        retval.append( "    " ).append( XmlHandler.addTagValue( "useCustomSegment", useCustomSegment ) );
        retval.append( "    " ).append( XmlHandler.addTagValue( "customSegment", customSegment ) );
        retval.append( "    " ).append( XmlHandler.addTagValue( "segmentId", segmentId ) );
        retval.append( "    " ).append( XmlHandler.addTagValue( "segmentName", segmentName ) );
        retval.append( "    " ).append( XmlHandler.addTagValue( "samplingLevel", samplingLevel ) );
        retval.append( "    " ).append( XmlHandler.addTagValue( "rowLimit", rowLimit ) );

        for ( int i = 0; i < feedField.length; i++ ) {
            retval.append( "      <feedField>" ).append( Const.CR );
            retval.append( "        " ).append( XmlHandler.addTagValue( "feedFieldType", feedFieldType[ i ] ) );
            retval.append( "        " ).append( XmlHandler.addTagValue( "feedField", feedField[ i ] ) );
            retval.append( "        " ).append( XmlHandler.addTagValue( "outField", outputField[ i ] ) );
            retval.append( "        " )
                    .append( XmlHandler.addTagValue( "type", ValueMetaFactory.getValueMetaName( outputType[ i ] ) ) );
            retval.append( "        " ).append( XmlHandler.addTagValue( "conversionMask", conversionMask[ i ] ) );
            retval.append( "      </feedField>" ).append( Const.CR );
        }
        return retval.toString();
    }

    @Override
    public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider) throws HopXmlException {

        try {
            // Check for legacy fields (user/pass/API key), present an error if found
            String user = XmlHandler.getTagValue( transformNode, "user" );
            String pass = XmlHandler.getTagValue( transformNode, "pass" );
            String apiKey = XmlHandler.getTagValue( transformNode, "apiKey" );

            oauthServiceAccount = XmlHandler.getTagValue( transformNode, "oauthServiceAccount" );
            oauthKeyFile = XmlHandler.getTagValue( transformNode, "oauthKeyFile" );

            // Are we loading a legacy transformation?
            if ( ( user != null || pass != null || apiKey != null )
                    && ( oauthServiceAccount == null && oauthKeyFile == null ) ) {
                logError( BaseMessages.getString( PKG, "GoogleAnalytics.Error.TransformationUpdateNeeded" ) );
            }
            gaAppName = XmlHandler.getTagValue( transformNode, "appName" );
            gaProfileName = XmlHandler.getTagValue( transformNode, "profileName" );
            gaProfileTableId = XmlHandler.getTagValue( transformNode, "profileTableId" );
            gaCustomTableId = XmlHandler.getTagValue( transformNode, "customTableId" );
            useCustomTableId = getBooleanAttributeFromNode( transformNode, "useCustomTableId" );
            startDate = XmlHandler.getTagValue( transformNode, "startDate" );
            endDate = XmlHandler.getTagValue( transformNode, "endDate" );
            dimensions = XmlHandler.getTagValue( transformNode, "dimensions" );
            metrics = XmlHandler.getTagValue( transformNode, "metrics" );
            filters = XmlHandler.getTagValue( transformNode, "filters" );
            sort = XmlHandler.getTagValue( transformNode, "sort" );
            useSegment =
                    XmlHandler.getTagValue( transformNode, "useSegment" ) == null ? true : getBooleanAttributeFromNode(
                            transformNode, "useSegment" ); // assume true for non-present
            useCustomSegment = getBooleanAttributeFromNode( transformNode, "useCustomSegment" );
            customSegment = XmlHandler.getTagValue( transformNode, "customSegment" );
            segmentId = XmlHandler.getTagValue( transformNode, "segmentId" );
            segmentName = XmlHandler.getTagValue( transformNode, "segmentName" );
            samplingLevel = XmlHandler.getTagValue( transformNode, "samplingLevel" );
            rowLimit = Const.toInt( XmlHandler.getTagValue( transformNode, "rowLimit" ), 0 );

            allocate( 0 );

            int nrFields = XmlHandler.countNodes( transformNode, "feedField" );
            allocate( nrFields );

            for ( int i = 0; i < nrFields; i++ ) {
                Node knode = XmlHandler.getSubNodeByNr( transformNode, "feedField", i );

                feedFieldType[ i ] = XmlHandler.getTagValue( knode, "feedFieldType" );
                feedField[ i ] = XmlHandler.getTagValue( knode, "feedField" );
                outputField[ i ] = XmlHandler.getTagValue( knode, "outField" );
                outputType[ i ] = ValueMetaFactory.getIdForValueMeta( XmlHandler.getTagValue( knode, "type" ) );
                conversionMask[ i ] = XmlHandler.getTagValue( knode, "conversionMask" );

                if ( outputType[ i ] < 0 ) {
                    outputType[ i ] = IValueMeta.TYPE_STRING;
                }

            }

        } catch ( Exception e ) {
            throw new HopXmlException( BaseMessages.getString( PKG, "GoogleAnalytics.Error.UnableToReadFromXML" ), e );
        }

    }

    @Override
    public void check(List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                      IRowMeta prev, String[] input, String[] output, IRowMeta info,
                      IVariables variables, IHopMetadataProvider metadataProvider) {
        CheckResult cr;

        if ( prev == null || prev.size() == 0 ) {
            cr =
                    new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString(
                            PKG, "GoogleAnalytics.CheckResult.NotReceivingFields" ), transformMeta );
            remarks.add( cr );
        } else {
            cr =
                    new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
                            PKG, "GoogleAnalytics.CheckResult.TransformRecevingData", prev.size() + "" ), transformMeta );
            remarks.add( cr );
        }

        // See if we have input streams leading to this transform!
        if ( input.length > 0 ) {
            cr =
                    new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
                            PKG, "GoogleAnalytics.CheckResult.TransformRecevingData2" ), transformMeta );
            remarks.add( cr );
        } else {
            cr =
                    new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString(
                            PKG, "GoogleAnalytics.CheckResult.NoInputReceivedFromOtherTransforms" ), transformMeta );
            remarks.add( cr );
        }

    }

    @Override
    public GoogleAnalytics createTransform(TransformMeta transformMeta, GoogleAnalyticsData data, int cnr,
                                           PipelineMeta pipelineMeta, Pipeline pipeline) {
        return new GoogleAnalytics( transformMeta, this, data, cnr, pipelineMeta, pipeline );
    }

    @Override
    public GoogleAnalyticsData getTransformData() {
        return new GoogleAnalyticsData();
    }

    public String getOAuthKeyFile() {
        return oauthKeyFile;
    }

    public void setOAuthKeyFile( String oauthKeyFile ) {
        this.oauthKeyFile = oauthKeyFile;
    }

    public String getOAuthServiceAccount() {
        return oauthServiceAccount;
    }


    public void setOAuthServiceAccount( String oauthServiceAccount ) {
        this.oauthServiceAccount = oauthServiceAccount;
    }

    /**
     * If we use injection we can have different arrays lengths.
     * We need synchronize them for consistency behavior with UI
     */
    @AfterInjection
    public void afterInjectionSynchronization() {
        int nrFields = ( feedField == null ) ? -1 : feedField.length;
        if ( nrFields <= 0 ) {
            return;
        }
        String[][] rtnStringArray = Utils.normalizeArrays( nrFields, feedFieldType, outputField, conversionMask );
        feedFieldType = rtnStringArray[ 0 ];
        outputField = rtnStringArray[ 1 ];
        conversionMask = rtnStringArray[ 2 ];

        int[][] rtnIntArray = Utils.normalizeArrays( nrFields, outputType );
        outputType = rtnIntArray[ 0 ];

    }
}
