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

import com.google.api.client.auth.oauth2.TokenResponseException;
import com.google.api.services.analytics.Analytics;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.io.IOException;
import java.util.List;

public class GoogleAnalytics extends BaseTransform<GoogleAnalyticsMeta, GoogleAnalyticsData> implements ITransform<GoogleAnalyticsMeta, GoogleAnalyticsData> {

    private static Class<?> PKG = GoogleAnalyticsMeta.class; // for i18n purposes

    private Analytics analytics;
    private String accountName;

    public GoogleAnalytics(TransformMeta transformMeta, GoogleAnalyticsMeta meta, GoogleAnalyticsData data, int c, PipelineMeta pipelineMeta, Pipeline pipeline ) {
        super(transformMeta, meta, data, c, pipelineMeta, pipeline);
    }

    @Override
    public boolean processRow() throws HopException {

        if ( first ) {

            first = false;

            data.outputRowMeta = new RowMeta();
            meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

            // stores the indices where to look for the key fields in the input rows
            data.conversionMeta = new IValueMeta[ meta.getFieldsCount() ];

            for ( int i = 0; i < meta.getFieldsCount(); i++ ) {

                // get output and from-string conversion format for each field
                IValueMeta returnMeta = data.outputRowMeta.getValueMeta( i );

                IValueMeta conversionMeta;

                conversionMeta = ValueMetaFactory.cloneValueMeta( returnMeta, IValueMeta.TYPE_STRING );
                conversionMeta.setConversionMask( meta.getConversionMask()[ i ] );
                conversionMeta.setDecimalSymbol( "." ); // google analytics is en-US
                conversionMeta.setGroupingSymbol( null ); // google analytics uses no grouping symbol

                data.conversionMeta[ i ] = conversionMeta;
            }
        }

        // generate output row, make it correct size
        Object[] outputRow = RowDataUtil.allocateRowData( data.outputRowMeta.size() );

        List<String> entry = getNextDataEntry();

        if ( entry != null && ( meta.getRowLimit() <= 0 || getLinesWritten() < meta.getRowLimit() ) ) { // another record to
            // fill the output fields with look up data
            for ( int i = 0, j = 0; i < meta.getFieldsCount(); i++ ) {
                String fieldName = resolve( meta.getFeedField()[ i ] );
                Object dataObject;
                String type = resolve( meta.getFeedFieldType()[ i ] );

                // We handle fields differently depending on whether its a Dimension/Metric, Data Source Property, or
                // Data Source Field. Also the API doesn't exactly match the concepts anymore (see individual comments below),
                // so there is quite a bit of special processing.
                if ( GoogleAnalyticsMeta.FIELD_TYPE_DATA_SOURCE_PROPERTY.equals( type ) ) {
                    // Account name has to be handled differently, it's in the Accounts API not Profiles API
                    if ( GoogleAnalyticsMeta.PROPERTY_DATA_SOURCE_ACCOUNT_NAME.equals( fieldName ) ) {
                        // We expect a single account name, and already fetched it during init
                        dataObject = accountName;
                    } else {
                        dataObject = data.feed.getProfileInfo().get( removeClassifier( fieldName ) );
                    }
                } else if ( GoogleAnalyticsMeta.FIELD_TYPE_DATA_SOURCE_FIELD.equals( type ) ) {
                    // Get tableId or tableName
                    if ( GoogleAnalyticsMeta.FIELD_DATA_SOURCE_TABLE_ID.equals( fieldName ) ) {
                        dataObject = data.feed.getProfileInfo().get( removeClassifier( fieldName ) );
                    } else {
                        // We only have two Data Source Fields and they're hard-coded, so we handle tableName in this else-clause
                        // since tableId was done in the if-clause. We have to handle the two differently because tableName is
                        // actually the profile name in this version (v3) of the Google Analytics API.
                        dataObject = data.feed.getProfileInfo().getProfileName();
                    }
                } else if ( GoogleAnalyticsMeta.DEPRECATED_FIELD_TYPE_CONFIDENCE_INTERVAL.equals( type ) ) {
                    dataObject = null;
                    if ( log.isRowLevel() ) {
                        logRowlevel( BaseMessages.getString( PKG, "GoogleAnalytics.Warn.FieldTypeNotSupported",
                                GoogleAnalyticsMeta.DEPRECATED_FIELD_TYPE_CONFIDENCE_INTERVAL ) );
                    }
                } else {
                    // Assume it's a Dimension or Metric, we've covered the rest of the cases above.
                    dataObject = entry.get( j++ );
                }
                outputRow[ i ] = data.outputRowMeta.getValueMeta( i ).convertData( data.conversionMeta[ i ], dataObject );
            }

            // copy row to possible alternate rowset(s)
            putRow( data.outputRowMeta, outputRow );

            // Some basic logging
            if ( checkFeedback( getLinesWritten() ) ) {
                if ( log.isBasic() ) {
                    logBasic( "Linenr " + getLinesWritten() );
                }
            }
            return true;

        } else {
            setOutputDone();
            return false;
        }
    }

    protected Analytics.Data.Ga.Get getQuery(Analytics analytics ) {

        Analytics.Data dataApi = analytics.data();
        Analytics.Data.Ga.Get query;

        try {
            String metrics = resolve( meta.getMetrics() );
            if ( Utils.isEmpty( metrics ) ) {
                logError( BaseMessages.getString( PKG, "GoogleAnalytics.Error.NoMetricsSpecified.Message" ) );
                return null;
            }
            query = dataApi.ga().get(
                    meta.isUseCustomTableId() ? resolve( meta.getGaCustomTableId() ) : meta.getGaProfileTableId(),
                    //ids
                    resolve( meta.getStartDate() ), // start date
                    resolve( meta.getEndDate() ), // end date
                    metrics  // metrics
            );

            String dimensions = resolve( meta.getDimensions() );
            if ( !Utils.isEmpty( dimensions ) ) {
                query.setDimensions( dimensions );
            }

            if ( meta.isUseSegment() ) {
                if ( meta.isUseCustomSegment() ) {
                    query.setSegment( resolve( meta.getCustomSegment() ) );
                } else {
                    query.setSegment( meta.getSegmentId() );
                }
            }

            if ( !Utils.isEmpty( meta.getSamplingLevel() ) ) {
                query.setSamplingLevel( resolve( meta.getSamplingLevel() ) );
            }

            if ( !Utils.isEmpty( meta.getFilters() ) && !Utils.isEmpty( resolve( meta.getFilters() ) ) ) {
                query.setFilters( resolve( meta.getFilters() ) );
            }
            if ( !Utils.isEmpty( meta.getSort() ) ) {
                query.setSort( resolve( meta.getSort() ) );
            }

            return query;
        } catch ( IOException ioe ) {
            return null;
        }

    }

    @Override
    public boolean init() {

        if ( !super.init() ) {
            return false;
        }

        // Look for deprecated field types and log error(s) for them
        String[] types = resolve( meta.getFeedFieldType() );
        if ( types != null ) {
            for ( String type : types ) {
                if ( GoogleAnalyticsMeta.DEPRECATED_FIELD_TYPE_CONFIDENCE_INTERVAL.equals( type ) ) {
                    logError( BaseMessages.getString( PKG, "GoogleAnalytics.Warn.FieldTypeNotSupported",
                            GoogleAnalyticsMeta.DEPRECATED_FIELD_TYPE_CONFIDENCE_INTERVAL ) );
                }
            }
        }

        String appName = resolve( meta.getGaAppName() );
        String serviceAccount = resolve( meta.getOAuthServiceAccount() );
        String OAuthKeyFile = resolve( meta.getOAuthKeyFile() );

        if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "GoogleAnalyticsDialog.AppName.Label" ) + ": " + appName );
            logDetailed( BaseMessages.getString( PKG, "GoogleAnalyticsDialog.OauthAccount.Label" ) + ": " + serviceAccount );
            logDetailed( BaseMessages.getString( PKG, "GoogleAnalyticsDialog.KeyFile.Label" ) + ": " + OAuthKeyFile );
        }

        try {
            // Create an Analytics object, and fetch what we can for later (account name, e.g.)
            analytics = GoogleAnalyticsApiFacade.createFor( appName, serviceAccount, OAuthKeyFile ).getAnalytics();
            // There is necessarily an account name associated with this, so any NPEs or other exceptions mean bail out
            accountName = analytics.management().accounts().list().execute().getItems().iterator().next().getName();
        } catch ( TokenResponseException tre ) {
            Exception exceptionToLog = tre;
            if ( tre.getDetails() != null && tre.getDetails().getError() != null ) {
                exceptionToLog = new IOException( BaseMessages.getString( PKG, "GoogleAnalytics.Error.OAuth2.Auth",
                        tre.getDetails().getError() ), tre );
            }
            logError( BaseMessages.getString( PKG, "GoogleAnalytics.Error.AccessingGaApi" ), exceptionToLog );
            return false;
        } catch ( Exception e ) {
            logError( BaseMessages.getString( PKG, "GoogleAnalytics.Error.AccessingGaApi" ), e );
            return false;
        }
        return true;
    }

    // made not private for testing purposes
    List<String> getNextDataEntry() throws HopException {
        // no query prepared yet?
        if ( data.query == null ) {

            data.query = getQuery( analytics );
            // use default max results for now
            //data.query.setMaxResults( 10000 );

            if ( log.isDetailed() ) {
                logDetailed( "querying google analytics: " + data.query.buildHttpRequestUrl().toURI().toString() );
            }

            try {
                data.feed = data.query.execute();
                data.entryIndex = 0;

            } catch ( IOException e2 ) {
                throw new HopException( e2 );
            }

        } else if ( data.feed != null
                // getItemsPerPage():
                //    Its value ranges from 1 to 10,000 with a value of 1000 by default, or otherwise
                //    specified by the max-results query parameter
                && data.entryIndex + 1 >= data.feed.getItemsPerPage() ) {
            try {
                // query is there, check whether we hit the last entry and re-query as necessary
                int startIndex = ( data.query.getStartIndex() == null ) ? 1 : data.query.getStartIndex();
                int totalResults = ( data.feed.getTotalResults() == null ) ? 0 : data.feed.getTotalResults();

                int newStartIndex = startIndex + data.entryIndex;
                if ( newStartIndex <= totalResults ) {
                    // need to query for next page
                    data.query.setStartIndex( newStartIndex );
                    data.feed = data.query.execute();
                    data.entryIndex = 0;
                }
            } catch ( IOException e2 ) {
                throw new HopException( e2 );
            }
        }

        if ( data.feed != null ) {
            List<List<String>> entries = data.feed.getRows();
            if ( entries != null && data.entryIndex < entries.size() ) {
                return entries.get( data.entryIndex++ );
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    @Override
    public void dispose() {
        super.dispose();
    }

    private String removeClassifier( String original ) {
        int colonIndex = original.indexOf( ":" );
        return original.substring( colonIndex + 1 );
    }

}
