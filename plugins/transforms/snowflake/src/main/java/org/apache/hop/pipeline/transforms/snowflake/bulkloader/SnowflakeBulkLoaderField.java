package org.apache.hop.pipeline.transforms.snowflake.bulkloader;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * Describes a single field mapping from the Pentaho stream to the Snowflake table
 */
public class SnowflakeBulkLoaderField implements Cloneable{

    /** The field name on the stream */
    @HopMetadataProperty(
            key = "stream_field",
            injectionGroupKey = "OUTPUT_FIELDS" )
    private String streamField;

    /** The field name on the table */
    @HopMetadataProperty(
            key = "table_field",
            injectionGroupKey = "OUTPUT_FIELDS" )
    private String tableField;

    /**
     *
     * @param streamField The name of the stream field
     * @param tableField The name of the field on the table
     */
    public SnowflakeBulkLoaderField( String streamField, String tableField ) {
        this.streamField = streamField;
        this.tableField = tableField;
    }

    public SnowflakeBulkLoaderField() {
    }

    /**
     * Enables deep cloning
     * @return A new instance of SnowflakeBulkLoaderField
     */
    public Object clone() {
        try {
            return super.clone();
        } catch ( CloneNotSupportedException e ) {
            return null;
        }
    }

    /**
     * Validate that the SnowflakeBulkLoaderField is good
     * @return
     * @throws HopException
     */
    public boolean validate() throws HopException {
        if ( streamField == null || tableField == null ) {
            throw new HopException( "Validation error: Both stream field and database field must be populated." );
        }

        return true;
    }

    /**
     *
     * @return The name of the stream field
     */
    public String getStreamField() {
        return streamField;
    }

    /**
     * Set the stream field
     * @param streamField The name of the field on the Pentaho stream
     */
    public void setStreamField( String streamField ) {
        this.streamField = streamField;
    }

    /**
     *
     * @return The name of the field in the Snowflake table
     */
    public String getTableField() {
        return tableField;
    }

    /**
     * Set the field in the Snowflake table
     * @param tableField The name of the field on the table
     */
    public void setTableField( String tableField ) {
        this.tableField = tableField;
    }

    /**
     *
     * @return A string in the "streamField -> tableField" format
     */
    public String toString() {
        return streamField + " -> " + tableField;
    }

}
