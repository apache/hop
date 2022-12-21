package org.apache.hop.pipeline.transforms.vertica.bulkloader;

import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.Objects;

public class VerticaBulkLoaderField {

    public VerticaBulkLoaderField(){

    }

    public VerticaBulkLoaderField(String fieldDatabase, String fieldStream){
        this.fieldDatabase = fieldDatabase;
        this.fieldStream = fieldStream;
    }

    @HopMetadataProperty(
      key = "stream_name",
      injectionKey = "STREAM_FIELDNAME",
      injectionKeyDescription = "VerticaBulkLoader.Inject.FIELDSTREAM"
    )
    private String fieldStream;

    @HopMetadataProperty(
            key = "column_name",
            injectionKey = "DATABASE_FIELDNAME",
            injectionKeyDescription = "VerticaBulkLoader.Inject.FIELDDATABASE"
    )
    private String fieldDatabase;

    public String getFieldStream(){
        return fieldStream;
    }

    public void setFieldStream(String fieldStream){
        this.fieldStream = fieldStream;
    }

    public String getFieldDatabase(){
        return fieldDatabase;
    }

    public void setFieldDatabase(String fieldDatabase){
        this.fieldDatabase = fieldDatabase;
    }

    @Override
    public boolean equals(Object o){
        if(this == o) return true;
        if(o == null || getClass() != o.getClass()) return false;
        VerticaBulkLoaderField that = (VerticaBulkLoaderField) o;
        return fieldStream.equals(that.fieldStream) && fieldDatabase.equals(that.fieldDatabase);
    }

    @Override
    public int hashCode(){
        return Objects.hash(fieldStream, fieldDatabase);
    }

}
