package org.apache.hop.pipeline.transforms.googleanalytics;

import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.metadata.api.HopMetadataProperty;

public class GoogleAnalyticsField {

    @HopMetadataProperty(key = "feed_field_type")
    private String feedFieldType;

    @HopMetadataProperty(key = "feed_field")
    private String feedField;

    @HopMetadataProperty(key = "output_field")
    private String outputFieldName;

    @HopMetadataProperty(key = "type")
    private String type;

    @HopMetadataProperty(key = "input_format")
    private String inputFormat;

    public GoogleAnalyticsField(){}

    public GoogleAnalyticsField(GoogleAnalyticsField f){
        this.feedField = f.feedField;
        this.type = f.type;
        this.feedFieldType = f.feedFieldType;
        this.outputFieldName = f.outputFieldName;
        this.inputFormat = f.inputFormat;
    }

    public int getHopType(){
        return ValueMetaFactory.getIdForValueMeta(type);
    }

    public IValueMeta createValueMeta() throws HopPluginException{
        IValueMeta v = ValueMetaFactory.createValueMeta(outputFieldName, getHopType());
        return v;
    }

    public String getFeedFieldType() {
        return feedFieldType;
    }

    public void setFeedFieldType(String feedFieldType) {
        this.feedFieldType = feedFieldType;
    }

    public String getFeedField() {
        return feedField;
    }

    public void setFeedField(String feedField) {
        this.feedField = feedField;
    }

    public String getOutputFieldName() {
        return outputFieldName;
    }

    public void setOutputFieldName(String outputFieldName) {
        this.outputFieldName = outputFieldName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public void setInputFormat(String inputFormat) {
        this.inputFormat = inputFormat;
    }
}
