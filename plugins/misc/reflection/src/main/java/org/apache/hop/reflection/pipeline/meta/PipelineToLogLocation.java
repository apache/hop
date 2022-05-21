package org.apache.hop.reflection.pipeline.meta;

import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.Objects;

public class PipelineToLogLocation {

    @HopMetadataProperty private String pipelineToLogFilename;

    public PipelineToLogLocation(){

    }

    public PipelineToLogLocation(String pipelineTologFilename){
        this.pipelineToLogFilename = pipelineTologFilename;
    }

    @Override
    public boolean equals(Object o){
        if(this == o){
            return true;
        }

        if(o == null || getClass() != o.getClass()){
            return false;
        }

        PipelineToLogLocation that = (PipelineToLogLocation) o;
        return Objects.equals(pipelineToLogFilename, that.pipelineToLogFilename);
    }

    @Override
    public int hashCode(){
        return Objects.hash(pipelineToLogFilename);
    }

    public String getPipelineToLogFilename(){
        return pipelineToLogFilename;
    }

    public void setPipelineToLogFilename(String pipelineToLogFilename){
        this.pipelineToLogFilename = pipelineToLogFilename;
    }
}
