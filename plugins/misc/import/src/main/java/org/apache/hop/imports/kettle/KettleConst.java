package org.apache.hop.imports.kettle;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KettleConst {

    public static final Map<String, String> kettleElementReplacements = Stream.of(new Object[][]{
            // transformations
            {"transformation", "pipeline"},
            {"trans_type", "pipeline_type"},
            {"trans_version", "pipeline_version"},
            {"trans_status", "pipeline_status"},
            {"step", "transform"},
            {"step_error_handling", "transform_error_handling"},
            {"capture_step_performance","capture_transform_performance"},
            {"step_performance_capturing_size_limit", "transform_performance_capturing_size_limit"},
            {"step_performance_capturing_delay", "transform_performance_capturing_delay"},
            // jobs
            {"job", "workflow"},
            {"job_version", "workflow_version"},
            {"entries","actions"},
            {"entry","action"},
            {"source_step", "source_transform"},
            {"target_step", "target_transform"}
    }).collect(Collectors.toMap(data -> (String)data[0], data -> (String)data[1]));

    public static final Map<String, String> kettleElementsToRemove = Stream.of(new Object[][]{
            {"size_rowset", ""},
            {"sleep_time_empty", ""},
            {"sleep_time_full",""},
            {"unique_connections",""},
            {"feedback_shown",""},
            {"feedback_size",""},
            {"using_thread_priorities",""},
            {"shared_objects_file",""},
            {"dependencies",""},
            {"partitionschemas",""},
            {"slaveservers",""},
            {"remotesteps",""},
            {"clusterschemas",""},
            {"maxdate",""},
            {"log", ""},
            {"connection", "pipeline"},
            {"slave-step-copy-partition-distribution", ""},
            {"slave_transformation", ""},
            {"trans_object_id", ""},
            {"job_object_id", ""},
            {"specification_method", ""},
            {"job-log-table", ""},
            {"jobentry-log-table", ""},
            {"channel-log-table", ""},
            {"checkpoint-log-table", ""}
    }).collect(Collectors.toMap(data -> (String)data[0], data -> (String)data[1]));

    public static final Map<String, String> kettleReplaceContent = Stream.of(new Object[][]{
            {"JOB", "WORKFLOW"},
            {"TRANS", "PIPELINE"},
            {"BlockUntilStepsFinish", "BlockUntilTransformsFinish"},
            {"TypeExitExcelWriterStep", "TypeExitExcelWriterTransform"}
    }).collect(Collectors.toMap(data -> (String)data[0], data -> (String)data[1]));

    public static final Map<String, String> kettleReplaceInContent = Stream.of(new Object[][]{
            {".kjb", ".hwf"},
            {".ktr", ".hpl"},
            {"Internal.Job", "Internal.Workflow"},
            {"Internal.Transformation", "Internal.Pipeline"},
            {"Filename.Directory", "Filename.Folder"},
            {"Repository.Directory", "Repository.Folder"},
            {"Current.Directory", "Current.Folder"}
    }).collect(Collectors.toMap(data -> (String)data[0], data -> (String)data[1]));

    public static final HashMap<String, String> replacements = new HashMap<String, String>();

    public static final List<String> repositoryTypes = Arrays.asList(new String[]{"JOB", "TRANS"});

    public static final List<String> jobTypes = Arrays.asList(new String[]{"JOB"});

    public static final List<String> transTypes = Arrays.asList(new String[]{"TRANS"});

    public KettleConst(){
    }
}
