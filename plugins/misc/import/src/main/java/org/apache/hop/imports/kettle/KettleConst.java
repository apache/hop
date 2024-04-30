/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.imports.kettle;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KettleConst {

  public static final Map<String, String> kettleElementReplacements =
      Stream.of(
              new Object[][] {
                // transformations
                {"transformation", "pipeline"},
                {"trans_type", "pipeline_type"},
                {"trans_version", "pipeline_version"},
                {"trans_status", "pipeline_status"},
                {"step", "transform"},
                {"step_error_handling", "transform_error_handling"},
                {"capture_step_performance", "capture_transform_performance"},
                {
                  "step_performance_capturing_size_limit",
                  "transform_performance_capturing_size_limit"
                },
                {"target_step_name", "target_transform_name"},
                {"step_performance_capturing_delay", "transform_performance_capturing_delay"},
                {"transformationPath", "pipelinePath"},
                {"SUB_STEP", "subTransform"},
                {"variablemapping", "variable_mapping"},
                // jobs
                {"job", "workflow"},
                {"job_version", "workflow_version"},
                {"entries", "actions"},
                {"entry", "action"},
                {"source_step", "source_transform"},
                {"target_step", "target_transform"},
                {"step1", "transform1"},
                {"step2", "transform2"},
                {"accept_stepname", "accept_transform_name"},
                {"steps", "transforms"},
                {"default_target_step", "default_target_transform"},
                {"input_step", "input_transform"},
                {"output_step", "output_transform"},
                // UDJC
                {"info_steps", "info_transforms"},
                {"info_step", "info_transform"},
                {"target_steps", "target_transforms"},
                {"step_tag", "transform_tag"},
                {"step_name", "transform_name"},
                {"step_description", "transform_description"},
                // ExcelWriter
                {"extention", "extension"},
                // Formula
                {"formula_string", "formula"},
                // XML Join
                {"targetXMLstep", "targetXMLTransform"},
                {"sourceXMLstep", "sourceXMLTransform"},
                // Multiway Merge Join, 0-9 for simplicity
                {"step0", "transform0"},
                // step1 and step2 are already covered earlier in the mapping. left in comments here
                // for clarity.
                // {"step1", "transform1"},
                // {"step2", "transform2"},
                {"step3", "transform3"},
                {"step4", "transform4"},
                {"step5", "transform5"},
                {"step6", "transform6"},
                {"step7", "transform7"},
                {"step8", "transform8"},
                {"step9", "transform9"},
              })
          .collect(Collectors.toMap(data -> (String) data[0], data -> (String) data[1]));

  public static final Map<String, String> kettleElementsToRemove =
      Stream.of(
              new Object[][] {
                {"size_rowset", ""},
                {"sleep_time_empty", ""},
                {"sleep_time_full", ""},
                {"unique_connections", ""},
                {"feedback_shown", ""},
                {"feedback_size", ""},
                {"using_thread_priorities", ""},
                {"shared_objects_file", ""},
                {"dependencies", ""},
                {"partitionschemas", ""},
                {"slaveservers", ""},
                {"remotesteps", ""},
                {"clusterschemas", ""},
                {"maxdate", ""},
                {"log", ""},
                {"connection", "workflow,pipeline"},
                {"slave-step-copy-partition-distribution", ""},
                {"slave_transformation", ""},
                {"trans_object_id", ""},
                {"job_object_id", ""},
                {"specification_method", ""},
                {"job-log-table", ""},
                {"jobentry-log-table", ""},
                {"channel-log-table", ""},
                {"checkpoint-log-table", ""}
              })
          .collect(Collectors.toMap(data -> (String) data[0], data -> (String) data[1]));

  public static final Map<String, String> kettleStartEntryElementsToRemove =
      Stream.of(
              new Object[][] {
                {"start", ""},
                {"draw", ""},
                {"nr", ""},
                {"dummy", ""}
              })
          .collect(Collectors.toMap(data -> (String) data[0], data -> (String) data[1]));

  public static final Map<String, String> kettleDummyEntryElementsToRemove =
      Stream.of(
              new Object[][] {
                {"start", ""},
                {"dummy", ""},
                {"repeat", ""},
                {"schedulerType", ""},
                {"intervalSeconds", ""},
                {"intervalMinutes", ""},
                {"hour", ""},
                {"minutes", ""},
                {"weekDay", ""},
                {"DayOfMonth", ""},
                {"draw", ""},
                {"nr", ""}
              })
          .collect(Collectors.toMap(data -> (String) data[0], data -> (String) data[1]));

  public static final Map<String, String> kettleReplaceContent =
      Stream.of(
              new Object[][] {
                {"JOB", "WORKFLOW"},
                {"TRANS", "PIPELINE"},
                {"PARENT_JOB", "PARENT_WORKFLOW"},
                {"GP_JOB", "GP_WORKFLOW"},
                {"ROOT_JOB", "ROOT_WORKFLOW"},
                {"BlockingStep", "BlockingTransform"},
                {"BlockUntilStepsFinish", "BlockUntilTransformsFinish"},
                {"TypeExitExcelWriterStep", "TypeExitExcelWriterTransform"},
                {"StepMetastructure", "TransformMetaStructure"},
                {"JobExecutor", "WorkflowExecutor"},
                {"execution_result_target_step", "execution_result_target_transform"},
                {"TransExecutor", "PipelineExecutor"},
                {"Mapping", "SimpleMapping"},
                // Text File Input deprecated
                {"TextFileInput", "TextFileInput2"},
                {"KettleKafkaConsumerInput", "KafkaConsumer"},
                {"PentahoGoogleSheetsPluginOutputMeta", "GoogleSheetsOutput"},
                {"PentahoGoogleSheetsPluginInputMeta", "GoogleSheetsInput"}
              })
          .collect(Collectors.toMap(data -> (String) data[0], data -> (String) data[1]));

  public static final Map<String, String> kettleReplaceInContent =
      Stream.of(
              new Object[][] {
                {".kjb", ".hwf"},
                {".ktr", ".hpl"},
                {"Internal.Job", "Internal.Workflow"},
                {"Internal.Transformation", "Internal.Pipeline"},
                {"Filename.Directory", "Filename.Folder"},
                {"Repository.Directory", "Repository.Folder"},
                {"Current.Directory", "Current.Folder"},
                {"Internal.Job.Filename.Name", "Internal.Workflow.Filename.Name"},
                {"Internal.Entry.Current.Directory", "Internal.Entry.Current.Folder"},
                {"Internal.Job.Filename.Directory", "Internal.Workflow.Filename.Folder"},
                // Injection key
                {"HEAD_STEP", "HEAD_TRANSFORM"}, // append
                {"TAIL_STEP", "TAIL_TRANSFORM"}, // append
                {"LEFT_STEP", "LEFT_TRANSFORM"}, // mergejoin
                {"RIGHT_STEP", "RIGHT_TRANSFORM"}, // mergejoin
                {"INPUT_STEPS", "INPUT_TRANSFORMS"}, // multimerge
                {"SOURCE_STEP_NAME", "SOURCE_TRANSFORM_NAME"}, // metainject
                {"STREAMING_SOURCE_STEP", "STREAMING_SOURCE_TRANSFORM"}, // metainject
                {"STREAMING_TARGET_STEP", "STREAMING_TARGET_TRANSFORM"}, // metainject
                {"SEND_TRUE_STEP", "SEND_TRUE_TRANSFORM"}, // filterrows
                {"SEND_FALSE_STEP", "SEND_FALSE_TRANSFORM"}, // filterrows
                {"MAIN_STEP", "MAIN_TRANSFORM"}, // joinrows
                {"SUB_STEP", "SUB_TRANSFORM"}, // kafka
                {"INC_STEPNR_IN_FILENAME", "INC_TRANSFORMNR_IN_FILENAME"}, // jsonoutput & xmloutput
                {"SOURCE_XML_STEP", "SOURCE_XML_TRANSFORM"}, // xmljoin
                {"TARGET_XML_STEP", "TARGET_XML_TRANSFORM"}, // xmljoin
                {"OUTPUT_INCLUDE_STEPNR", "OUTPUT_INCLUDE_TRANSFORMNR"}, // webservice
                {
                  "SWITCH_CASE_TARGET.CASE_TARGET_STEP_NAME",
                  "SWITCH_CASE_TARGET.CASE_TARGET_TRANSFORM_NAME"
                }, // switchcase
                {"DEFAULT_TARGET_STEP_NAME", "DEFAULT_TARGET_TRANSFORM_NAME"}, // switchcase
                {"CURRENT_JOB", "CURRENT_WORKFLOW"}, // set variable
                {"PARENT_JOB", "PARENT_WORKFLOW"}, // set variable
                {"ROOT_JOB", "ROOT_WORKFLOW"} // set variable
              })
          .collect(Collectors.toMap(data -> (String) data[0], data -> (String) data[1]));

  public static final HashMap<String, String> replacements = new HashMap<>();

  public static final List<String> repositoryTypes = Arrays.asList("JOB", "TRANS");

  public static final List<String> jobTypes = Arrays.asList("JOB");

  public static final List<String> transTypes = Arrays.asList("TRANS");

  public KettleConst() {}
}
