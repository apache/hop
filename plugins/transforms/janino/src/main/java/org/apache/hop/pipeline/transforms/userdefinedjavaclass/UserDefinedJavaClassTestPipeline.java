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

package org.apache.hop.pipeline.transforms.userdefinedjavaclass;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.rowgenerator.GeneratorField;
import org.apache.hop.pipeline.transforms.rowgenerator.RowGeneratorMeta;

/**
 * Builds the throwaway pipeline behind the "Test class" button of the User Defined Java Class
 * dialog: generated test data for the main input, and stand-ins for the info and target transforms
 * the class reads from and writes to by name.
 */
public final class UserDefinedJavaClassTestPipeline {
  public static final String TEST_DATA_TRANSFORM_NAME = "## TEST DATA ##";

  private UserDefinedJavaClassTestPipeline() {}

  /**
   * @param variables the variables to resolve the source pipeline's fields with
   * @param sourcePipelineMeta the pipeline the transform lives in, used to derive field layouts
   * @param scriptTransformName the name of the transform under test
   * @param udjcMeta the metadata of the transform under test, as currently edited
   * @param mainGenMeta the test data generator for the main input
   * @return the test pipeline
   */
  public static PipelineMeta build(
      IVariables variables,
      PipelineMeta sourcePipelineMeta,
      String scriptTransformName,
      UserDefinedJavaClassMeta udjcMeta,
      RowGeneratorMeta mainGenMeta)
      throws HopTransformException {
    PluginRegistry registry = PluginRegistry.getInstance();

    TransformMeta genTransform =
        new TransformMeta(
            registry.getPluginId(TransformPluginType.class, mainGenMeta),
            TEST_DATA_TRANSFORM_NAME,
            mainGenMeta);
    genTransform.setLocation(50, 50);

    TransformMeta scriptTransform =
        new TransformMeta(
            registry.getPluginId(TransformPluginType.class, udjcMeta),
            scriptTransformName,
            udjcMeta);
    scriptTransform.setLocation(250, 50);

    PipelineMeta testPipelineMeta = new PipelineMeta();
    testPipelineMeta.addTransform(genTransform);
    testPipelineMeta.addTransform(scriptTransform);
    testPipelineMeta.addPipelineHop(new PipelineHopMeta(genTransform, scriptTransform));

    // The class can read from info transforms and write to target transforms by name.
    // Those don't exist in the test pipeline, so stand in for them: generated test data
    // for every info transform, a dummy for every target transform.
    //
    int y = 150;
    for (InfoTransformDefinition infoDefinition : udjcMeta.getInfoTransformDefinitions()) {
      String infoTransformName = infoDefinition.getTransformName();
      if (Utils.isEmpty(infoTransformName)
          || testPipelineMeta.findTransform(infoTransformName) != null) {
        continue;
      }
      IRowMeta infoRowMeta = sourcePipelineMeta.getTransformFields(variables, infoTransformName);
      RowGeneratorMeta infoGenMeta =
          createTestDataGenerator(infoRowMeta == null ? new RowMeta() : infoRowMeta.clone());
      TransformMeta infoTransform =
          new TransformMeta(
              registry.getPluginId(TransformPluginType.class, infoGenMeta),
              infoTransformName,
              infoGenMeta);
      infoTransform.setLocation(50, y);
      y += 100;
      testPipelineMeta.addTransform(infoTransform);
      testPipelineMeta.addPipelineHop(new PipelineHopMeta(infoTransform, scriptTransform));
    }
    y = 150;
    for (TargetTransformDefinition targetDefinition : udjcMeta.getTargetTransformDefinitions()) {
      String targetTransformName = targetDefinition.transformName;
      if (Utils.isEmpty(targetTransformName)
          || testPipelineMeta.findTransform(targetTransformName) != null) {
        continue;
      }
      DummyMeta dummyMeta = new DummyMeta();
      TransformMeta targetTransform =
          new TransformMeta(
              registry.getPluginId(TransformPluginType.class, dummyMeta),
              targetTransformName,
              dummyMeta);
      targetTransform.setLocation(450, y);
      y += 100;
      testPipelineMeta.addTransform(targetTransform);
      testPipelineMeta.addPipelineHop(new PipelineHopMeta(scriptTransform, targetTransform));
    }

    // Point the info and target definitions at the stand-ins so the info hops are
    // recognised as such and the class finds its row sets.
    //
    udjcMeta.searchInfoAndTargetTransforms(testPipelineMeta.getTransforms());

    return testPipelineMeta;
  }

  /**
   * The fields entering the transform over its main input, leaving out the info transforms
   * configured in the given (possibly not yet saved) metadata.
   */
  public static IRowMeta getMainInputFields(
      IVariables variables,
      PipelineMeta pipelineMeta,
      String transformName,
      UserDefinedJavaClassMeta udjcMeta)
      throws HopTransformException {
    TransformMeta thisTransform = pipelineMeta.findTransform(transformName);
    if (thisTransform == null) {
      return null;
    }
    List<String> infoTransformNames = new ArrayList<>();
    for (InfoTransformDefinition infoDefinition : udjcMeta.getInfoTransformDefinitions()) {
      infoTransformNames.add(Const.NVL(infoDefinition.getTransformName(), ""));
    }

    IRowMeta rowMeta = new RowMeta();
    for (TransformMeta prevTransform : pipelineMeta.findPreviousTransforms(thisTransform, true)) {
      if (Const.indexOfString(prevTransform.getName(), infoTransformNames) >= 0) {
        continue;
      }
      IRowMeta prevRowMeta =
          pipelineMeta.getTransformFields(variables, prevTransform, thisTransform, null);
      for (IValueMeta valueMeta : prevRowMeta.getValueMetaList()) {
        if (rowMeta.searchValueMeta(valueMeta.getName()) == null) {
          rowMeta.addValueMeta(valueMeta.clone());
        }
      }
    }
    return rowMeta;
  }

  /** A row generator producing 10 rows of test values for the given fields. */
  public static RowGeneratorMeta createTestDataGenerator(IRowMeta rowMeta) {
    RowGeneratorMeta generatorMeta = new RowGeneratorMeta();
    generatorMeta.setRowLimit("10");
    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(i);
      if (valueMeta.isStorageBinaryString()) {
        valueMeta.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
      }
      GeneratorField field = new GeneratorField();
      field.setName(valueMeta.getName());
      field.setType(valueMeta.getTypeDesc());
      field.setLength(valueMeta.getLength());
      field.setPrecision(valueMeta.getPrecision());
      field.setCurrency(valueMeta.getCurrencySymbol());
      field.setDecimal(valueMeta.getDecimalSymbol());
      field.setGroup(valueMeta.getGroupingSymbol());

      String string = null;
      try {
        switch (valueMeta.getType()) {
          case IValueMeta.TYPE_DATE:
            field.setFormat("yyyy/MM/dd HH:mm:ss");
            valueMeta.setConversionMask(field.getFormat());
            string = valueMeta.getString(new Date());
            break;
          case IValueMeta.TYPE_STRING:
            string = "test value test value";
            break;
          case IValueMeta.TYPE_INTEGER:
            field.setFormat("#");
            valueMeta.setConversionMask(field.getFormat());
            string = valueMeta.getString(0L);
            break;
          case IValueMeta.TYPE_NUMBER:
            field.setFormat("#.#");
            valueMeta.setConversionMask(field.getFormat());
            string = valueMeta.getString(0.0D);
            break;
          case IValueMeta.TYPE_BIGNUMBER:
            field.setFormat("#.#");
            valueMeta.setConversionMask(field.getFormat());
            string = valueMeta.getString(BigDecimal.ZERO);
            break;
          case IValueMeta.TYPE_BOOLEAN:
            string = valueMeta.getString(Boolean.TRUE);
            break;
          case IValueMeta.TYPE_BINARY:
            string =
                valueMeta.getString(
                    new byte[] {
                      65, 66, 67, 68, 69, 70, 71, 72, 73, 74,
                    });
            break;
          default:
            break;
        }
      } catch (HopValueException e) {
        // Leave the test value empty
      }

      field.setValue(string);
      generatorMeta.getFields().add(field);
    }
    return generatorMeta;
  }
}
