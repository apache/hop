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

package org.apache.hop.pipeline.transforms.pgpdecryptstream;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "PGPDecryptStream",
    image = "pgpdecryptstream.svg",
    description = "i18n::PGPDecryptStream.Description",
    name = "i18n::PGPDecryptStream.Name",
    categoryDescription =
        "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Cryptography",
    keywords = "i18n::PGPDecryptStreamMeta.keyword",
    documentationUrl = "/pipeline/transforms/pgpdecryptstream.html")
@Getter
@Setter
public class PGPDecryptStreamMeta
    extends BaseTransformMeta<PGPDecryptStream, PGPDecryptStreamData> {
  private static final Class<?> PKG = PGPDecryptStreamMeta.class;

  /** GPG location */
  @HopMetadataProperty(key = "gpglocation")
  private String gpgLocation;

  /** passhrase */
  @HopMetadataProperty(key = "passhrase", password = true)
  private String passPhrase;

  /** Flag : passphrase from field */
  @HopMetadataProperty(key = "passphraseFromField")
  private boolean passPhraseFromField;

  /** passphrase fieldname */
  @HopMetadataProperty(key = "passphraseFieldName")
  private String passPhraseFieldName;

  /** dynamic stream filed */
  @HopMetadataProperty(key = "streamfield")
  private String streamField;

  /** function result: new value name */
  @HopMetadataProperty(key = "resultfieldname")
  private String resultFieldName;

  public PGPDecryptStreamMeta() {
    super();
    resultFieldName = "result";
  }

  public PGPDecryptStreamMeta(PGPDecryptStreamMeta m) {
    this();
    this.gpgLocation = m.gpgLocation;
    this.passPhrase = m.passPhrase;
    this.passPhraseFieldName = m.passPhraseFieldName;
    this.passPhraseFromField = m.passPhraseFromField;
    this.resultFieldName = m.resultFieldName;
    this.streamField = m.streamField;
  }

  @Override
  public Object clone() {
    return new PGPDecryptStreamMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Output fields (String)
    if (!Utils.isEmpty(resultFieldName)) {
      IValueMeta v = new ValueMetaString(variables.resolve(resultFieldName));
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;
    String errorMessage = "";

    if (Utils.isEmpty(gpgLocation)) {
      errorMessage =
          BaseMessages.getString(PKG, "PGPDecryptStreamMeta.CheckResult.GPGLocationMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "PGPDecryptStreamMeta.CheckResult.GPGLocationOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
    }
    if (!isPassPhraseFromField()) {
      // Check static pass-phrase
      if (Utils.isEmpty(passPhrase)) {
        errorMessage =
            BaseMessages.getString(PKG, "PGPDecryptStreamMeta.CheckResult.PassphraseMissing");
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        errorMessage = BaseMessages.getString(PKG, "PGPDecryptStreamMeta.CheckResult.PassphraseOK");
        cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      }
    }
    if (Utils.isEmpty(resultFieldName)) {
      errorMessage =
          BaseMessages.getString(PKG, "PGPDecryptStreamMeta.CheckResult.ResultFieldMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "PGPDecryptStreamMeta.CheckResult.ResultFieldOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(streamField)) {
      errorMessage =
          BaseMessages.getString(PKG, "PGPDecryptStreamMeta.CheckResult.StreamFieldMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "PGPDecryptStreamMeta.CheckResult.StreamFieldOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "PGPDecryptStreamMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "PGPDecryptStreamMeta.CheckResult.NoInpuReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
