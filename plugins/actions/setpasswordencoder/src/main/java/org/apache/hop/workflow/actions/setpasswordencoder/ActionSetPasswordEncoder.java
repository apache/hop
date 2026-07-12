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

package org.apache.hop.workflow.actions.setpasswordencoder;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.ITwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;

/**
 * Explicitly enable (or switch) the process-global two-way password encoder from a point in a
 * workflow. Prefer project/environment variables for default configuration; use this action when
 * the decision should be visible on the canvas.
 *
 * <p>Does not store raw key material in the workflow. Supply a key via variable (default {@link
 * Const#HOP_AES_ENCODER_KEY}) and/or a key file path ({@link Const#HOP_AES_ENCODER_KEY_FILE}).
 */
@Action(
    id = "SET_PASSWORD_ENCODER",
    name = "i18n::ActionSetPasswordEncoder.Name",
    description = "i18n::ActionSetPasswordEncoder.Description",
    image = "SetPasswordEncoder.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
    keywords = "i18n::ActionSetPasswordEncoder.keyword",
    documentationUrl = "/workflow/actions/setpasswordencoder.html")
public class ActionSetPasswordEncoder extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionSetPasswordEncoder.class;

  /** Plugin ID: Hop, AES2, AES, or another registered two-way password encoder. */
  @HopMetadataProperty(key = "encoder_plugin_id")
  private String encoderPluginId;

  /**
   * Optional variable name that holds the AES key. When empty, {@link Const#HOP_AES_ENCODER_KEY} is
   * used as-is from the variable space / system properties.
   */
  @HopMetadataProperty(key = "key_variable")
  private String keyVariable;

  /** Optional path to a file containing the AES key (supports variables). */
  @HopMetadataProperty(key = "key_file")
  private String keyFile;

  public ActionSetPasswordEncoder(String name) {
    super(name, "");
    encoderPluginId = "AES2";
    keyVariable = Const.HOP_AES_ENCODER_KEY;
    keyFile = null;
  }

  public ActionSetPasswordEncoder() {
    this("");
  }

  public ActionSetPasswordEncoder(ActionSetPasswordEncoder other) {
    super(other.getName(), other.getDescription(), other.getPluginId());
    this.encoderPluginId = other.encoderPluginId;
    this.keyVariable = other.keyVariable;
    this.keyFile = other.keyFile;
  }

  @Override
  public Object clone() {
    return new ActionSetPasswordEncoder(this);
  }

  @Override
  public Result execute(Result result, int nr) throws HopException {
    result.setResult(false);
    result.setNrErrors(1);

    try {
      String pluginId = resolve(Const.NVL(encoderPluginId, "Hop"));
      if (Utils.isEmpty(pluginId)) {
        throw new HopException(
            BaseMessages.getString(PKG, "ActionSetPasswordEncoder.Error.PluginIdMissing"));
      }

      // Optionally promote a named variable's value into HOP_AES_ENCODER_KEY for the encoder.
      // Only set workflow variables — do not write key material into JVM system properties so
      // project switches do not inherit a previous key via Variables.initializeFrom(System).
      String keyVarName = resolve(Const.NVL(keyVariable, Const.HOP_AES_ENCODER_KEY));
      if (StringUtils.isNotEmpty(keyVarName) && !Const.HOP_AES_ENCODER_KEY.equals(keyVarName)) {
        String keyValue = getVariable(keyVarName);
        if (StringUtils.isNotEmpty(keyValue)) {
          setVariable(Const.HOP_AES_ENCODER_KEY, keyValue);
          if (parentWorkflow != null) {
            parentWorkflow.setVariable(Const.HOP_AES_ENCODER_KEY, keyValue);
          }
        }
      }

      String resolvedKeyFile = resolve(keyFile);
      if (StringUtils.isNotEmpty(resolvedKeyFile)) {
        setVariable(Const.HOP_AES_ENCODER_KEY_FILE, resolvedKeyFile);
        if (parentWorkflow != null) {
          parentWorkflow.setVariable(Const.HOP_AES_ENCODER_KEY_FILE, resolvedKeyFile);
        }
      }

      setVariable(Const.HOP_PASSWORD_ENCODER_PLUGIN, pluginId);
      if (parentWorkflow != null) {
        parentWorkflow.setVariable(Const.HOP_PASSWORD_ENCODER_PLUGIN, pluginId);
      }

      Encr.init(pluginId, this);
      updateMetadataProviderEncoder(Encr.getEncoder());

      logBasic(
          BaseMessages.getString(PKG, "ActionSetPasswordEncoder.Log.EncoderInitialized", pluginId));
      result.setResult(true);
      result.setNrErrors(0);
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "ActionSetPasswordEncoder.Error.Failed"), e);
      result.setResult(false);
      result.setNrErrors(1);
    }

    return result;
  }

  private void updateMetadataProviderEncoder(ITwoWayPasswordEncoder encoder) {
    if (encoder == null) {
      return;
    }
    IHopMetadataProvider provider = getMetadataProvider();
    applyEncoder(provider, encoder);

    if (getParentWorkflow() != null) {
      applyEncoder(getParentWorkflow().getMetadataProvider(), encoder);
    }
  }

  private void applyEncoder(IHopMetadataProvider provider, ITwoWayPasswordEncoder encoder) {
    if (provider == null) {
      return;
    }
    if (provider instanceof MultiMetadataProvider multi) {
      multi.setTwoWayPasswordEncoder(encoder);
      if (multi.getProviders() != null) {
        for (IHopMetadataProvider nested : multi.getProviders()) {
          applyEncoder(nested, encoder);
        }
      }
    } else if (provider instanceof JsonMetadataProvider json) {
      json.setTwoWayPasswordEncoder(encoder);
    } else if (provider instanceof MemoryMetadataProvider memory) {
      memory.setTwoWayPasswordEncoder(encoder);
    }
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }

  public String getEncoderPluginId() {
    return encoderPluginId;
  }

  public void setEncoderPluginId(String encoderPluginId) {
    this.encoderPluginId = encoderPluginId;
  }

  public String getKeyVariable() {
    return keyVariable;
  }

  public void setKeyVariable(String keyVariable) {
    this.keyVariable = keyVariable;
  }

  public String getKeyFile() {
    return keyFile;
  }

  public void setKeyFile(String keyFile) {
    this.keyFile = keyFile;
  }
}
