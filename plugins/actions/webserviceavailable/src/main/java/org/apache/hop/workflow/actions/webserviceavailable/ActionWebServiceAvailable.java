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

package org.apache.hop.workflow.actions.webserviceavailable;

import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;

/** This defines a webservice available action. */
@Action(
    id = "WEBSERVICE_AVAILABLE",
    name = "i18n::ActionWebServiceAvailable.Name",
    description = "i18n::ActionWebServiceAvailable.Description",
    image = "WebServiceAvailable.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Conditions",
    keywords = "i18n::ActionWebServiceAvailable.keyword",
    documentationUrl = "/workflow/actions/webserviceavailable.html")
@Getter
@Setter
public class ActionWebServiceAvailable extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionWebServiceAvailable.class;
  public static final String CONST_SPACES = "      ";

  @HopMetadataProperty private String url;

  @HopMetadataProperty private String connectTimeOut;

  @HopMetadataProperty private String readTimeOut;

  public ActionWebServiceAvailable(String n) {
    super(n, "");
    url = null;
    connectTimeOut = "0";
    readTimeOut = "0";
  }

  public ActionWebServiceAvailable() {
    this("");
  }

  @Override
  public Object clone() {
    ActionWebServiceAvailable je = (ActionWebServiceAvailable) super.clone();
    return je;
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(false);

    String realURL = resolve(getUrl());

    if (!Utils.isEmpty(realURL)) {
      int connectTimeOut = Const.toInt(resolve(getConnectTimeOut()), 0);
      int readTimeOut = Const.toInt(resolve(getReadTimeOut()), 0);
      InputStream in = null;
      try {

        URLConnection conn = new URL(realURL).openConnection();
        conn.setConnectTimeout(connectTimeOut);
        conn.setReadTimeout(readTimeOut);
        in = conn.getInputStream();
        // Web service is available
        result.setResult(true);
      } catch (Exception e) {
        result.setNrErrors(1);
        String message =
            BaseMessages.getString(
                PKG, "ActionWebServiceAvailable.ERROR_0004_Exception", realURL, e.toString());
        logError(message);
        result.setLogText(message);
      } finally {
        if (in != null) {
          try {
            in.close();
          } catch (Exception e) {
            /* Ignore */
          }
        }
      }
    } else {
      result.setNrErrors(1);
      String message =
          BaseMessages.getString(PKG, "ActionWebServiceAvailable.ERROR_0005_No_URL_Defined");
      logError(message);
      result.setLogText(message);
    }

    return result;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }
}
