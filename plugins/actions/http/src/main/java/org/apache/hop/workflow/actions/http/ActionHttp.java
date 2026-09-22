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

package org.apache.hop.workflow.actions.http;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.utils.DateUtils;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.InputStreamEntity;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.io.CountingInputStream;
import org.apache.hop.core.io.CountingOutputStream;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.HttpClientManager;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.LineageHttpIoEmitter;
import org.apache.hop.lineage.model.HttpDirection;
import org.apache.hop.lineage.model.HttpLineagePayload;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.rest.RestConnection;
import org.apache.hop.metadata.rest.client.RestClientFactory;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.w3c.dom.Node;

/** This defines an HTTP action. */
@Action(
    id = "HTTP",
    name = "i18n::ActionHTTP.Name",
    description = "i18n::ActionHTTP.Description",
    image = "HTTP.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    keywords = "i18n::ActionHttp.keyword",
    documentationUrl = "/workflow/actions/http.html",
    // Shared with the REST connection metadata type and the REST transform: a connection selected
    // here is loaded from the same class loader that defined it.
    classLoaderGroup = "rest")
@Getter
@Setter
public class ActionHttp extends ActionBase {
  private static final Class<?> PKG = ActionHttp.class;

  private static final String CONST_URL_FIELDNAME = "URL";
  private static final String COSNT_UPLOADFILE_FIELDNAME = "UPLOAD";
  private static final String CONST_TARGETFILE_FIELDNAME = "DESTINATION";

  /** Port used when a proxy host is given without one, matching the JDK's own default. */
  private static final int DEFAULT_PROXY_PORT = 8080;

  // Base info

  /**
   * Optional REST connection supplying the client: proxy, credentials, TLS and timeouts. When one
   * is selected the action's own authentication and proxy fields are not read.
   */
  @HopMetadataProperty(
      key = "connection_name",
      hopMetadataPropertyType = HopMetadataPropertyType.REST_CONNECTION)
  private String connectionName;

  @HopMetadataProperty(key = "url")
  private String url;

  @HopMetadataProperty(key = "targetfilename")
  private String targetFilename;

  @HopMetadataProperty(key = "file_appended")
  private boolean fileAppended;

  @HopMetadataProperty(key = "date_time_added")
  private boolean dateTimeAdded;

  @HopMetadataProperty(key = "targetfilename_extension")
  private String targetFilenameExtension;

  // Send file content to server?
  @HopMetadataProperty(key = "uploadfilename")
  private String uploadFilename;

  // The fieldname that contains the URL
  // Get it from a previous pipeline with Result.
  @HopMetadataProperty(key = "url_fieldname")
  private String urlFieldname;

  @HopMetadataProperty(key = "upload_fieldname")
  private String uploadFieldname;

  @HopMetadataProperty(key = "dest_fieldname")
  private String destinationFieldname;

  @HopMetadataProperty(key = "run_every_row")
  private boolean runForEveryRow;

  @HopMetadataProperty(key = "ignore_ssl")
  private boolean ignoreSsl;

  // Proxy settings
  @HopMetadataProperty(key = "proxy_host")
  private String proxyHostname;

  @HopMetadataProperty(key = "proxy_port")
  private String proxyPort;

  @HopMetadataProperty(key = "non_proxy_hosts")
  private String nonProxyHosts;

  @HopMetadataProperty(key = "proxy_username")
  private String proxyUsername;

  @HopMetadataProperty(key = "proxy_password", password = true)
  private String proxyPassword;

  @HopMetadataProperty(key = "username")
  private String username;

  @HopMetadataProperty(key = "password", password = true)
  private String password;

  @HopMetadataProperty(key = "addfilenameresult")
  private boolean addFilenameResult;

  @HopMetadataProperty(key = "reply_variable")
  private String replyVariableName;

  @HopMetadataProperty(key = "header", groupKey = "headers")
  private List<Header> headers;

  public ActionHttp(String n) {
    super(n, "");
    url = null;
    addFilenameResult = true;
  }

  public ActionHttp() {
    this("");
  }

  /**
   * @deprecated keep for backwards compatibility
   * @param entrynode the top-level XML node
   * @param metadataProvider The metadataProvider to optionally load from.
   * @param variables
   * @throws HopXmlException
   */
  @Override
  @Deprecated(since = "2.13")
  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode, metadataProvider, variables);
      // Keep
      targetFilenameExtension =
          Const.NVL(
              XmlHandler.getTagValue(entrynode, "targetfilename_extension"),
              XmlHandler.getTagValue(entrynode, "targetfilename_extention"));
    } catch (HopXmlException xe) {
      throw new HopXmlException("Unable to load action of type 'HTTP' from XML node", xe);
    }
  }

  public boolean isAddFilenameToResult() {
    return addFilenameResult;
  }

  public void setAddFilenameToResult(boolean addfilenameresult) {
    this.addFilenameResult = addfilenameresult;
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(false);

    if (isBasic()) {
      logBasic(BaseMessages.getString(PKG, "ActionHTTP.StartAction"));
    }

    // Get previous result rows...
    List<RowMetaAndData> resultRows;
    String urlFieldnameToUse;
    String uploadFieldnameToUse;
    String destinationFieldnameToUse;

    if (Utils.isEmpty(urlFieldname)) {
      urlFieldnameToUse = CONST_URL_FIELDNAME;
    } else {
      urlFieldnameToUse = urlFieldname;
    }

    if (Utils.isEmpty(uploadFieldname)) {
      uploadFieldnameToUse = COSNT_UPLOADFILE_FIELDNAME;
    } else {
      uploadFieldnameToUse = uploadFieldname;
    }

    if (Utils.isEmpty(destinationFieldname)) {
      destinationFieldnameToUse = CONST_TARGETFILE_FIELDNAME;
    } else {
      destinationFieldnameToUse = destinationFieldname;
    }

    if (runForEveryRow) {
      resultRows = previousResult.getRows();
      if (resultRows == null) {
        result.setNrErrors(1);
        logError(BaseMessages.getString(PKG, "ActionHTTP.Error.UnableGetResultPrevious"));
        return result;
      }
    } else {
      resultRows = new ArrayList<>();
      RowMetaAndData row = new RowMetaAndData();
      row.addValue(new ValueMetaString(urlFieldnameToUse), resolve(url));
      row.addValue(new ValueMetaString(uploadFieldnameToUse), resolve(uploadFilename));
      row.addValue(new ValueMetaString(destinationFieldnameToUse), resolve(targetFilename));
      resultRows.add(row);
    }

    RestConnection restConnection;
    try {
      restConnection = loadRestConnection();
    } catch (HopException e) {
      result.setNrErrors(1);
      logError(e.getMessage());
      return result;
    }

    for (int i = 0; i < resultRows.size() && result.getNrErrors() == 0; i++) {
      RowMetaAndData row = resultRows.get(i);

      OutputStream outputFile = null;
      InputStream input = null;
      long bytesReadThisRow = 0L;
      long bytesWrittenThisRow = 0L;
      long httpLineageStart = 0L;
      long httpLineageRequestBytes = 0L;
      long httpLineageResponseBytes = 0L;

      try {
        httpLineageStart = System.currentTimeMillis();
        String urlToUse =
            restConnection == null
                ? resolve(row.getString(urlFieldnameToUse, ""))
                : RestConnection.resolveAgainstBase(
                    resolve(restConnection.getBaseUrl()),
                    resolve(row.getString(urlFieldnameToUse, "")));
        String realUploadFile = resolve(row.getString(uploadFieldnameToUse, ""));
        String realTargetFile = resolve(row.getString(destinationFieldnameToUse, ""));

        if (isBasic()) {
          logBasic(BaseMessages.getString(PKG, "ActionHTTP.Log.ConnectingURL", urlToUse));
        }

        if (dateTimeAdded) {
          SimpleDateFormat daf = new SimpleDateFormat();
          Date now = new Date();

          daf.applyPattern("yyyMMdd");
          realTargetFile += "_" + daf.format(now);
          daf.applyPattern("HHmmss");
          realTargetFile += "_" + daf.format(now);

          if (!Utils.isEmpty(targetFilenameExtension)) {
            realTargetFile += "." + resolve(targetFilenameExtension);
          }
        }

        // Create the output File...
        outputFile = new CountingOutputStream(HopVfs.getOutputStream(realTargetFile, fileAppended));

        URI uri = toUri(urlToUse);
        HttpHost target = HttpClientManager.createHttpHost(uri);

        ClassicHttpRequest request =
            Utils.isEmpty(realUploadFile) ? new HttpGet(uri) : new HttpPost(uri);
        addRequestHeaders(request);
        addConnectionAuthentication(request, restConnection, urlToUse);

        CountingInputStream uploadStream = null;

        // A client per request: the target host decides which credentials apply, and with a URL
        // taken from a result row that host changes from row to row.
        try (CloseableHttpClient httpClient =
            restConnection == null
                ? createHttpClient(target)
                : RestClientFactory.createClient(restConnection.createClientSettings())) {

          // See if we need to send a file over?
          if (!Utils.isEmpty(realUploadFile)) {
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(PKG, "ActionHTTP.Log.SendingFile", realUploadFile));
            }
            FileObject uploadFileObject = HopVfs.getFileObject(realUploadFile);
            uploadStream =
                new CountingInputStream(
                    new BufferedInputStream(HopVfs.getInputStream(uploadFileObject)));
            request.setEntity(
                new InputStreamEntity(uploadStream, uploadFileObject.getContent().getSize(), null));
          }

          if (isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "ActionHTTP.Log.StartReadingReply"));
          }

          try (CloseableHttpResponse response = httpClient.execute(target, request)) {
            int statusCode = response.getCode();

            if (uploadStream != null) {
              bytesReadThisRow += uploadStream.getCount();
              bytesWrittenThisRow += uploadStream.getCount();
              httpLineageRequestBytes = uploadStream.getCount();
              if (isDetailed()) {
                logDetailed(BaseMessages.getString(PKG, "ActionHTTP.Log.FinishedSendingFile"));
              }
            }

            if (statusCode >= HttpStatus.SC_BAD_REQUEST) {
              result.setNrErrors(1);
              logError(statusErrorMessage(statusCode, urlToUse));
            } else {
              HttpEntity entity = response.getEntity();
              String contentType = entity == null ? null : entity.getContentType();

              Instant lastModified =
                  DateUtils.parseStandardDate(response, HttpHeaders.LAST_MODIFIED);
              if (isBasic()) {
                logBasic(
                    BaseMessages.getString(
                        PKG,
                        "ActionHTTP.Log.ReplayInfo",
                        contentType,
                        lastModified == null ? new Date(0) : Date.from(lastModified)));
              }

              ByteArrayOutputStream replyBuffer = null;
              String resolvedReplyVariable =
                  Utils.isEmpty(replyVariableName) ? "" : resolve(replyVariableName);
              if (!Utils.isEmpty(resolvedReplyVariable)) {
                replyBuffer = new ByteArrayOutputStream();
              }

              input =
                  new CountingInputStream(
                      entity == null ? InputStream.nullInputStream() : entity.getContent());
              byte[] buffer = new byte[8192];
              int bytesRead;
              while ((bytesRead = input.read(buffer)) != -1) {
                outputFile.write(buffer, 0, bytesRead);
                if (replyBuffer != null) {
                  replyBuffer.write(buffer, 0, bytesRead);
                }
              }
              bytesReadThisRow += ((CountingInputStream) input).getCount();
              bytesWrittenThisRow += ((CountingOutputStream) outputFile).getCount();
              httpLineageResponseBytes = ((CountingInputStream) input).getCount();

              if (replyBuffer != null) {
                storeReplyInVariable(resolvedReplyVariable, replyBuffer.toByteArray(), contentType);
              }

              if (isBasic()) {
                logBasic(
                    BaseMessages.getString(
                        PKG,
                        "ActionHTTP.Log.FinisedWritingReply",
                        ((CountingInputStream) input).getCount(),
                        realTargetFile));
              }

              if (addFilenameResult) {
                // Add to the result files...
                ResultFile resultFile =
                    new ResultFile(
                        ResultFile.FILE_TYPE_GENERAL,
                        HopVfs.getFileObject(realTargetFile),
                        parentWorkflow.getWorkflowName(),
                        toString());
                result.getResultFiles().put(resultFile.getFile().toString(), resultFile);
              }

              result.setResult(true);

              if (parentWorkflow != null) {
                LineageHttpIoEmitter.emitWorkflowActionHttpIo(
                    parentWorkflow,
                    this,
                    new HttpLineagePayload(
                        HttpDirection.CLIENT,
                        request.getMethod(),
                        urlToUse,
                        statusCode,
                        httpLineageRequestBytes > 0 ? httpLineageRequestBytes : null,
                        httpLineageResponseBytes > 0 ? httpLineageResponseBytes : null,
                        System.currentTimeMillis() - httpLineageStart,
                        true,
                        null));
              }
            }
          }
        } finally {
          if (uploadStream != null) {
            uploadStream.close();
          }
        }
      } catch (URISyntaxException e) {
        result.setNrErrors(1);
        logError(BaseMessages.getString(PKG, "ActionHTTP.Error.NotValidURL", url, e.getMessage()));
        logError(Const.getStackTracker(e));
      } catch (IOException e) {
        result.setNrErrors(1);
        logError(
            BaseMessages.getString(PKG, "ActionHTTP.Error.CanNotSaveHTTPResult", e.getMessage()));
        logError(Const.getStackTracker(e));
      } catch (Exception e) {
        result.setNrErrors(1);
        logError(
            BaseMessages.getString(PKG, "ActionHTTP.Error.ErrorGettingFromHTTP", e.getMessage()));
        logError(Const.getStackTracker(e));
      } finally {
        // Close it all
        try {
          if (input != null) {
            input.close();
          }
          if (outputFile != null) {
            outputFile.close();
          }
        } catch (Exception e) {
          logError(
              BaseMessages.getString(PKG, "ActionHTTP.Error.CanNotCloseStream", e.getMessage()));
          result.setNrErrors(1);
        }
      }

      result.setBytesReadThisAction(result.getBytesReadThisAction() + bytesReadThisRow);
      result.setBytesWrittenThisAction(result.getBytesWrittenThisAction() + bytesWrittenThisRow);
    }

    return result;
  }

  /**
   * The URL to call, as a URI HttpClient can route. Anything without a scheme and a host is
   * rejected here rather than further down, where it would surface as a less obvious error.
   */
  private static URI toUri(String urlToUse) throws URISyntaxException {
    URI uri = new URI(Const.NVL(urlToUse, "").trim());
    if (uri.getScheme() == null || uri.getAuthority() == null) {
      throw new URISyntaxException(String.valueOf(urlToUse), "No protocol or host in the URL");
    }
    return uri;
  }

  /**
   * The client for one request: proxy routing, the bypass list and both sets of credentials.
   *
   * <p>Server and proxy credentials are registered for their own host only. They used to share a
   * single JVM-wide {@link java.net.Authenticator}, which answers a 401 from the server and a 407
   * from the proxy alike, so whichever asked first received the other's user name and password.
   */
  private CloseableHttpClient createHttpClient(HttpHost target) {
    HttpClientManager.HttpClientBuilderFacade builder =
        HttpClientManager.getInstance().createBuilder();

    String realProxyHost = resolve(proxyHostname);
    if (!Utils.isEmpty(realProxyHost)) {
      int realProxyPort = Const.toInt(resolve(proxyPort), DEFAULT_PROXY_PORT);
      HttpHost proxy = new HttpHost("http", realProxyHost, realProxyPort);
      builder.setProxy(proxy.getHostName(), proxy.getPort(), proxy.getSchemeName());
      builder.setNonProxyHosts(resolve(nonProxyHosts));
      if (!Utils.isEmpty(resolve(proxyUsername))) {
        builder.setCredentials(
            resolve(proxyUsername),
            Encr.decryptPasswordOptionallyEncrypted(resolve(proxyPassword)),
            new AuthScope(proxy));
      }
    }

    if (!Utils.isEmpty(resolve(username))) {
      builder.setCredentials(
          resolve(username),
          Encr.decryptPasswordOptionallyEncrypted(resolve(password)),
          new AuthScope(target));
    }

    builder.ignoreSsl(isIgnoreSsl());
    return builder.build();
  }

  /**
   * The REST connection this action was pointed at, or {@code null} when it configures its own
   * client.
   */
  private RestConnection loadRestConnection() throws HopException {
    String realConnectionName = resolve(connectionName);
    if (Utils.isEmpty(realConnectionName)) {
      return null;
    }
    IHopMetadataProvider provider = getMetadataProvider();
    if (provider == null) {
      throw new HopException(
          BaseMessages.getString(PKG, "ActionHTTP.Error.ConnectionNotFound", realConnectionName));
    }
    try {
      RestConnection connection =
          provider.getSerializer(RestConnection.class).load(realConnectionName);
      if (connection == null) {
        throw new HopException(
            BaseMessages.getString(PKG, "ActionHTTP.Error.ConnectionNotFound", realConnectionName));
      }
      connection.setVariables(this);
      return connection;
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      // Keep the cause: a class loader split between the metadata plugin and this action surfaces
      // here as a ClassCastException, which is not a missing connection at all.
      throw new HopException(
          BaseMessages.getString(PKG, "ActionHTTP.Error.ConnectionNotLoaded", realConnectionName),
          e);
    }
  }

  /**
   * Bearer, API-key and preemptive Basic authentication are request headers rather than answers to
   * a challenge, so the connection writes them onto every request itself.
   */
  private void addConnectionAuthentication(
      ClassicHttpRequest request, RestConnection restConnection, String urlToUse)
      throws HopException {
    if (restConnection == null) {
      return;
    }
    Map<String, String> authHeaders = new LinkedHashMap<>();
    restConnection.applyAuthentication(authHeaders, urlToUse);
    for (Map.Entry<String, String> authHeader : authHeaders.entrySet()) {
      request.setHeader(authHeader.getKey(), authHeader.getValue());
    }
  }

  /** Copies the configured headers onto the request. */
  private void addRequestHeaders(ClassicHttpRequest request) {
    if (Utils.isEmpty(headers)) {
      return;
    }
    if (isDebug()) {
      logDebug(BaseMessages.getString(PKG, "ActionHTTP.Log.HeadersProvided"));
    }
    for (Header header : headers) {
      if (!Utils.isEmpty(header.getHeaderValue())) {
        String name = resolve(header.getHeaderName());
        String value = resolve(header.getHeaderValue());
        request.setHeader(name, value);
        if (isDebug()) {
          logDebug(BaseMessages.getString(PKG, "ActionHTTP.Log.HeaderSet", name, value));
        }
      }
    }
  }

  /**
   * A 407 gets its own message: it means the proxy rejected the request rather than the server, and
   * the fix is to fill in the proxy user name and password rather than the server's.
   */
  private String statusErrorMessage(int statusCode, String urlToUse) {
    if (statusCode == HttpStatus.SC_PROXY_AUTHENTICATION_REQUIRED) {
      return BaseMessages.getString(
          PKG, "ActionHTTP.Error.ProxyAuthenticationRequired", resolve(proxyHostname));
    }
    return BaseMessages.getString(PKG, "ActionHTTP.Error.HttpStatus", statusCode, urlToUse);
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    String realUrl = resolve(url);
    ResourceReference reference = new ResourceReference(this);
    reference.getEntries().add(new ResourceEntry(realUrl, ResourceType.URL));
    references.add(reference);
    return references;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "targetFilename",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "targetFilenameExtention",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "uploadFilename",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "proxyPort",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.integerValidator()));
  }

  /**
   * Stores the HTTP reply body in a workflow variable so later actions can use it. The value is set
   * on this action and on the parent workflow (and its parents).
   */
  private void storeReplyInVariable(String variableName, byte[] replyBytes, String contentType) {
    if (Utils.isEmpty(variableName) || replyBytes == null) {
      return;
    }
    String reply = new String(replyBytes, charsetFromContentType(contentType));
    setVariable(variableName, reply);
    IWorkflowEngine<WorkflowMeta> parent = getParentWorkflow();
    while (parent != null) {
      parent.setVariable(variableName, reply);
      parent = parent.getParentWorkflow();
    }
    if (isBasic()) {
      logBasic(BaseMessages.getString(PKG, "ActionHTTP.Log.ReplyStoredInVariable", variableName));
    }
  }

  static Charset charsetFromContentType(String contentType) {
    if (Utils.isEmpty(contentType)) {
      return StandardCharsets.UTF_8;
    }
    String lower = contentType.toLowerCase(Locale.ROOT);
    int idx = lower.indexOf("charset=");
    if (idx < 0) {
      return StandardCharsets.UTF_8;
    }
    String charsetName = contentType.substring(idx + 8).trim();
    int separator = charsetName.indexOf(';');
    if (separator >= 0) {
      charsetName = charsetName.substring(0, separator).trim();
    }
    charsetName = charsetName.replace("\"", "").trim();
    try {
      return Charset.forName(charsetName);
    } catch (Exception e) {
      return StandardCharsets.UTF_8;
    }
  }

  @Getter
  @Setter
  public static final class Header {

    @HopMetadataProperty(key = "header_name")
    private String headerName;

    @HopMetadataProperty(key = "header_value")
    private String headerValue;
  }
}
