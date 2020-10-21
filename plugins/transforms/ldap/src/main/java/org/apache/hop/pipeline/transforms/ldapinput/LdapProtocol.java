/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.apache.hop.pipeline.transforms.ldapinput;

import java.util.Collection;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.ldap.InitialLdapContext;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;

/** Class encapsulating Ldap protocol configuration */
public class LdapProtocol {

  private static Class<?> classFromResourcesPackage = LdapProtocol.class; // for i18n purposes,
  // needed by Translator!!

  private static final String CONNECTION_PREFIX = "ldap://";

  public static final String NAME = "LDAP";

  private final String hostname;

  private final int port;

  private final String derefAliases;

  private final String referral;

  private final ILogChannel log;

  private InitialLdapContext ctx;

  private final Set<String> binaryAttributes;

  public InitialLdapContext getCtx() {
    return ctx;
  }

  public LdapProtocol(
      ILogChannel log, IVariables variables, ILdapMeta meta, Collection<String> binaryAttributes) {
    this.log = log;
    hostname = variables.environmentSubstitute(meta.getHost());
    port =
        Const.toInt(variables.environmentSubstitute(meta.getPort()), LdapConnection.DEFAULT_PORT);
    derefAliases = meta.getDerefAliases();
    referral = meta.getReferrals();

    if (binaryAttributes == null) {
      this.binaryAttributes = new HashSet<>();
    } else {
      this.binaryAttributes = new HashSet<>(binaryAttributes);
    }
  }

  protected String getConnectionPrefix() {
    return CONNECTION_PREFIX;
  }

  /**
   * Method signature used by factory to get display name, method should exist in every ldap
   * protocol
   *
   * @return the display name
   */
  public static String getName() {
    return NAME;
  }

  protected void setupEnvironment(Map<String, String> env, String username, String password)
      throws HopException {
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
    env.put("java.naming.ldap.derefAliases", derefAliases);
    env.put(Context.REFERRAL, referral);

    if (hostname.startsWith(getConnectionPrefix())) {
      env.put(Context.PROVIDER_URL, hostname + ":" + port);
    } else {
      env.put(Context.PROVIDER_URL, getConnectionPrefix() + hostname + ":" + port);
    }

    if (!Utils.isEmpty(username)) {
      env.put(Context.SECURITY_PRINCIPAL, username);
      env.put(Context.SECURITY_CREDENTIALS, password);
      env.put(Context.SECURITY_AUTHENTICATION, "simple");
    } else {
      env.put(Context.SECURITY_AUTHENTICATION, "none");
    }

    if (!binaryAttributes.isEmpty()) {
      env.put("java.naming.ldap.attributes.binary", String.join(" ", binaryAttributes));
    }
  }

  protected InitialLdapContext createLdapContext(Hashtable<String, String> env)
      throws NamingException {
    return new InitialLdapContext(env, null);
  }

  protected void doConnect(String username, String password) throws HopException {
    Hashtable<String, String> env = new Hashtable<>();
    setupEnvironment(env, username, password);
    try {
      ctx = createLdapContext(env);
    } catch (NamingException e) {
      throw new HopException(e);
    }
  }

  public final void connect(String username, String password) throws HopException {
    Hashtable<String, String> env = new Hashtable<>();
    setupEnvironment(env, username, password);
    try {
      /* Establish LDAP association */
      doConnect(username, password);

      if (log.isBasic()) {
        log.logBasic(
            BaseMessages.getString(
                classFromResourcesPackage,
                "LdapInput.Log.ConnectedToServer",
                hostname,
                Const.NVL(username, "")));
      }
      if (log.isDetailed()) {
        log.logDetailed(
            BaseMessages.getString(
                classFromResourcesPackage,
                "LdapInput.ClassUsed.Message",
                ctx.getClass().getName()));
      }

    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              classFromResourcesPackage, "LDAPinput.Exception.ErrorConnecting", e.getMessage()),
          e);
    }
  }

  public void close() throws HopException {
    if (ctx != null) {
      try {
        ctx.close();
        if (log.isBasic()) {
          log.logBasic(
              BaseMessages.getString(
                  classFromResourcesPackage, "LdapInput.log.Disconnection.Done"));
        }
      } catch (Exception e) {
        log.logError(
            BaseMessages.getString(
                classFromResourcesPackage, "LdapInput.Exception.ErrorDisconecting", e.toString()));
        log.logError(Const.getStackTracker(e));
      } finally {
        ctx = null;
      }
    }
  }
}
