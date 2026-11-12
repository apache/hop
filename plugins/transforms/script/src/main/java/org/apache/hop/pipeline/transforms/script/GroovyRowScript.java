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

package org.apache.hop.pipeline.transforms.script;

import groovy.lang.Binding;
import groovy.lang.Closure;
import groovy.lang.DelegatingMetaClass;
import groovy.lang.GroovySystem;
import groovy.lang.MetaClass;
import groovy.lang.MissingMethodException;
import groovy.lang.MissingPropertyException;
import groovy.lang.Script;
import groovy.lang.Tuple;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import org.codehaus.groovy.jsr223.GroovyScriptEngineImpl;
import org.codehaus.groovy.runtime.InvokerHelper;
import org.codehaus.groovy.util.ManagedConcurrentValueMap;

/**
 * Runs the row script of a Groovy Script transform without going through the JSR-223 engine for
 * every row.
 *
 * <p>The engine's {@code CompiledScript.eval()} builds, for every call, a new script object, a
 * closure for each of its methods and a metaclass for that one object. The per-object metaclass
 * makes Groovy link every call in the script again, which is most of the cost of a short script.
 * Here the script class is compiled once and each row gets a new instance that is run directly.
 *
 * <p>What a script can see and do is kept the same as with the engine:
 *
 * <ul>
 *   <li>a new script object per row, so {@code @Field} variables start fresh for every row;
 *   <li>variables are read from and written to the script context, with the engine's own rules for
 *       {@code out} and {@code context};
 *   <li>a method the script does not have is looked up among the functions of the scripts the
 *       engine evaluated before, such as the start script;
 *   <li>the functions of the row script are known to the engine, for the start and end scripts;
 *   <li>an exception leaves as a {@link ScriptException} around the original one.
 * </ul>
 */
final class GroovyRowScript {

  private final Class<?> scriptClass;
  private final Binding binding;

  /** The script object of the current row. */
  private Script lastScript;

  /** The functions registered with the engine, see {@link #registerFunctions}. */
  private final List<Closure<?>> functions = new ArrayList<>();

  private GroovyRowScript(Class<?> scriptClass, Binding binding) {
    this.scriptClass = scriptClass;
    this.binding = binding;
  }

  /**
   * @return the row script, or null when this is not Groovy's JSR-223 engine or the source is a
   *     class and not a script: the caller then keeps using the engine
   */
  static GroovyRowScript create(ScriptEngine engine, String source, ScriptContext context) {
    if (!(engine instanceof GroovyScriptEngineImpl groovyEngine) || context == null) {
      return null;
    }
    // The engine's own class loader: classes that the start script defined are visible.
    Class<?> scriptClass = groovyEngine.getClassLoader().parseClass(source);
    if (!Script.class.isAssignableFrom(scriptClass)) {
      return null;
    }
    // One metaclass for the class, set before the first instance exists. Unlike a metaclass per
    // object it leaves Groovy's call site caches alone.
    MetaClass metaClass = GroovySystem.getMetaClassRegistry().getMetaClass(scriptClass);
    GroovySystem.getMetaClassRegistry()
        .setMetaClass(scriptClass, new GlobalFunctionsMetaClass(metaClass, groovyEngine));
    GroovyRowScript rowScript = new GroovyRowScript(scriptClass, new ContextBinding(context));
    return rowScript.registerFunctions(groovyEngine) ? rowScript : null;
  }

  /** Runs the script for one row and returns its result. */
  Object run() throws ScriptException {
    try {
      Script script = InvokerHelper.createScript(scriptClass, binding);
      lastScript = script;
      return script.run();
    } catch (Exception e) {
      throw new ScriptException(e);
    }
  }

  /**
   * Tells the engine about the functions of the row script, as it does itself for every script it
   * runs: a function of the start script that calls one of the row script, and the end script, find
   * them there. The engine keeps them in a private map, so this needs reflection.
   *
   * @return false when the engine's map cannot be reached: the caller must not run directly
   */
  @SuppressWarnings("unchecked")
  private boolean registerFunctions(GroovyScriptEngineImpl engine) {
    try {
      Field field = GroovyScriptEngineImpl.class.getDeclaredField("globalClosures");
      field.setAccessible(true);
      ManagedConcurrentValueMap<String, Closure<?>> globalClosures =
          (ManagedConcurrentValueMap<String, Closure<?>>) field.get(engine);
      for (Method method : scriptClass.getMethods()) {
        String name = method.getName();
        Closure<?> function = new CurrentScriptFunction(name);
        // the engine's map does not keep its values alive
        functions.add(function);
        globalClosures.put(name, function);
      }
      return true;
    } catch (ReflectiveOperationException | RuntimeException e) {
      return false;
    }
  }

  /** A function of the row script, called on the script object of the current row. */
  private final class CurrentScriptFunction extends Closure<Object> {
    private static final long serialVersionUID = 1L;
    private final String name;

    CurrentScriptFunction(String name) {
      super(GroovyRowScript.class);
      this.name = name;
    }

    @Override
    public Object call(Object... args) {
      Script script = lastScript;
      if (script == null) {
        throw new MissingMethodException(name, scriptClass, args);
      }
      return InvokerHelper.invokeMethod(script, name, args);
    }
  }

  /** Variable lookup in the script context, as the engine's own binding does it. */
  private static final class ContextBinding extends Binding {
    private final transient ScriptContext context;

    ContextBinding(ScriptContext context) {
      super(context.getBindings(ScriptContext.ENGINE_SCOPE));
      this.context = context;
    }

    @Override
    public Object getVariable(String name) {
      synchronized (context) {
        int scope = context.getAttributesScope(name);
        if (scope != -1) {
          return context.getAttribute(name, scope);
        }
        // Script output goes to the context writer when no out variable is provided
        if ("out".equals(name)) {
          Writer writer = context.getWriter();
          if (writer != null) {
            return writer instanceof PrintWriter printWriter
                ? printWriter
                : new PrintWriter(writer, true);
          }
        }
        if ("context".equals(name)) {
          return context;
        }
      }
      throw new MissingPropertyException(name, getClass());
    }

    @Override
    public void setVariable(String name, Object value) {
      synchronized (context) {
        int scope = context.getAttributesScope(name);
        if (scope == -1) {
          scope = ScriptContext.ENGINE_SCOPE;
        }
        context.setAttribute(name, value, scope);
      }
    }
  }

  /**
   * Passes a method the script does not have on to the engine, which knows the functions of every
   * script it evaluated: the start script's, for one.
   */
  private static final class GlobalFunctionsMetaClass extends DelegatingMetaClass {
    private final Invocable engine;

    GlobalFunctionsMetaClass(MetaClass delegate, Invocable engine) {
      super(delegate);
      this.engine = engine;
    }

    @Override
    public Object invokeMethod(Object object, String name, Object args) {
      if (args == null) {
        return invokeMethod(object, name, new Object[0]);
      }
      if (args instanceof Tuple<?> tuple) {
        return invokeMethod(object, name, tuple.toArray());
      }
      if (args instanceof Object[] array) {
        return invokeMethod(object, name, array);
      }
      return invokeMethod(object, name, new Object[] {args});
    }

    @Override
    public Object invokeMethod(Object object, String name, Object[] args) {
      try {
        return super.invokeMethod(object, name, args);
      } catch (MissingMethodException e) {
        return callGlobal(name, args);
      }
    }

    @Override
    public Object invokeStaticMethod(Object object, String name, Object[] args) {
      try {
        return super.invokeStaticMethod(object, name, args);
      } catch (MissingMethodException e) {
        return callGlobal(name, args);
      }
    }

    private Object callGlobal(String name, Object[] args) {
      try {
        return engine.invokeFunction(name, args);
      } catch (NoSuchMethodException e) {
        // No script has such a function either: the error the engine gives for it
        throw new MissingMethodException(name, engine.getClass(), args);
      } catch (ScriptException e) {
        // The engine wraps what the function threw. The script must see the original, so that
        // its own try/catch works as it does when the engine runs it.
        Throwable cause = e.getCause();
        if (cause == null) {
          throw new IllegalStateException(e);
        }
        throw GlobalFunctionsMetaClass.<RuntimeException>sneak(cause);
      }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> T sneak(Throwable throwable) throws T {
      throw (T) throwable;
    }
  }
}
