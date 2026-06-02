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
package org.apache.hop.pipeline.transforms.janino.scanner;

import static org.apache.xbean.asm9.Opcodes.ACC_PUBLIC;
import static org.apache.xbean.asm9.Opcodes.ACC_STATIC;
import static org.apache.xbean.asm9.Opcodes.ACC_SUPER;
import static org.apache.xbean.asm9.Opcodes.ALOAD;
import static org.apache.xbean.asm9.Opcodes.ARETURN;
import static org.apache.xbean.asm9.Opcodes.IFNONNULL;
import static org.apache.xbean.asm9.Opcodes.V20;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.apache.xbean.asm9.AnnotationVisitor;
import org.apache.xbean.asm9.ClassWriter;
import org.apache.xbean.asm9.Label;
import org.apache.xbean.asm9.MethodVisitor;

class FunctionGenerator {

  private static final String INTERNAL_CLASS_NAME =
      "org/apache/hop/pipeline/transforms/janino/test/TestFunctions";

  private static final String JANINO_FUNCTION_DESC =
      "Lorg/apache/hop/pipeline/transforms/janino/function/JaninoFunction;";

  static byte[] generateClass() {
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
    cw.visit(V20, ACC_PUBLIC | ACC_SUPER, INTERNAL_CLASS_NAME, null, "java/lang/Object", null);

    generateNvlMethod(cw);

    cw.visitEnd();
    return cw.toByteArray();
  }

  private static void generateNvlMethod(ClassWriter cw) {
    MethodVisitor mv =
        cw.visitMethod(
            ACC_PUBLIC | ACC_STATIC,
            "nvl",
            "(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;",
            null,
            null);

    AnnotationVisitor av = mv.visitAnnotation(JANINO_FUNCTION_DESC, true);
    av.visit("name", "nvl");
    av.visit("category", "General");
    av.visit("description", "Implements Oracle style NVL function.");
    av.visit("syntax", "nvl(source, def)");
    av.visit("returns", "String");
    av.visit("semantics", "If source == null or empty return def");
    av.visit(
        "examples",
        "[{\"expression\":\"nvl(null,\\\"bar\\\")\",\"result\":\"bar\",\"level\":\"1\",\"comment\":\"null returns bar\"}]");
    av.visitEnd();

    Label notNull = new Label();
    mv.visitCode();
    mv.visitVarInsn(ALOAD, 0);
    mv.visitJumpInsn(IFNONNULL, notNull);
    mv.visitVarInsn(ALOAD, 1);
    mv.visitInsn(ARETURN);
    mv.visitLabel(notNull);
    mv.visitVarInsn(ALOAD, 0);
    mv.visitInsn(ARETURN);
    mv.visitMaxs(0, 0);
    mv.visitEnd();
  }

  static void writeToDirectory(Path dir) throws IOException {
    Path classFile = dir.resolve(INTERNAL_CLASS_NAME + ".class");
    Files.createDirectories(classFile.getParent());
    Files.write(classFile, generateClass());
  }

  static void writeToJar(Path jarFile) throws IOException {
    try (JarOutputStream jos = new JarOutputStream(Files.newOutputStream(jarFile))) {
      String[] parts = INTERNAL_CLASS_NAME.split("/");
      String prefix = "";
      for (int i = 0; i < parts.length - 1; i++) {
        prefix += parts[i] + "/";
        jos.putNextEntry(new JarEntry(prefix));
        jos.closeEntry();
      }
      jos.putNextEntry(new JarEntry(INTERNAL_CLASS_NAME + ".class"));
      jos.write(generateClass());
      jos.closeEntry();
    }
  }
}
