package org.apache.hop.core.util;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;

public class ArrowBufferAllocator {
  private static BufferAllocator root;

  public static BufferAllocator rootAllocator() {
    if (root == null) {
      root = new RootAllocator();
    }

    return root;
  }

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      if (root != null) {
        AutoCloseables.closeNoChecked(root);
      }
    }));
  }
}
