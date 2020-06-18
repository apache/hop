package org.apache.hop.core.vfs.plugin;

import org.apache.commons.vfs2.provider.FileProvider;

public interface IVfs {
  String[] getUrlSchemes();

  FileProvider getProvider();
}
