package tech.streamfusion.iceberg;

import tech.streamfusion.NativeExtensionLoader;

/** JNI boundary for the optional Iceberg data-file implementation. */
final class NativeIceberg {
  static {
    NativeExtensionLoader.load(
        NativeIceberg.class,
        "iceberg",
        NativeIceberg::nativeBuildVersion,
        NativeIceberg::liveNativeHandles);
  }

  private NativeIceberg() {}

  static boolean isLoaded() {
    return true;
  }

  private static native String nativeBuildVersion();

  static native String liveNativeHandles();

  static native long createWriter(
      long schemaAddress,
      String icebergSchema,
      String settings,
      String location,
      Object output,
      byte[] transferBuffer);

  static native void write(long handle, long arrayAddress, int[] selected, int count);

  static native long estimatedBytes(long handle);

  static native String finish(long handle);

  static native void closeWriter(long handle);
}
