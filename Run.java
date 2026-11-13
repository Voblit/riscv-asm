import java.io.*;
public class Run {
  static native String readLine();
  public static void main(String[] a) throws Exception {
    System.setIn(new InputStream() {
      byte[] buf = new byte[0]; int pos = 0;
      public int read() {
        if (pos >= buf.length) { buf = readLine().getBytes(); pos = 0; }
        return buf[pos++] & 0xff;
      }
    });
    rars.Launch.main(a);
  }
}
