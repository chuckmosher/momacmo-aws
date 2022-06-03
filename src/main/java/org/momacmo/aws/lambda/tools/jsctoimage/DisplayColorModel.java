package org.momacmo.aws.lambda.tools.jsctoimage;

import java.awt.image.IndexColorModel;


public class DisplayColorModel {/** Color scale to use for the display: @link http://boole.mines.edu/doc/api/edu/mines/jtk/awt/ColorMap.html  */
  
  public enum ColorModel {
    GRAY,
    SPECTRAL,
    PLUS_MINUS,
    BLUE_WHITE_RED,
    BLACK_WHITE_RED
  };

  static byte[] r = new byte[256];

  static byte[] g = new byte[256];

  static byte[] b = new byte[256];
  
  static byte byteMin = 0;
  static byte byteMax = (byte) 255;
  
  public static IndexColorModel getColorModel( ColorModel colorScale ) {

    switch (colorScale) {
      case GRAY:
        return grayScale();
      case SPECTRAL:
        return spectral();
      case PLUS_MINUS:
        return plusMinus();
      case BLUE_WHITE_RED:
        return blueWhiteRed();
      case BLACK_WHITE_RED:
          return blackWhiteRed();
      default:
        return spectral();
    }
    
  }

  public static IndexColorModel grayScale() {

    /* Black -> white, Increase rgb over 256 entries */
    for (int i = 0; i < 256; i++) {
      byte val = (byte)(i);
      r[i] = val;
      g[i] = val;
      b[i] = val;
    }

    return new IndexColorModel(8, 256, r, g, b);
  }

  public static IndexColorModel blueWhiteRed() {
    int j;
    float slope;
    byte byteMax = (byte) 0xff;
    float xmax = 255;
    byte val = 0;
    int i = 0;

    /* Initialize to white */
    for (i = 0; i < 256; i++) {
      r[i] = byteMax;
      b[i] = byteMax;
      g[i] = byteMax;
    }

    /* Blue -> white, Increase red and green over 127 entries */
    i = 0;
    slope = (xmax - xmax / 127f) / 127f;
    for (j = 0; j < 127; j++, i++) {
      val = (byte) (slope * j);
      g[i] = val;
      r[i] = val;
    }

    /* Skip over center values (already full white) */
    i += 2;

    /* white -> red, decrease blue and green over 127 entries */
    for (j = 1; j < 128; j++, i++) {
      val = (byte) (xmax - slope * j);
      b[i] = val;
      g[i] = val;
    }
    return new IndexColorModel(8, 256, r, g, b);
  }

  public static IndexColorModel blackWhiteRed() {
    int j;
    float slope;
    byte max = (byte) 0xff;
    float xmax = 255;
    byte val = 0;
    int i = 0;

    /* Initialize to white */
    for (i = 0; i < 256; i++) {
      r[i] = max;
      b[i] = max;
      g[i] = max;
    }

    /* Black -> white, Increase rgb over 127 entries */
    i = 0;
    slope = (xmax - xmax / 127f) / 127f;
    for (j = 0; j < 127; j++, i++) {
      val = (byte) (slope * j);
      r[i] = val;
      g[i] = val;
      b[i] = val;
    }

    /* Skip over center values (already full white) */
    i += 2;

    /* white -> red, decrease blue and green over 127 entries */
    for (j = 1; j < 128; j++, i++) {
      val = (byte) (xmax - slope * j);
      b[i] = val;
      g[i] = val;
    }
    return new IndexColorModel(8, 256, r, g, b);
  }

  public static IndexColorModel plusMinus() {
    int j;
    float slope;
    byte max = (byte) 0xff;
    float xmax = 255;
    int i = 0;

    /* magenta -> blue, decrease red over 42 entries */
    slope = (xmax - xmax / 41f) / 41f;
    for (j = 0; j < 42; j++, i++) {
      b[i] = max;
      r[i] = (byte) (xmax - slope * j);
      g[i] = 0;
    }

    /* blue -> cyan, increase green over 42 entries */
    for (j = 0; j < 42; j++, i++) {
      b[i] = max;
      g[i] = (byte) (slope * j);
      r[i] = 0;
    }

    /* cyan -> white, increase red over 42 entries */
    for (j = 0; j < 42; j++, i++) {
      b[i] = max;
      g[i] = max;
      r[i] = (byte) (slope * j);
    }
    /* White in the middle */
    for (j = 0; j < 4; j++, i++) {
      b[i] = max;
      g[i] = max;
      r[i] = max;
    }
    /* white -> green, decrease red and blue over 42 entries */
    for (j = 0; j < 42; j++, i++) {
      g[i] = max;
      b[i] = r[i] = (byte) (xmax - slope * j);
    }

    /* green -> yellow, increase red over 42 entries */
    for (j = 0; j < 42; j++, i++) {
      g[i] = max;
      r[i] = (byte) (slope * j);
      b[i] = 0;
    }

    /* yellow -> red, decrease green over 42 entries */
    for (j = 1; j <= 42; j++, i++) {
      r[i] = max;
      g[i] = (byte) (xmax - slope * j);
      b[i] = 0;
    }
    return new IndexColorModel(8, 256, r, g, b);
  }

  public static IndexColorModel spectral() {
    int j;
    float slope;
    byte max = (byte) 0xff;
    float xmax = 255;
    int i = 0;

    /* magenta -> blue, decrease red over 51 entries */
    slope = (xmax - xmax / 50f) / 50f;
    for (j = 0; j < 51; j++, i++) {
      b[i] = max;
      r[i] = (byte) (xmax - slope * j);
      g[i] = 0;
    }

    /* blue -> cyan, increase green over 51 entries */
    for (j = 0; j < 51; j++, i++) {
      b[i] = max;
      g[i] = (byte) (slope * j);
      r[i] = 0;
    }

    /* cyan -> green, decrease blue over 51 entries */
    for (j = 0; j < 51; j++, i++) {
      g[i] = max;
      b[i] = (byte) (xmax - slope * j);
      r[i] = 0;
    }

    /* green -> yellow, increase red over 51 entries */
    for (j = 0; j < 51; j++, i++) {
      g[i] = max;
      r[i] = (byte) (slope * j);
      b[i] = 0;
    }

    /* yellow -> red, decrease green over 51 entries */
    for (j = 1; j <= 51; j++, i++) {
      r[i] = max;
      g[i] = (byte) (xmax - slope * j);
      b[i] = 0;
    }
    r[i] = max;
    g[i] = b[i] = 0;
    return new IndexColorModel(8, 256, r, g, b);
  }

}
