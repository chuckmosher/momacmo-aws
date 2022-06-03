package org.momacmo.aws.lambda.tools.jsctoimage;

import org.apache.commons.codec.binary.Base64;
import java.awt.image.BufferedImage;
import java.awt.image.IndexColorModel;
import java.awt.image.WritableRaster;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

public class FloatToImage {

  IndexColorModel model;
  WritableRaster raster;
  BufferedImage bim;
  int n0, n1;
  float scaleMin, scaleMax;
  String format;
  
  public FloatToImage( DisplayColorModel.ColorModel colorModel, int width, int height, float min, float max ) {
    model = DisplayColorModel.getColorModel(colorModel);
    raster = model.createCompatibleWritableRaster(width, height);
    n0 = width;
    n1 = height;
    scaleMin = min;
    scaleMax = max;
    bim = new BufferedImage(model, raster, model.isAlphaPremultiplied(), null); 
    format = "jpg";
  }
  
  public boolean setFormat(String newFormat ) {
    switch (newFormat) {
      case "gif":
      case "GIF":
        format = "gif";
        return true;
      case "jpg":
      case "JPG":
        format = "jpg";
        return true;
      case "bmp":
      case "BMP":
        format = "bmp";
        return true;
      case "png":
      case "PNG":
        format = "png";
        return true;
      default:
        format = "jpg";
        return false;
    }       
  }
  
  public String getFormat() {
    return format;
  }
  
  public void putFloats( float[][] f ) {
    float rng = scaleMax-scaleMin;
    float scale = (rng > 0 ? 255/rng : 1);
    int[] cmp = new int[model.getNumColorComponents()];
    for (int j=0; j<n1; j++) {
      for (int i=0; i<n0; i++) {
        float val = f[j][i];
        if (val < scaleMin) {
          model.getComponents(0, cmp, 0);
        } else if (val > scaleMax) {
          model.getComponents(255, cmp, 0);
        } else {
          int ival = Math.round(scale*(val - scaleMin));
          model.getComponents(ival, cmp, 0);
        }
        raster.setPixel(i, j, cmp);
      }
    }
  }
  
  public WritableRaster getRaster() {
    return raster;
  }
   
  public BufferedImage getBufferedImage() {
    return bim;
  }
  
  public String getBase64Image() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      ImageIO.write(bim, format, baos);
    } catch (IOException e) {
      throw new IllegalStateException("Could not convert BufferedImage to bytes",e.getCause());
    }
    byte[] bytes = baos.toByteArray();
    return Base64.encodeBase64String(bytes);
  }
  
  public void writeImage(String path, float[][] f) throws IOException {
    putFloats(f);
    writeImage(path);
  }
  
  public void writeImage(String path) throws IOException {
    File f = new File(path);
    ImageIO.write(bim, format, f);
  }
}
