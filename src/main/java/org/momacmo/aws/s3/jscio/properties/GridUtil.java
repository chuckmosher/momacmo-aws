package org.momacmo.aws.s3.jscio.properties;

import java.util.Arrays;

import org.javaseis.grid.GridDefinition;
import org.javaseis.properties.AxisLabel;

public class GridUtil {
  
  public static AxisLabel getAxisLabel( String axisName ) {
    AxisLabel label = AxisLabel.get(axisName);
    if (label != null) return label;
    return new AxisLabel( axisName, axisName );
  }
  
  public static GridDefinition standardGrid(int dataType, int[] lengths) {
    long[] lo = new long[4];
    long[] ld = new long[4];
    Arrays.fill(ld,1);
    double[] po = new double[4];
    double[] pd = new double[4];
    Arrays.fill(pd, 1);
    return GridDefinition.standardGrid(dataType, lengths, lo, ld, po, pd);
  }
  
  /**
   * Return grid values for a given set of index values
   * @param indexValues - input index values for each axis
   * @param gridValues - output grid values for each axis
   */
  public static void indexToGrid( GridDefinition grid, int[] indexValues, long[] gridValues ) {
    for (int i=0; i<grid.getNumDimensions(); i++) {
      gridValues[i] = (grid.getAxisLogicalOrigin(i) + grid.getAxisLogicalDelta(i) * indexValues[i]);
    }
  }
  
  public static boolean restrictToRange( GridDefinition grid, int axisIndex, long[] inRange, long[] outRange ) {
    long lstrt = grid.getAxisLogicalOrigins()[axisIndex];
    long linc = grid.getAxisLogicalDeltas()[axisIndex];
    long lend = lstrt + linc*(grid.getAxisLength(axisIndex) - 1);
    if (inRange[0] == 0 && inRange[1] == 0 && inRange[2] == 0) {
      outRange[0] = lstrt;
      outRange[1] = lend;
      outRange[2] = linc;
      return true;
    }
    if (inRange[0]%linc != 0) return false;
    if (inRange[0] > lend) return false;
    if (inRange[1] < inRange[0]) return false;
    if (inRange[1] < lstrt) return false;
    if (inRange[2] < linc || inRange[2]%linc != 0) return false;
    linc = outRange[2];
    outRange[0] = Math.max(lstrt, linc*(inRange[0]/linc));
    outRange[1] = Math.min(lend, linc*(inRange[1]/linc));
    return true;
  }
 
  public static long[] getRange( GridDefinition grid, int axisIndex ) {
    long lstrt = grid.getAxisLogicalOrigins()[axisIndex];
    long linc = grid.getAxisLogicalDeltas()[axisIndex];
    long lend = lstrt + linc*(grid.getAxisLength(axisIndex) - 1);
    return new long[] { lstrt, lend, linc };
  }
  
  public static int logicalToIndex( GridDefinition grid, int axisIndex, long logicalValue ) {
    long lstrt = grid.getAxisLogicalOrigins()[axisIndex];
    long linc = grid.getAxisLogicalDeltas()[axisIndex];
    return (int) ((logicalValue-lstrt)/linc);
  }
  
  public static String toString(GridDefinition grid) {
    return JsonUtil.toJsonString(grid);
  }
}
