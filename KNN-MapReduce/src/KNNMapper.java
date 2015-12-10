import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KNNMapper extends Mapper<Object, Text, Text, distanceLabel> {

  private static double getDistance(double[] p1, double[] p2) {
    double distance = 0;
    for (int i = 0; i < p1.length; i++) {
      distance += Math.pow(Math.abs(p1[i] - p2[i]), 2);
    }
    return Math.sqrt(distance);
  }

  // read the file and parse it as a line
  private static String fileReader(Context context) {
    String content = "";
    BufferedReader reader = null;
    try {
      FileSystem fs = FileSystem.get(context.getConfiguration());
      reader = new BufferedReader(new InputStreamReader(fs.open(new Path(context.getConfiguration()
              .get("train")))));
      String line;
      content += reader.readLine();
      while ((line = reader.readLine()) != null) {
        content = content + "\n" + line;
      }
      if (reader != null) {
        try {
          reader.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return content;
  }

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String test = value.toString();
    String[] testLines = test.split("\n");
    String[][] testData = new String[testLines.length][4];
    for (int i = 0; i < testLines.length; i++) {
      testData[i] = testLines[i].split(",");
    }

    String train = fileReader(context);
    String[] trainLines = train.split("\n");
    String[][] trainData = new String[trainLines.length][5];
    for (int i = 0; i < trainLines.length; i++) {
      trainData[i] = trainLines[i].split(",");
    }

    for (int i = 0; i < testLines.length; i++) {
      for (int j = 0; j < trainLines.length; j++) {
        double[] p1 = new double[4];
        double[] p2 = new double[4];
        for (int k = 0; k < 4; k++) {
          p1[k] = Double.parseDouble(testData[i][k]);
          p2[k] = Double.parseDouble(trainData[j][k]);
        }
        double distance = getDistance(p1, p2);
        distanceLabel item = new distanceLabel();
        item.put(distance, trainData[j][4]);
        context.write(new Text(testLines[i]), item);
      }
    }
  }
}
