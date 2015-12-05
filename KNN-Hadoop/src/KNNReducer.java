import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KNNReducer extends Reducer<Text, distanceLabel, Text, Text> {

  public void reduce(Text key, Iterable<distanceLabel> values, Context context) throws IOException,
          InterruptedException {
    int k = Integer.parseInt(context.getConfiguration().get("k"));
    distanceLabel sum = new distanceLabel();
    for (distanceLabel item : values) {
      Iterator<Entry<Double, String>> iter = item.entrySet().iterator();
      while (iter.hasNext()) {
        @SuppressWarnings("rawtypes")
        Map.Entry entry = (Map.Entry) iter.next();
        Double key1 = (Double) entry.getKey();
        String val = (String) entry.getValue();
        sum.put(key1, val);
      }
    }

    Set<Double> distances = sum.keySet();
    ArrayList<Double> sort = new ArrayList<Double>(distances);
    Collections.sort(sort);

    HashMap<String, Integer> vote = new HashMap<String, Integer>();
    for (int i = 0; i < k; i++) {
      Double dis = sort.get(i);
      String lab = sum.get(dis);
      if (!vote.containsKey(lab)) {
        vote.put(lab, 1);
      } else {
        vote.put(lab, vote.get(lab) + 1);
      }
    }

    String res;
    int v1 = vote.containsKey("versicolor") ? vote.get("versicolor") : 0;
    int v2 = vote.containsKey("setosa") ? vote.get("setosa") : 0;
    int v3 = vote.containsKey("virginica") ? vote.get("virginica") : 0;
    int max = Math.max(Math.max(v1, v2), v3);
    if (vote.containsKey("versicolor") && vote.get("versicolor") == max) {
      res = "versicolor";
    } else if (vote.containsKey("setosa") && vote.get("setosa") == max) {
      res = "setosa";
    } else {
      res = "virginica";
    }

    context.write(key, new Text(res));
  }
}
