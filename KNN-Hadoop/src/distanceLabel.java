import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;

@SuppressWarnings("serial")
public class distanceLabel extends HashMap<Double, String> implements Writable {

  @Override
  public void readFields(DataInput in) throws IOException {
    clear();
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      Double id = in.readDouble();
      String f = in.readUTF();
      put(id, f);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.size());
    for (Double s : this.keySet()) {
      out.writeDouble(s);
      out.writeUTF(get(s));
    }
  }
}
