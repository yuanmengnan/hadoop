package info.xiaohei.www.mr.yearhot;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupHot extends WritableComparator {
    public GroupHot() {
        super(KeyPair.class, true);
    }

    public int compare(WritableComparable a, WritableComparable b) {
        KeyPair o1 = (KeyPair) a;
        KeyPair o2 = (KeyPair) b;
        System.out.println("分组比较：" + o1.toString() + "####" + o2.toString());
        return Integer.compare(o1.getYear(), o2.getYear());
    }
}