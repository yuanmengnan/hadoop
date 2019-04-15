package info.hbase.domain;


import java.util.ArrayList;
import java.util.List;

/**
 * 地理信息模型
 * 
 * @author gy
 * 
 */
public class Geo {

    /**
     * 类型
     */
    private String type;

    /**
     * 坐标数组
     */
    private List<String> coordinates=new ArrayList<>();

    public Geo() {
        super();
    }

    public Geo(String type, List<String> coordinates) {
        super();
        this.type = type;
        this.coordinates = coordinates;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<String> getCoordinates() {
        return coordinates;
    }

    public void setCoordinates(List<String> coordinates) {
        this.coordinates = coordinates;
    }

    @Override
    public String toString() {
        if (null != coordinates && 2 == coordinates.size()) {
            return coordinates.get(0) + "," + coordinates.get(1);
        }
        return "";
    }

}