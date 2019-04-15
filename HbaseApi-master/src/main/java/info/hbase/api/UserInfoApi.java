package info.hbase.api;


import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import info.hbase.dao.HbaseAdmin;
import info.hbase.domain.User;
import info.soft.utils.config.ConfigUtil;

public class UserInfoApi {
   public static void main(String[] args) {
        UserInfoApi u=new UserInfoApi();
        String s=u.getUser("sina_user_baseinfo", "5509619713").toString();
        System.out.println(s);
    }

    private static final Logger logger = LoggerFactory.getLogger(UserInfoApi.class);

    private static HbaseAdmin ha = HbaseAdmin.getInstence();

    private static Properties pros = ConfigUtil.getProps("app.properties");

    /**
     * 添加数据
     * 
     * @param rowKey
     * @param tableName
     * @param familyCol
     * @param values
     */
    public void post2Hbase(String rowKey, String tableName, String[] familyCol, String[] values) {

       try {
            
           ha.addData(rowKey, tableName, familyCol, values);
            logger.info("insert hbase successed!");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            logger.error("post data error :{}", e.getMessage());
        }
    }

    public void post2Hbase(String rowkey, String[] familyCol, String[] values) {
        post2Hbase(rowkey, pros.getProperty("tablename"), familyCol, values);
    }

    public void post2Hbase(String rowKey, String values) {
        Gson gson=new Gson();
        User user=gson.fromJson(values, User.class);
        String[] value = {user.getId() + "", user.getScreen_name(), user.getName(),
        user.getProvince() + "", user.getCity() + "", user.getLocation(),
        user.getDescription(), user.getUrl(), user.getProfile_image_url(),
        user.getDomain(), user.getGender(), user.getFollowers_count() + "",
        user.getFriends_count() + "", user.getStatuses_count() + "",
        user.getFavourites_count() + "", user.getCreated_at() + "",
        user.isVerified() + "", user.getVerified_type() + "",
        user.getAvatar_large() + "", user.getBi_followers_count() + "",
        user.getRemark(), user.getVerified_reason(), user.getWeihao(),
        user.getLasttime() + ""};
        post2Hbase(rowKey, pros.getProperty("columns").split(","), value);

    }

    /**
     * 查询数据
     */
    public User getUser(String tableName, String rowKey, String familyCol, String col) {
       try {
            return ha.getUser(tableName, rowKey, familyCol, col);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            logger.error("search data error :{}",e.getMessage());
            return null;
        }
    }
    public User getUser(String tableName, String rowKey){
        return getUser(tableName,rowKey,null,null);
    }
    
    public User getUser(String tableName, String rowKey, String familyCol){
        return getUser(tableName,rowKey,familyCol,null);
    }
}
