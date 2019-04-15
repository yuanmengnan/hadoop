package info.hbase.dao;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import info.hbase.api.UserInfoApi;
import info.hbase.api.WeiboUserApi;
import info.hbase.core.HbasePool;
import info.hbase.domain.User;
import info.hbase.http.HttpHandle;
import info.hbase.pool.HbaseConfig;
import info.hbase.pool.PoolConfig;
import info.hbase.utils.ConfigUtil;
import info.hbase.utils.SinaDomainUtils;
import info.soft.utils.http.ClientDao;
import info.weibo.api.domain.SinaDomain;

public class HbaseAdmin {
    private static final Logger logger = LoggerFactory.getLogger(HbaseAdmin.class);

    private Connection conn;

    private static HbaseAdmin sbi;

    private static ClientDao clientDao = new HttpHandle();

    public static Properties pros = ConfigUtil.getProps("app.properties");

    private static UserInfoApi userInfoDao = new UserInfoApi();

    public static HbaseAdmin getInstence() {

        sbi = new HbaseAdmin();
        /* 连接池配置 */
        PoolConfig config = new PoolConfig();
        config.setMaxTotal(20);
        config.setMaxIdle(5);
        config.setMaxWaitMillis(1000);
        config.setTestOnBorrow(true);

        /* Hbase配置 */
        Configuration hbaseConfig = HbaseConfig.getHbaseConf();
        /* 初始化连接池 */
        HbasePool pool = new HbasePool(config, hbaseConfig);
        /* 从连接池中获取对象 */
        sbi.conn = pool.getConnection();
        logger.info("hbase连接已建立");
        return sbi;
    }

    public void createTable(String tableName, String[] columnFamilys, short regionCount,
            long regionMaxSize) throws IOException {

        /* 获取Admin对象 */
        try (Admin admin = conn.getAdmin();) {
            boolean booleansExisted = admin.tableExists(TableName.valueOf(tableName));
            boolean isExisted = admin.isTableAvailable(TableName.valueOf(tableName));
            if (!booleansExisted || !isExisted) {
                // 创建数据表描述器
                HTableDescriptor tableDescriptor =
                        new HTableDescriptor(TableName.valueOf(tableName));
                // 设置并添加列族描述器
                for (String columnFamily : columnFamilys) {
                    HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);
                    // 数据压缩算法
                    // columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
                    // 块大小
                    columnDescriptor.setBlocksize(256 * 1024);
                    // 布隆过滤器类型
                    columnDescriptor.setBloomFilterType(BloomType.ROW);
                    tableDescriptor.addFamily(columnDescriptor);
                }
                // 设置最大文件大小
                tableDescriptor.setMaxFileSize(regionMaxSize);
                // 根据最大常量值设置分区策略
                tableDescriptor.setValue(HTableDescriptor.SPLIT_POLICY,
                        ConstantSizeRegionSplitPolicy.class.getName());

                // 分区数和范围
                regionCount = (short) Math.abs(regionCount);
                int regionRange = Short.MAX_VALUE / regionCount;
                // 分区key
                int counter = 0;
                byte[][] splitKeys = new byte[regionCount][];
                for (int i = 0; i < splitKeys.length; i++) {
                    counter = counter + regionRange;
                    String key = StringUtils.leftPad(Integer.toString(counter), 5, '0');
                    splitKeys[i] = Bytes.toBytes(key);
                    logger.info(" - Split: " + i + " '" + key + "'");
                }
                admin.createTable(tableDescriptor, splitKeys);
            }
        }
    }

    public void createTable(String tableName, String[] columnFamilys) {

        /* 获取Admin对象 */
        try (Admin admin = conn.getAdmin();) {
            boolean booleansExisted = admin.tableExists(TableName.valueOf(tableName));
            boolean isExisted = admin.isTableAvailable(TableName.valueOf(tableName));
            if (!booleansExisted || !isExisted) {
                // 创建数据表描述器
                HTableDescriptor tableDescriptor =
                        new HTableDescriptor(TableName.valueOf(tableName));
                // 设置并添加列族描述器
                for (String columnFamily : columnFamilys) {
                    HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);
                    tableDescriptor.addFamily(columnDescriptor);
                }

                admin.createTable(tableDescriptor);
            }
        } catch (Exception e) {

        }

    }

    /**
     * 添加数据
     * 
     * @param connect
     * @param rowKey
     * @param tableName
     * @param column1
     * @param value1
     * @param column2
     * @param value2
     * @throws IOException
     */
    public void addData(String rowKey, String tableName, String[] column1, String[] value1)
            throws IOException {

        TableName tn = TableName.valueOf(tableName);// HTabel负责跟记录相关的操作如增删改查等//
        Table table = conn.getTable(tn); // 获取表
        Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor() // 获取所有的列族
                .getColumnFamilies();
        for (int i = 0; i < columnFamilies.length; i++) {
            String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
            if (familyName.equals("user_info")) { // user_info列族put数据
                for (int j = 0; j < column1.length; j++) {
                    put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column1[j]),
                            Bytes.toBytes(value1[j]));
                }
            }
            table.put(put);
        }
    }

    /**
     * 查询数据
     * 
     * @throws IOException
     */
    public User getUser(String tableName, String rowkey, String colFamily, String col)
            throws IOException {

        TableName tn = TableName.valueOf(tableName);// HTabel负责跟记录相关的操作如增删改查等//
        Table table = conn.getTable(tn);
        Get get = new Get(Bytes.toBytes(rowkey));
        // 获取指定列族数据
        if (colFamily != null) {
            // 获取指定列数据
            if (col != null) {
                get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
            } else
                get.addFamily(Bytes.toBytes(colFamily));
        }
        Result result = table.get(get);
        try {
            if (!result.isEmpty())
                return showCell(result);
            else {
                return httpGetUer(rowkey);
            }
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            return null;
        }
    }

    public static User showCell(Result result) throws ParseException {

        Cell[] cells = result.rawCells();
        SimpleDateFormat sdf =
                new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", java.util.Locale.US);
        User user = new User();
        for (Cell cell : cells) {
            String ucell = new String(CellUtil.cloneQualifier(cell));

            String uvalue = new String(CellUtil.cloneValue(cell));
            if ("avatar_large".equals(ucell)) {
                user.setAvatar_large(uvalue);
                continue;
            } else if ("bi_followers_count".equals(ucell)) {
                user.setBi_followers_count(Integer.parseInt(uvalue));
                continue;
            } else if ("city".equals(ucell)) {
                user.setCity(Integer.parseInt(uvalue));
                continue;
            } else if ("created_at".equals(ucell)) {
                System.out.println(sdf.parse(uvalue));
                user.setCreated_at(sdf.parse(uvalue));
                continue;
            } else if ("description".equals(ucell)) {
                user.setDescription(uvalue + "");
                continue;
            } else if ("domain".equals(ucell)) {
                user.setDomain(uvalue);
                continue;
            } else if ("favourites_count".equals(ucell)) {
                user.setFavourites_count(Integer.parseInt(uvalue));
                continue;
            } else if ("followers_count".equals(ucell)) {
                user.setFollowers_count(Integer.parseInt(uvalue));
                continue;
            } else if ("friends_count".equals(ucell)) {
                user.setFriends_count(Integer.parseInt(uvalue));
                continue;
            } else if ("gender".equals(ucell)) {
                user.setGender(uvalue);
                continue;
            } else if ("id".equals(ucell)) {
                user.setId(Long.parseLong(uvalue));
                continue;
            } else if ("lasttime".equals(ucell)) {
                user.setLasttime(sdf.parse(uvalue));
                continue;
            } else if ("location".equals(ucell)) {
                user.setLocation(uvalue);
                continue;
            } else if ("name".equals(ucell)) {
                user.setName(uvalue);
                continue;
            } else if ("profile_image_url".equals(ucell)) {
                user.setProfile_image_url(uvalue);
                continue;
            } else if ("province".equals(ucell)) {
                user.setProvince(Integer.parseInt(uvalue));
                continue;
            } else if ("remark".equals(ucell)) {
                user.setRemark(uvalue);
                continue;
            } else if ("screen".equals(ucell)) {
                user.setScreen_name(uvalue);
                continue;
            } else if ("verified".equals(ucell)) {
                user.setVerified(uvalue.equals("true") ? true : false);
                continue;
            } else if ("verified_reason".equals(ucell)) {
                user.setVerified_reason(uvalue);
                continue;
            } else if ("verified_type".equals(ucell)) {
                user.setVerified_type(Integer.parseInt(uvalue));
                continue;
            } else if ("weihao".equals(ucell)) {
                user.setWeihao(uvalue);
            }
        }
        return user;
    }

    private static User httpGetUer(String rowkey){
        WeiboUserApi api=new  WeiboUserApi(clientDao);
        SinaDomain sinaDomain= api.usersShow(rowkey, pros.getProperty("cookie"), pros.getProperty("source"));
        User user=null;
        Gson gson=new Gson(); 
        if(!sinaDomain.containsKey("error")){
            user=SinaDomainUtils.getUser(sinaDomain);
            userInfoDao.post2Hbase(rowkey, gson.toJson(user));
        }
        return user;
    }

}
