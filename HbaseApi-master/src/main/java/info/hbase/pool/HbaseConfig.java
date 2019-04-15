package info.hbase.pool;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import info.soft.utils.config.ConfigUtil;

/**
 * CDH5配置信息
 *
 * 默认配置在src/main/resources/hdfs/hbase.properties
 *
 * @author gy
 *
 */
public class HbaseConfig {

	private static final Properties PROPS = ConfigUtil.getProps("hbase.properties");

	/**
	 * 获取Hbase所需配置信息
	 */
	public static Configuration getHbaseConf() {
		// 在classpath下查找hbase-site.xml文件，如果不存在，则使用默认的hbase-core.xml文件
		Configuration config = HBaseConfiguration.create();
		// 加入Zookeeper配置，必选参数
		config.set("hbase.zookeeper.quorum", PROPS.getProperty("zookeeper.servers"));
		config.set("hbase.zookeeper.property.clientPort", PROPS.getProperty("zookeeper.port"));
		// 加入HBase配置，可选参数
		if (!PROPS.getProperty("hbase.master").isEmpty()) {
			config.set("hbase.master", PROPS.getProperty("hbase.master"));
		}
		if (!PROPS.getProperty("hbase.rootdir").isEmpty()) {
			config.set("hbase.rootdir", PROPS.getProperty("hbase.rootdir"));
		}

		return config;
	}

}
