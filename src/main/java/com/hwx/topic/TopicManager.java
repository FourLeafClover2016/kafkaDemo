package com.hwx.topic;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class TopicManager {
    public static void main(String[] args) {
        // 会话超时时间和连接超时时间设置30秒
        ZkUtils zkUtils = ZkUtils.apply("10.121.198.133", 3000, 3000, JaasUtils.isZkSecurityEnabled());
        //查看topic是否存在
        if (!AdminUtils.topicExists(zkUtils, "t2")) {
            // 分区数为3,副本数为1
            AdminUtils.createTopic(zkUtils, "t2", 3, 1, new Properties(), RackAwareMode.Enforced$.MODULE$);
        }

        if (AdminUtils.topicExists(zkUtils, "t1")) {
            //删除topic t1
            AdminUtils.deleteTopic(zkUtils, "t1");
        }

        //查询topic级别参数
        Properties properties = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "t2");
        Iterator<Map.Entry<Object, Object>> iterator = properties.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Object, Object> entry = iterator.next();
            System.out.println(entry.getKey() + "-----" + entry.getValue());
        }

        // 改变topic的参数
        properties.put("min.cleanable.dirty.ratio", "0.3");
        properties.remove("max.message.types");
        AdminUtils.changeTopicConfig(zkUtils, "t2", properties);


        zkUtils.close();
    }
}
