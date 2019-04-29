package com.hwx.consumer;

import kafka.admin.AdminClient;
import kafka.coordinator.GroupOverview;
import org.apache.kafka.common.Node;
import scala.collection.JavaConversions;
import scala.collection.immutable.List;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class ConsumerManager {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "10.121.198.133:9092");
        AdminClient adminClient = AdminClient.create(properties);
        Map<Node, List<GroupOverview>> groups = JavaConversions.mapAsJavaMap(adminClient.listAllConsumerGroups());

        for (Map.Entry<Node, List<GroupOverview>> entry : groups.entrySet()) {
            Iterator<GroupOverview> groupOverviewIterator = JavaConversions.asJavaIterator(entry.getValue().iterator());
            while (groupOverviewIterator.hasNext()){
                GroupOverview next = groupOverviewIterator.next();
                System.out.println(next.groupId());
                System.out.println(next.toString());
            }
        }
        adminClient.close();

    }
}
