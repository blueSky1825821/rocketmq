/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.simple;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;

public class PullConsumer {
    private static final Map<MessageQueue, Long> OFFSE_TABLE = new HashMap<MessageQueue, Long>();

    public static void main(String[] args) throws MQClientException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("please_rename_unique_group_name_5");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.start();

        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("broker-a");
        for (MessageQueue mq : mqs) {
            System.out.printf("Consume from the queue: %s%n", mq);
            SINGLE_MQ:
            while (true) {
                try {
                    PullResult pullResult =
                        consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
                    System.out.printf("%s%n", pullResult);
                    putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            break;
                        case NO_MATCHED_MSG:
                            break;
                        case NO_NEW_MSG:
                            break SINGLE_MQ;
                        case OFFSET_ILLEGAL:
                            break;
                        default:
                            break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        consumer.shutdown();
    }

    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = OFFSE_TABLE.get(mq);
        if (offset != null)
            return offset;

        return 0;
    }

    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        OFFSE_TABLE.put(mq, offset);
    }

}

/**
 * 手动实现负载均衡
 *
 * public class Test2 {
 *     public static void main(String[] args) throws MQClientException, InterruptedException {
 *         String namesrvAddr = "";
 *         //消费组
 *         DefaultMQPullConsumer pullConsumer = new DefaultMQPullConsumer("GroupName1");
 *         //MQ NameService地址
 *         pullConsumer.setNamesrvAddr(namesrvAddr);
 *         //负载均衡模式
 *         pullConsumer.setMessageModel(MessageModel.CLUSTERING);
 *
 *         //需要处理的消息topic
 *         pullConsumer.start();
 *
 *
 *         while (true) {
 *             boolean waiting = true;
 *             Set<MessageQueue> mqs = pullConsumer.fetchMessageQueuesInBalance("TopicTest1");
 *             //未获取到负载均衡的时候，等待1S重新获取
 *             if (!CollectionUtils.isEmpty(mqs)) {
 *                 waiting = false;
 *                 Thread.sleep(1000L);
 *             }
 *             for (MessageQueue mq : mqs) {
 *                 System.out.printf("Consume from the queue: " + mq + "%n");
 *                 SINGLE_MQ:
 *                 while (true) {
 *                     long offset = pullConsumer.fetchConsumeOffset(mq, false);
 *                     try {
 *                         PullResult pullResult =
 *                                 pullConsumer.pullBlockIfNotFound(mq, null, offset, 32);  //遍历所有queue，挨个调用pull
 *
 *                         System.out.printf("%s%n", pullResult);
 *                         switch (pullResult.getPullStatus()) {
 *                             case FOUND:
 *                                 offset = pullResult.getNextBeginOffset();
 *                                 pullConsumer.updateConsumeOffset(mq, offset);
 *                                 break;
 *                             case NO_MATCHED_MSG:
 *                                 break;
 *                             case NO_NEW_MSG:
 *                                 break SINGLE_MQ;
 *                             case OFFSET_ILLEGAL:
 *                                 break;
 *                             default:
 *                                 break;
 *                         }
 *                     } catch (Exception e) {
 *                         e.printStackTrace();
 *                     }
 *                 }
 *             }
 *             if(waiting){
 *                 Thread.sleep(100L);
 *             }
 *         }
 *     }
 * }
 */
