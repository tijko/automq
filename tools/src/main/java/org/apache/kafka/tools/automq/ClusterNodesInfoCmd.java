/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package org.apache.kafka.tools.automq;

import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.tools.TerseException;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * get cluster nodes info
 */
public class ClusterNodesInfoCmd {

    private final Parameter parameter;
    private int leaderId;
    private String[] servers;
    public ClusterNodesInfoCmd(Parameter parameter) {
        this.parameter = parameter;
    }

    static class Parameter {
        final String serverList;
        Parameter(Namespace res) {
//            this.serverList = res.getString("--bootstrap-server");
            this.serverList = res.getString("bootstrap-server");
        }
    }

    public static ArgumentParser addArguments(Subparser parser) {

        parser.addArgument("cluster-nodes-info")
            .action(store())
            .required(true);
        parser.addArgument("--bootstrap-server")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("bootstrap-server")
            .metavar("BOOTSTRAP-SERVER")
            .help("The list of servers in the cluster. The format is 'host1:port1,host2:port2,...'.");
        return parser;
    }


public String run() throws ExecutionException, InterruptedException {
    System.out.println("##################### cluster-nodes-info ######################");
    Properties props = initializeProperties();

    try (AdminClient adminClient = AdminClient.create(props)) {
        QuorumInfo quorumInfo = getQuorumInfo(adminClient);
        this.leaderId = quorumInfo.leaderId();
        List<Integer> controllers = getControllers(quorumInfo);
        DescribeClusterResult clusterResult = adminClient.describeCluster();
        Set<String> topics = getAllTopics(adminClient);
        int partitionCount = countTopic(adminClient, topics);
        String clusterId = getClusterId(clusterResult);
        printClusterInfo(clusterId, topics, partitionCount);
        printNodesInfo(clusterResult, controllers);

    } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
        throw e;
    }
    return "ClusterNodesInfoCmd";
}

    private Properties initializeProperties() {
        Properties props = new Properties();
        this.servers = parameter.serverList.split(",");
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servers[0]);
        // TODO 增加配置文件
        // Additional configuration can be added here if needed
        // props.put(AdminClientConfig.CLIENT_ID_CONFIG, "ClusterNodesInfoCmd");
        return props;
    }

    private QuorumInfo getQuorumInfo(AdminClient adminClient) throws ExecutionException, InterruptedException {
        return adminClient.describeMetadataQuorum().quorumInfo().get();
    }

    private List<Integer> getControllers(QuorumInfo quorumInfo) {
        return quorumInfo.voters().stream()
                .map(v -> v.replicaId())
                .collect(Collectors.toList());
    }

    private String getClusterId(DescribeClusterResult clusterResult) throws ExecutionException, InterruptedException {
        return clusterResult.clusterId().get();
    }

    private void printClusterInfo(String clusterId, Set<String> topics, int partitionCount) {
        System.out.println("Cluster ID: " + clusterId);
        System.out.println("Cluster Topics: " + topics.size());
        System.out.println("Cluster Partitions: " + partitionCount);
    }

    private void printNodesInfo(DescribeClusterResult clusterResult, List<Integer> controllers) throws ExecutionException, InterruptedException {
        for (Node node : clusterResult.nodes().get()) {
            String role = determineRole(node, controllers);
            printNodeInfo(node, role);
        }

    }

    private String determineRole(Node node, List<Integer> controllers) {
        if (controllers.contains(node.id())) {
            return "CONTROLLER";
        } else if (node.id() == leaderId) {
            return "LEADER";
        } else {
            return "BROKER";
        }
    }

    private void printNodeInfo(Node node, String role) {
        String base = String.format("[%d]%s:%d(%s)", node.id(), node.host(), node.port(), role);
        System.out.println(base + " automq.version=?");
        System.out.println(base + " kafka.version=" + AppInfoParser.getVersion());
        System.out.println(base + " Leader=" + leaderId);
        System.out.println(base + " Rock=" + node.rack());
    }

    // TODO 配置文件
    private static Properties getProperties(File optionalCommandConfig) throws TerseException, IOException {
        if (optionalCommandConfig == null) {
            return new Properties();
        } else {
            if (!optionalCommandConfig.exists())
                throw new TerseException("Properties file " + optionalCommandConfig.getPath() + " does not exists!");
            return Utils.loadProps(optionalCommandConfig.getPath());
        }
    }

    // TODO 具体状态
    private static void handleDescribeStatus(Admin admin) throws ExecutionException, InterruptedException {
        String clusterId = admin.describeCluster().clusterId().get();
        QuorumInfo quorumInfo = admin.describeMetadataQuorum().quorumInfo().get();
        int leaderId = quorumInfo.leaderId();
        QuorumInfo.ReplicaState leader = quorumInfo.voters().stream().filter(voter -> voter.replicaId() == leaderId).findFirst().get();
        QuorumInfo.ReplicaState maxLagFollower = quorumInfo.voters().stream().min(Comparator.comparingLong(qi -> qi.logEndOffset())).get();
        long maxFollowerLag = leader.logEndOffset() - maxLagFollower.logEndOffset();

        long maxFollowerLagTimeMs;
        if (leader == maxLagFollower)
            maxFollowerLagTimeMs = 0;
        else if (leader.lastCaughtUpTimestamp().isPresent() && maxLagFollower.lastCaughtUpTimestamp().isPresent()) {
            maxFollowerLagTimeMs = leader.lastCaughtUpTimestamp().getAsLong() - maxLagFollower.lastCaughtUpTimestamp().getAsLong();
        } else {
            maxFollowerLagTimeMs = -1;
        }

        List<Integer> voterReplicaIds = quorumInfo.voters().stream()
                .map(v -> v.replicaId())
                .collect(Collectors.toList());

        System.out.println(voterReplicaIds);
        System.out.println(
                "ClusterId:              " + clusterId +
                        "\nLeaderId:               " + quorumInfo.leaderId() +
                        "\nLeaderEpoch:            " + quorumInfo.leaderEpoch() +
                        "\nHighWatermark:          " + quorumInfo.highWatermark() +
                        "\nMaxFollowerLag:         " + maxFollowerLag +
                        "\nMaxFollowerLagTimeMs:   " + maxFollowerLagTimeMs +
                        "\nCurrentVoters:          " + Utils.mkString(quorumInfo.voters().stream().map(v -> v.replicaId()), "[", "]", ",") +
                        "\nCurrentObservers:       " + Utils.mkString(quorumInfo.observers().stream().map(v -> v.replicaId()), "[", "]", ",")
        );
    }

    private Set<String> getAllTopics(AdminClient adminClient) throws ExecutionException, InterruptedException {
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
        ListTopicsResult listTopicsResult = adminClient.listTopics(options);
        return listTopicsResult.names().get();
    }

    private int countTopic(AdminClient adminClient, Set<String> topics) throws ExecutionException, InterruptedException {
        int partitionCount = 0;
        for (String topic : topics) {
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(topic));
            Map<String, TopicDescription> topicDescriptions = describeTopicsResult.allTopicNames().get();
            partitionCount += topicDescriptions.get(topic).partitions().size();
        }
        return partitionCount;
    }

}
