package io.confluent.ps.pctools;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;

import io.confluent.parallelconsumer.offsets.OffsetDecodingError;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Slf4j
@Command(name = "kafkapcgroups", mixinStandardHelpOptions = true, version = "kafkapcgroups 1.0",
        description = "Prints highest seen offset and incomplete offsets for parallel consumer groups.")
class KafkaPcGroups implements Callable<Integer> {
    @Option(names = {"--bootstrap-server"}, required = true, description = "REQUIRED: The server(s) to connect to.")
    String bootstrap;

    @SuppressWarnings("DefaultAnnotationParam")
    @CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
    OneOrAll groupSelect;
    static class OneOrAll {
        @Option(names = {"--group"}, required = true, description = "The consumer group we wish to act on.")
        private String groupid;
        @Option(names = {"--all-groups"}, required = true, description = "Apply to all consumer groups.")
        boolean allgroups = false;
    }
    @Option(names = {"--command-config"}, description = "Property file containing configs to be passed to Admin Client and Consumer.")
    String configFile;
    @Option(names = {"--timeout"}, description = "The timeout that can be set for some" +
                                                 "use cases. For example, it can be " +
                                                 "used when describing the group to " +
                                                 "specify the maximum amount of time " +
                                                 "in milliseconds to wait before the " +
                                                 "group stabilizes (when the group is " +
                                                 "just created, or is going through " +
                                                 "some changes). (default: 15000)")
    Integer timeout = 15000;

    @Override
    public Integer call() throws Exception {
        Properties props = new Properties();
        if (!StringUtils.isBlank(configFile)) {
            props = loadConfig(configFile);
        }
        props.put("bootstrap.servers", bootstrap);
        try (AdminClient aclient = AdminClient.create(props)) {
            Set<String> groupsList = new HashSet<>();
            if (groupSelect.allgroups) {
                aclient.listConsumerGroups().all().get().forEach(item -> groupsList.add(item.groupId()));
            } else {
                groupsList.add(groupSelect.groupid);
            }
            var cgDescribe = aclient.describeConsumerGroups(groupsList,
                            new DescribeConsumerGroupsOptions().timeoutMs(timeout))
                    .describedGroups();
            for (var groupId : groupsList) {
                log.info("Concurrently processing group: {}", groupId);
                var cgInfo = aclient.listConsumerGroupOffsets(groupId,
                        new ListConsumerGroupOffsetsOptions().timeoutMs(timeout)).partitionsToOffsetAndMetadata().get();
                var members = cgDescribe.get(groupId).get().members();
                System.out.format("%-60s %-80s %-10s %-15s %-15s %-15s %-15s %-15s %-140s %-100s %-100s %-50s%n",
                        "GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "HIGHEST-OFFSET", "LOG-END-OFFSET",
                        "LAG", "ADJUSTED-LAG", "CONSUMER-ID", "HOST", "CLIENT-ID", "INCOMPLETE-ID");
                if (cgInfo.size() != 0) {
                    // with consumer info
                    for (var tp : cgInfo.entrySet()) {
                        OffsetAndMetadata offsetAndMetadata = tp.getValue();
                        Set<Long> incompleteOffsets = new HashSet<>();
                        Optional<Long> highestSeenOffset = Optional.empty();
                        if (offsetAndMetadata.metadata().length() > 0) {
                            OffsetMapCodecManager.HighestOffsetAndIncompletes highestOffsetAndIncompletes;
                            try {
                                highestOffsetAndIncompletes =
                                        OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(offsetAndMetadata.offset(), offsetAndMetadata.metadata());
                                highestSeenOffset = highestOffsetAndIncompletes.getHighestSeenOffset();
                                incompleteOffsets = highestOffsetAndIncompletes.getIncompleteOffsets();
                            } catch (OffsetDecodingError e) {
                                // invalid encoding of metadata not for PC
                                log.info("Group {} - decoding error - metadata is not valid or not from Parallel consumer.", groupId);
                            } catch (Exception e) {
                                log.info("Group {} - metadata is not valid or not from Parallel consumer.", groupId);
                            }
                        }
                        MemberDescription memberInfo = getMemberForPartion(members, tp.getKey());
                        for (var tpInfo : aclient.listOffsets(Map.of(tp.getKey(), OffsetSpec.latest()),
                                new ListOffsetsOptions().timeoutMs(timeout)).all().get().entrySet()) {
                            System.out.format("%-60s %-80s %-10s %-15s %-15s %-15s %-15s %-15s %-140s %-100s %-100s %-50s%n%n",
                                    groupId, tpInfo.getKey().topic(), tpInfo.getKey().partition(),
                                    offsetAndMetadata.offset(), highestSeenOffset.isEmpty() ? "-" : highestSeenOffset, tpInfo.getValue().offset(),
                                    tpInfo.getValue().offset() - offsetAndMetadata.offset(),
                                    highestSeenOffset.isEmpty() ? "-" : tpInfo.getValue().offset() - highestSeenOffset.get() - incompleteOffsets.size(),
                                    memberInfo != null ? StringUtils.defaultIfBlank(memberInfo.consumerId(), "-") : "-",
                                    memberInfo != null ? StringUtils.defaultIfBlank(memberInfo.host(), "-") : "-",
                                    memberInfo != null ? StringUtils.defaultIfBlank(memberInfo.clientId(), "-") : "-",
                                    StringUtils.defaultIfBlank(incompleteOffsets.toString(), "[]"));
                        }
                    }
                } else {
                    MemberDescription memberInfo = getMemberForPartion(members, null);
                    for (var member : members) {
                        for (var tp : member.assignment().topicPartitions() ) {
                            for (var tpInfo : aclient.listOffsets(Map.of(tp, OffsetSpec.latest()),
                                    new ListOffsetsOptions().timeoutMs(timeout)).all().get().entrySet()) {
                                System.out.format("%-60s %-80s %-10s %-15s %-15s %-15s %-15s %-15s %-140s %-100s %-100s %-50s%n%n",
                                        groupId, tpInfo.getKey().topic(), tpInfo.getKey().partition(),
                                        tpInfo.getValue().offset(), "-", tpInfo.getValue().offset(),
                                        "-",
                                        "-",
                                        memberInfo != null ? StringUtils.defaultIfBlank(memberInfo.consumerId(), "-") : "-",
                                        memberInfo != null ? StringUtils.defaultIfBlank(memberInfo.host(), "-") : "-",
                                        memberInfo != null ? StringUtils.defaultIfBlank(memberInfo.clientId(), "-") : "-",
                                        "[]");

                            }
                        }
                    }
                }
            }
        }
        return 0;
    }
    public static void main(String[] args) {
        int exitCode = new CommandLine(new KafkaPcGroups()).execute(args);
        System.exit(exitCode);
    }

    public static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }
    public static MemberDescription getMemberForPartion(Collection<MemberDescription> members, TopicPartition tp) {
        for (MemberDescription member : members){
            for (TopicPartition assignmentTp : member.assignment().topicPartitions()) {
                if ( tp == null || (assignmentTp.topic().equals(tp.topic()) && assignmentTp.partition() == tp.partition())) {
                    return member;
                }
            }
        }
        return null;
    }
}