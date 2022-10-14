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
import picocli.CommandLine.Parameters;

@Slf4j
@Command(name = "kafkapcgroups", mixinStandardHelpOptions = true, version = "kafkapcgroups 1.0",
        description = "Prints highest seen offset and incomplete offsets for parallel consumer groups.")
class KafkaPcGroups implements Callable<Integer> {
    @Parameters(paramLabel = "<group>", description = "Consumer group ID.")
    private String groupid;
    @Option(names = {"--bootstrap-server"}, required = true, description = "REQUIRED: The server(s) to connect to.")
    String bootstrap;
//    @Option(names = {"--all-groups"}, description = "Apply to all consumer groups.")
//    boolean allgroups = false;
    @Option(names = {"--command-config"}, description = "Property file containing configs to be passed to Admin Client and Consumer.")
    String configFile;
    @Option(names = {"--timeout"}, description = "The timeout that can be set for some\n" +
            "                                          use cases. For example, it can be\n" +
            "                                          used when describing the group to\n" +
            "                                          specify the maximum amount of time\n" +
            "                                          in milliseconds to wait before the\n" +
            "                                          group stabilizes (when the group is\n" +
            "                                          just created, or is going through\n" +
            "                                          some changes). (default: 15000)")
    Integer timeout = 15000;

    @Override
    public Integer call() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        if (!StringUtils.isBlank(configFile)) {
            props = loadConfig(configFile);
        }
        AdminClient aclient = AdminClient.create(props);
        log.info("Concurrently processing group: {}", groupid);
        var cgInfo = aclient.listConsumerGroupOffsets(groupid,
                        new ListConsumerGroupOffsetsOptions().timeoutMs(timeout))
                        .partitionsToOffsetAndMetadata().get();
        var cgDescribe = aclient.describeConsumerGroups(Set.of(groupid),
                        new DescribeConsumerGroupsOptions().timeoutMs(timeout))
                .describedGroups().get(groupid).get();
        for (var tp : cgInfo.entrySet()) {
            OffsetAndMetadata offsetAndMetadata = tp.getValue();
            Set<Long> incompleteOffsets = new HashSet<>();
            Optional<Long> highestSeenOffset = Optional.empty();
            if (offsetAndMetadata.metadata().length()>0) {
                OffsetMapCodecManager.HighestOffsetAndIncompletes highestOffsetAndIncompletes;
                try {
                    highestOffsetAndIncompletes =
                            OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(offsetAndMetadata.offset(), offsetAndMetadata.metadata());
                    highestSeenOffset = highestOffsetAndIncompletes.getHighestSeenOffset();
                    incompleteOffsets = highestOffsetAndIncompletes.getIncompleteOffsets();
                } catch (OffsetDecodingError e) {
                    // invalid encoding of metadata not for PC
                    log.info("Group {} - decoding error - metadata is not valid or not from Parallel consumer.", groupid);
                } catch (Exception e) {
                    log.info("Group {} - metadata is not valid or not from Parallel consumer.", groupid);
                }
            }
            MemberDescription memberInfo = getMemberForPartion(cgDescribe.members(), tp.getKey());
            System.out.format("%-50s %-50s %-10s %-15s %-15s %-15s %-15s %-15s %-100s %-50s %-50s %-50s\n",
                    "GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "HIGHEST-OFFSET", "LOG-END-OFFSET",
                    "LAG", "ADJUSTED-LAG", "CONSUMER-ID", "HOST", "CLIENT-ID", "INCOMPLETE-ID");
            for ( var tpInfo : aclient.listOffsets(Map.of(tp.getKey(),OffsetSpec.latest()),
                      new ListOffsetsOptions().timeoutMs(timeout)).all().get().entrySet()) {
                System.out.format("%-50s %-50s %-10s %-15s %-15s %-15s %-15s %-15s %-100s %-50s %-50s %-50s\n",
                                groupid, tpInfo.getKey().topic(),tpInfo.getKey().partition(),
                                offsetAndMetadata.offset(), highestSeenOffset.orElse(tpInfo.getValue().offset()), tpInfo.getValue().offset(),
                                tpInfo.getValue().offset() - offsetAndMetadata.offset(),
                                tpInfo.getValue().offset() - highestSeenOffset.orElse(tpInfo.getValue().offset()) - incompleteOffsets.size(),
                                memberInfo != null ? StringUtils.defaultIfBlank(memberInfo.consumerId(),"-") : "-",
                                memberInfo != null ? StringUtils.defaultIfBlank(memberInfo.host(), "-") : "-",
                                memberInfo != null ? StringUtils.defaultIfBlank(memberInfo.clientId(),"-") : "-",
                                StringUtils.defaultIfBlank(incompleteOffsets.toString(), "[]"));
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
                if (assignmentTp.topic().equals(tp.topic()) && assignmentTp.partition() == tp.partition()) {
                    return member;
                }
            }
        }
        return null;
    }
}
