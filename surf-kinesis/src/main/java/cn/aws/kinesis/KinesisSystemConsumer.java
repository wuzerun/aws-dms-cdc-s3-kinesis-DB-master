package cn.aws.kinesis;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.google.common.base.Preconditions;
import org.apache.samza.util.BlockingEnvelopeMap;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KinesisSystemConsumer extends BlockingEnvelopeMap {
    private static final String WORKER_ID_TEMPLATE = "kinesis-%s-%s";
    private ExecutorService executorService;
    private String topic;
    private AmazonKinesis client;
    private AmazonDynamoDB dynamoDB;
    private AmazonCloudWatch cloudWatch;
    private final List<Worker> workers = new LinkedList<Worker>();
    private final int mb = 1024 * 1024;
    public KinesisSystemConsumer(final String topic,
                                 final AmazonKinesis client,
                                 final AmazonDynamoDB dynamoDB,
                                 final AmazonCloudWatch cloudWatch) {
        this.topic = Preconditions.checkNotNull(topic, "A valid kinesis topic is required");
        this.client = Preconditions.checkNotNull(client, "AmazonKinesis is required");
        this.dynamoDB = Preconditions.checkNotNull(dynamoDB, "AmazonDynamoDB is required");
        this.cloudWatch = Preconditions.checkNotNull(cloudWatch, "AmazonCloudWatch is required");

    }


    @Override
    public void start() {
        System.out.println(String.format("Max memory: %s mb", Runtime.getRuntime().maxMemory() / mb));
        System.out.println("Starting up Kinesis Consumer... (may take a few seconds)");
        String appName = null;
        try {
            appName = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        executorService = Executors.newCachedThreadPool();
        Worker worker = new Worker.Builder()
                .recordProcessorFactory(new KinesisRecordProcessorFactory())
                .kinesisClient(client).dynamoDBClient(dynamoDB).cloudWatchClient(cloudWatch)
                .config(getKinesisClientLibConfig(appName)).build();
        workers.add(worker);
        executorService.execute(worker);
    }


     KinesisClientLibConfiguration getKinesisClientLibConfig(String appName){
        final String kinesisApplication = String.format(WORKER_ID_TEMPLATE, topic, appName);
        InitialPositionInStream startPos = InitialPositionInStream.LATEST;
        KinesisClientLibConfiguration kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration(kinesisApplication, topic, new DefaultAWSCredentialsProviderChain(), UUID.randomUUID().toString())
                        .withInitialPositionInStream(startPos);
        return kinesisClientLibConfiguration;
    }



    @Override
    public void stop() {
        System.out.println("Stop samza consumer for system " + topic);
        workers.forEach(Worker::shutdown);
        workers.clear();
        executorService.shutdownNow();
        System.out.println("Kinesis system consumer executor service for system " + topic + " is shutdown.");
    }
}
