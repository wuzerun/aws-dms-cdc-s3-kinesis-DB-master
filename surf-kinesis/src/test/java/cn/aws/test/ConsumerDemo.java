package cn.aws.test;

import cn.aws.kinesis.KinesisAWSCredentialsProvider;
import cn.aws.kinesis.KinesisSystemConsumer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

public class ConsumerDemo {
    private static final String WORKER_ID_TEMPLATE = "kinesis-%s-%s";
    private static final String regionName = "ap-southeast-1";
    private static final String topic = "ESCM_EEL-ESCMOWNER-SC_HD1";
    private static final String accessKey = "";
    private static final String secretKey = "";

    public static void main(String[] args) {
        AWSCredentialsProvider credentialsProvider = new AWSCredentialsProviderChain(new KinesisAWSCredentialsProvider(accessKey,secretKey), new DefaultAWSCredentialsProviderChain());
        AmazonKinesis client = AmazonKinesisClientBuilder.standard().withRegion(regionName).withCredentials(credentialsProvider).build();
        AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard().withRegion(regionName).withCredentials(credentialsProvider).build();
        AmazonCloudWatch cloudwatch = AmazonCloudWatchClientBuilder.standard().withRegion(regionName).withCredentials(credentialsProvider).build();
        KinesisSystemConsumer consumer = new KinesisSystemConsumer(topic, client, dynamoDB, cloudwatch);
        consumer.start();
    }

//    public static void main2(String[] args) {
//
//        KinesisClientLibConfiguration config = getKinesisClientLibConfig(appName);
//        AmazonKinesis client = AmazonKinesisClientBuilder.standard().withRegion(regionName).withClientConfiguration(config.getKinesisClientConfiguration()).build();
//        AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard().withRegion(regionName).withClientConfiguration(config.getDynamoDBClientConfiguration()).build();
//        AmazonCloudWatch cloudwatch = AmazonCloudWatchClientBuilder.standard().withRegion(regionName).withClientConfiguration(config.getCloudWatchClientConfiguration()).build();
//        KinesisSystemConsumer consumer = new KinesisSystemConsumer(topic, client, dynamoDB, cloudwatch);
//        consumer.start();
//    }

//    public static KinesisClientLibConfiguration getKinesisClientLibConfig(String appName){
//        final String kinesisApplication = String.format(WORKER_ID_TEMPLATE, topic, appName);
//        InitialPositionInStream startPos = InitialPositionInStream.LATEST;
//        AWSCredentialsProvider provider = credentialsProviderForStream();
//        System.out.println("provider: "+provider);
//        KinesisClientLibConfiguration kinesisClientLibConfiguration =
//                new KinesisClientLibConfiguration(kinesisApplication, topic, provider, UUID.randomUUID().toString())
//                        .withInitialPositionInStream(startPos);
//        return kinesisClientLibConfiguration;
//    }


}
