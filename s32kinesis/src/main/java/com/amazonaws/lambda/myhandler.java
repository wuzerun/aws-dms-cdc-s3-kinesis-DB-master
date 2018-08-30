package com.amazonaws.lambda;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;


public class myhandler implements RequestHandler<S3Event, String> {
    private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

    private static AmazonKinesis kinesis;

    public myhandler() {
    }

    public myhandler(AmazonS3 s3) {
        this.s3 = s3;
    }

    public String handleRequest(S3Event input, Context context) {
        context.getLogger().log("Received input: " + input);

        AmazonS3Client s3Client = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());

        // String holder for each line is CSV file
        String line = "String holder for each line is CSV file";

        for (S3EventNotification.S3EventNotificationRecord record : input.getRecords()) {
            String s3Key = record.getS3().getObject().getKey();
            String s3Bucket = record.getS3().getBucket().getName();
            context.getLogger().log("================CSV1: " + line);
            context.getLogger().log("found id: " + s3Bucket + " " + s3Key);
            try {
                S3Object object = s3Client.getObject(new GetObjectRequest(s3Bucket, s3Key));
                InputStreamReader isr = new InputStreamReader(object.getObjectContent());
                BufferedReader br = new BufferedReader(isr);

                while ((line = br.readLine()) != null) {
                    long createTime = System.currentTimeMillis();
                    context.getLogger().log("================CSV2: " + line);

                    kinesis = AmazonKinesisClientBuilder.standard().withRegion("ap-southeast-2").build();
                    String streamName = System.getenv("KinesisStream");
                    kinesis.putRecord(streamName, ByteBuffer.wrap(line.getBytes()), String.format("partitionKey-%d", createTime));
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
                context.getLogger().log(String.format("Error getting object %s from bucket %s. Make sure they exist and"
                        + " your bucket is in the same region as this function.", s3Key, s3Bucket));
                throw e;
            }

        }
        return "success";
    }
}
