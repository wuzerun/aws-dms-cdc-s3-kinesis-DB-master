package com.amazonaws.lambda;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.ListStreamsResult;

import java.util.List;

public class Client {
    private static AmazonKinesis kinesis;

    private static void init() throws Exception {
        ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e);
        }

//        ClientConfiguration config = new ClientConfiguration();
//                config.setProxyUsername("");
//                config.setProxyPassword("");
        kinesis = AmazonKinesisClientBuilder.standard()
//                .withClientConfiguration(config)
                .withCredentials(credentialsProvider)
                .withRegion("ap-southeast-1")
                .build();
    }

    public static void main(String args[]) throws Exception {
        init();
        ListStreamsResult list = kinesis.listStreams();
        List<String> streamNames = list.getStreamNames();

        for(String item : streamNames){
            System.out.println(item);
        }

        System.out.println("Hello World!");
    }
}
