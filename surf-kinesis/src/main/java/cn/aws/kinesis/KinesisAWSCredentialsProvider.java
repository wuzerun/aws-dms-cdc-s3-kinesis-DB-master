package cn.aws.kinesis;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.commons.lang.StringUtils;

public class KinesisAWSCredentialsProvider implements AWSCredentialsProvider {
    private final AWSCredentials creds;

    public KinesisAWSCredentialsProvider(String accessKey, String secretKey) {
        if (StringUtils.isEmpty(accessKey) || StringUtils.isEmpty(secretKey)) {
            creds = new AWSCredentials() {
                @Override
                public String getAWSAccessKeyId() {
                    return null;
                }

                @Override
                public String getAWSSecretKey() {
                    return null;
                }
            };
            System.out.println("Could not load credentials from KinesisAWSCredentialsProvider");
        } else {
            creds = new BasicAWSCredentials(accessKey, secretKey);
            System.out.println("Loaded credentials from KinesisAWSCredentialsProvider");
        }
    }

    @Override
    public AWSCredentials getCredentials() {
        return creds;
    }

    @Override
    public void refresh() {

    }
}
