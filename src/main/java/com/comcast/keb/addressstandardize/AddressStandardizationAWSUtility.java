package com.comcast.keb.addressstandardize;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.comcast.keb.addressstandardize.config.CommonProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public  class AddressStandardizationAWSUtility {
    @Autowired
    private  CommonProperties commonProperties;

    public  AmazonS3 S3Connection(){
        AmazonS3 s3 = null;
        if (commonProperties.getEnv().equals("PRE-PROD")) {
            s3 = AmazonS3ClientBuilder.standard()
                    .withRegion(Regions.US_EAST_2)
                    .build();
        }else if (commonProperties.getEnv().equals("PROD")){
            s3 = AmazonS3ClientBuilder.standard()
                    .withRegion(Regions.US_EAST_1)
                    .build();
        }else {
            s3 = AmazonS3ClientBuilder.standard()
                    .withCredentials(new ProfileCredentialsProvider("saml"))
                    .withRegion(Regions.US_EAST_2)
                    .build();
        }
        return  s3;
    }

    public  AmazonSQS SQSConnection(){
        AmazonSQS sqs= null;
        if (commonProperties.getEnv().equals("PRE-PROD")) {
            sqs = AmazonSQSClientBuilder.standard()
                    .withRegion(Regions.US_EAST_2)
                    .build();
        }else if (commonProperties.getEnv().equals("PROD")) {
            sqs = AmazonSQSClientBuilder.standard()
                    .withRegion(Regions.US_EAST_1)
                    .build();
        } else {
            sqs = AmazonSQSClientBuilder.standard()
                    .withCredentials(new ProfileCredentialsProvider("saml"))
                    .withRegion(Regions.US_EAST_2)
                    .build();
        }
        return sqs;
    }
    public  AmazonSNS SNSConnection(){
        AmazonSNS sns = null;
        if (commonProperties.getEnv().equals("PRE-PROD")) {
            sns = AmazonSNSClientBuilder.standard()
                    .withRegion(Regions.US_EAST_2)
                    .build();
        }else if (commonProperties.getEnv().equals("PROD")) {
            sns = AmazonSNSClientBuilder.standard()
                    .withRegion(Regions.US_EAST_1)
                    .build();
        }else {
            sns = AmazonSNSClientBuilder.standard()
                    .withCredentials(new ProfileCredentialsProvider("saml"))
                    .withRegion(Regions.US_EAST_2)
                    .build();
        }
        return sns;
    }

}
