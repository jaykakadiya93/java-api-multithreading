package com.comcast.keb.addressstandardize;

import ch.qos.logback.classic.Logger;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.comcast.keb.addressstandardize.config.CommonProperties;
import org.eclipse.persistence.jaxb.UnmarshallerProperties;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Component
public class ReadSQS {

    @Autowired
    private StandardizedAddressRepository standardizedAddressRepository;
    public static String bodyFromMessage;
    private static List<Message> messages;


    @Autowired
    private CommonProperties commonProperties;
    @Autowired
    private AddressStandardizationAWSUtility addressStandardizationAWSUtility;

    public void getMessages() {
        Logger logger = (Logger) LoggerFactory.getLogger(StandardizedAddressRepository.class.getName());
        AmazonSQS sqsconn = addressStandardizationAWSUtility.SQSConnection();
        AmazonSNS snsconn = addressStandardizationAWSUtility.SNSConnection();
        AmazonS3 s3conn = addressStandardizationAWSUtility.S3Connection();
        SqsMessage sqsMessage = new SqsMessage();
        try {
            messages = sqsconn.receiveMessage(commonProperties.getSQSUrl()).getMessages();
            for (Message message : messages) {
                bodyFromMessage = message.getBody().toString();
                sqsconn.deleteMessage(commonProperties.getSQSUrl(), message.getReceiptHandle());
            }
            JAXBContext jc = JAXBContext.newInstance(SqsMessage.class);
            Unmarshaller unmarshaller = jc.createUnmarshaller();
            unmarshaller.setProperty(UnmarshallerProperties.MEDIA_TYPE, "application/json");
            StreamSource json = new StreamSource(new StringReader("{\"sqsmessage\":" + bodyFromMessage + "}"));
            unmarshaller.setProperty(UnmarshallerProperties.JSON_INCLUDE_ROOT, true);
            sqsMessage = unmarshaller.unmarshal(json, SqsMessage.class).getValue();
            if (!messages.isEmpty()) {
                standardizedAddressRepository.processIncremetalFile(sqsMessage, snsconn, s3conn, sqsconn);
            }
        } catch (AmazonServiceException e) {
            logger.error(e.getLocalizedMessage());
            snsconn.publish(commonProperties.getSNSUrl(), e.getLocalizedMessage() + sqsMessage.getSourcename() + " Address standardization fail on " + sqsMessage.getEnv());
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage());
            snsconn.publish(commonProperties.getSNSUrl(), e.getLocalizedMessage() + sqsMessage.getSourcename() + " Address standardization fail on " + sqsMessage.getEnv());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
            snsconn.publish(commonProperties.getSNSUrl(), e.getLocalizedMessage(), sqsMessage.getSourcename() + " Address standardization fail on " + sqsMessage.getEnv());
        } finally {
            if (null != s3conn) {
                s3conn.shutdown();
            }
            if (null != sqsconn) {
                sqsconn.shutdown();
            }
            if (null != snsconn) {
                snsconn.shutdown();
            }
        }

    }
}


