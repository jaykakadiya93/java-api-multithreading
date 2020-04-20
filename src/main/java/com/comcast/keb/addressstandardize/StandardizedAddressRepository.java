package com.comcast.keb.addressstandardize;

import ch.qos.logback.classic.Logger;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.comcast.keb.addressstandardize.config.CommonProperties;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


@Component
public class StandardizedAddressRepository {
    @Autowired
    NaxService naxService;
    @Autowired
    private CommonProperties commonProperties;
    @Autowired
    private ReadSQS readSQS;
    @Autowired
    private AddressStandardizationAWSUtility addressStandardizationAWSUtility;

    StandardizedAddressApplication standardizedAddressApplication = new StandardizedAddressApplication();
    Logger logger = (Logger) LoggerFactory.getLogger(StandardizedAddressRepository.class.getName());

    private static String NEWLINE = "\n";
    private static String fileExtension = ".csv";
    private static String nullvalue = "null";
    private static int indexOfAddressLine1;
    private static int indexOfAddressLine2;
    private static int indexOfCity;
    private static int indexOfState;
    private static int indexOfZip;
    private static String delimiter = "_";
    private static String empty = "EMPTY";
    private String header;

    public void processIncremetalFile(SqsMessage sqsMessage, AmazonSNS snsconn, AmazonS3 s3conn, AmazonSQS sqsconn) throws Exception {
        snsconn.publish(commonProperties.getSNSUrl(), ReadSQS.bodyFromMessage, sqsMessage.getSourcename() + " picked up for address standardization on " + sqsMessage.getEnv());
        logger.info(sqsMessage.getSourcename() + " start File Process");
        downloadFileFromS3(sqsMessage, s3conn);
        BufferedReader readerInsert = new BufferedReader(new FileReader(sqsMessage.getInputpath() + sqsMessage.getInsert_filename()));
        header = readerInsert.readLine();
        appendHeaders(sqsMessage, header);
        processFile(readerInsert, snsconn, sqsMessage);
        readerInsert.close();


        //upload file to s3 method
        uploadFileToS3(sqsMessage, s3conn);


        //delete file from input folder
        deleteInputFiles(sqsMessage);

        //send message to SQS to match
        sendMessageToMatchSQS(sqsMessage, snsconn, sqsconn);
    }

    public double recordCount(String filename) throws IOException {
        Path path = Paths.get(filename);
        double filecount = Files.lines(path).count();
        return filecount;
    }

    public void append(String data, BufferedWriter writer) throws IOException {

        writer.append(data);
        writer.close();
    }

    public void downloadFileFromS3(SqsMessage sqsMessage, AmazonS3 s3conn) throws AmazonServiceException {
        s3conn.getObject(new GetObjectRequest(sqsMessage.getBucket(), sqsMessage.getinserts3filelocation()), new File(sqsMessage.getInputpath() + sqsMessage.getInsert_filename()));
    }

    public BufferedWriter getWriter(String fileName) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName,true));
        return writer;
    }

    public void processFile(BufferedReader reader, AmazonSNS snsconn, SqsMessage sqsMessage) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(50);
        Set<Callable<AddressResponse>> callables = new HashSet<Callable<AddressResponse>>();
        String line;
        while ((line = reader.readLine()) != null) {
            String record = line;
            callables.add(new Callable<AddressResponse>() {
                @Override
                public AddressResponse call() throws Exception {


                    String[] arrofline = record.split(sqsMessage.getSplitdelimiter());

                    AddressRequest addressRequest = new AddressRequest();
                    addressRequest.setAddressLine1(arrofline[indexOfAddressLine1]);

                    addressRequest.setCity(arrofline[indexOfCity]);
                    addressRequest.setState(arrofline[indexOfState]);
                    addressRequest.setZip(arrofline[indexOfZip]);
                    AddressResponse addressResponse = new AddressResponse();
                    String validAddressResponse = null;
                    String validAddressResponseString = null;
                    String naxAddressInvalidResponse = null;

                    try {
                        addressResponse = naxService.getNaxResponse(addressRequest);
                        if (!sqsMessage.getAddressline2().equals(nullvalue)) {
                            addressRequest.setAddressLine2(arrofline[indexOfAddressLine2]);
                        }
                        String ValidAddressResponse = null;
                        String ValidAddressResponseMerge = null;

                        ValidAddressResponse = formatAndStoreResponse(addressResponse, addressRequest, sqsMessage, record, validAddressResponse, validAddressResponseString);
                        ValidAddressResponseMerge = formatAndStoreAddressStringResponse(addressResponse, addressRequest, sqsMessage, record, validAddressResponse, validAddressResponseString);
                        append(ValidAddressResponse, getWriter(sqsMessage.getvalidfilelocation()));
                        append(ValidAddressResponseMerge, getWriter(sqsMessage.getvalidStringfilelocation()));

                    } catch (IOException ex) {
                        throw new IOException(ex);
                    }catch (Exception e) {
                        if (e.getMessage().equals(HttpStatus.INTERNAL_SERVER_ERROR)) {
                            throw new Exception(e);
                        }

                        naxAddressInvalidResponse = record + sqsMessage.getDelimiter() + e.getMessage() + NEWLINE;

                        append(naxAddressInvalidResponse, getWriter(sqsMessage.getinvalidfilelocation()));
                    }
                    return addressResponse;
                }
            });
        }
        List<Future<AddressResponse>> futures = executorService.invokeAll(callables);
        executorService.shutdown();
        logger.info(sqsMessage.getSourcename() + " Finish File Process");
    }


    public String formatAndStoreResponse(AddressResponse addressResponse, AddressRequest addressRequest, SqsMessage sqsMessage, String record, String validAddressResponselist, String validAddressStringlist) {
        String naxAddressResponse = null;
        if (addressResponse.getAddressLine1() != null && addressRequest.getAddressLine2() != null) {
            naxAddressResponse = record + sqsMessage.getDelimiter() + addressResponse.getAddressLine1() + sqsMessage.getDelimiter() + addressRequest.getAddressLine2() + sqsMessage.getDelimiter() + addressResponse.getCity() + sqsMessage.getDelimiter() + addressResponse.getState() + sqsMessage.getDelimiter() + addressResponse.getZip() + NEWLINE;

        } else {
            naxAddressResponse = record + sqsMessage.getDelimiter() + addressResponse.getAddressLine1() + sqsMessage.getDelimiter() + sqsMessage.getDelimiter() + addressResponse.getCity() + sqsMessage.getDelimiter() + addressResponse.getState() + sqsMessage.getDelimiter() + addressResponse.getZip() + NEWLINE;

        }

        return naxAddressResponse;
    }

    public String formatAndStoreAddressStringResponse(AddressResponse addressResponse, AddressRequest addressRequest, SqsMessage sqsMessage, String record, String validAddressResponselist, String validAddressStringlist) {

        String addReqString = null;
        String naxAddressStringResponse = null;
        if (addressResponse.getAddressLine1() != null && addressRequest.getAddressLine2() != null) {

            addReqString = addressRequest.getAddressLine1() + delimiter + addressRequest.getAddressLine2() + delimiter + addressRequest.getCity() + delimiter + addressRequest.getState() + delimiter + addressRequest.getZip();
            naxAddressStringResponse = addReqString.replaceAll("\\s", "") + sqsMessage.getDelimiter() + addressResponse.getAddressLine1() + sqsMessage.getDelimiter() + addressResponse.getAddressLine2() + sqsMessage.getDelimiter() + addressResponse.getCity() + sqsMessage.getDelimiter() + addressResponse.getState() + sqsMessage.getDelimiter() + addressResponse.getZip() + sqsMessage.getDelimiter() + sqsMessage.getSourcename() + NEWLINE;

        } else {

            addReqString = addressRequest.getAddressLine1() + delimiter + empty + delimiter + addressRequest.getCity() + delimiter + addressRequest.getState() + delimiter + addressRequest.getZip();
            naxAddressStringResponse = addReqString.replaceAll("\\s", "") + sqsMessage.getDelimiter() + addressResponse.getAddressLine1() + sqsMessage.getDelimiter() + addressResponse.getAddressLine2() + sqsMessage.getDelimiter() + addressResponse.getCity() + sqsMessage.getDelimiter() + addressResponse.getState() + sqsMessage.getDelimiter() + addressResponse.getZip() + sqsMessage.getDelimiter() + sqsMessage.getSourcename() + NEWLINE;

        }

        return naxAddressStringResponse;
    }


    public void  appendHeaders(SqsMessage sqsMessage, String header) throws Exception {
        String validHeader = null;
        String validAddressStringHeader = null;
        String InvalidHeader = null;
        String NaxAddr1 = "NAX_ADDRESS_LINE1";
        String NaxAddr2 = "NAX_ADDRESS_LINE2";
        String NaxCity = "NAX_CITY";
        String NaxState = "NAX_STATE_CD";
        String NaxZip = "NAX_ZIP_CODE";
        String AddressString = "ADDR_STR";
        String Source = "SOURCE";
        String ResponseCode = "RESPONSE_CODE";
        List arrayHeader = Arrays.asList(header.split(sqsMessage.getSplitdelimiter()));
        indexOfAddressLine1 = arrayHeader.indexOf(sqsMessage.getAddressline1());
        if (sqsMessage.getAddressline2().equals(nullvalue)) {
            indexOfAddressLine2 = 0;

        } else {
            indexOfAddressLine2 = arrayHeader.indexOf(sqsMessage.getAddressline2());
        }
        indexOfCity = arrayHeader.indexOf(sqsMessage.getCity());
        indexOfState = arrayHeader.indexOf(sqsMessage.getState());
        indexOfZip = arrayHeader.indexOf(sqsMessage.getZip());

        validHeader = header + sqsMessage.getDelimiter() + NaxAddr1 + sqsMessage.getDelimiter() + NaxAddr2 + sqsMessage.getDelimiter() + NaxCity + sqsMessage.getDelimiter() + NaxState + sqsMessage.getDelimiter() + NaxZip + NEWLINE;
        validAddressStringHeader = AddressString + sqsMessage.getDelimiter() + NaxAddr1 + sqsMessage.getDelimiter() + NaxAddr2 + sqsMessage.getDelimiter() + NaxCity + sqsMessage.getDelimiter() + NaxState + sqsMessage.getDelimiter() + NaxZip + sqsMessage.getDelimiter() + Source + NEWLINE;
        InvalidHeader = header + sqsMessage.getDelimiter() + ResponseCode + NEWLINE;


        append(validHeader, getWriter(sqsMessage.getvalidfilelocation()));
        append(validAddressStringHeader, getWriter(sqsMessage.getvalidStringfilelocation()));
        append(InvalidHeader, getWriter(sqsMessage.getinvalidfilelocation()));

    }


    public void uploadFileToS3(SqsMessage sqsMessage, AmazonS3 s3conn) throws AmazonServiceException {
        logger.info(sqsMessage.getSourcename() + " upload started");
        s3conn.putObject(sqsMessage.getBucket(), sqsMessage.getvalids3filelocation(), new File(sqsMessage.getvalidfilelocation()));
        s3conn.putObject(sqsMessage.getBucket(), sqsMessage.getinvalids3filelocation(), new File(sqsMessage.getinvalidfilelocation()));
        s3conn.putObject(sqsMessage.getBucket(), sqsMessage.getvalidstrings3filelocation(), new File(sqsMessage.getvalidStringfilelocation()));
        logger.info(sqsMessage.getSourcename() + " upload finished");
    }

    public void deleteOutPutFiles(SqsMessage sqsMessage) throws IOException {

        logger.info(sqsMessage.getSourcename() + " output file delition started");
        Files.deleteIfExists(Paths.get(sqsMessage.getvalidfilelocation()));
        Files.deleteIfExists(Paths.get(sqsMessage.getinvalidfilelocation()));
        Files.deleteIfExists(Paths.get(sqsMessage.getvalidStringfilelocation()));
        logger.info(sqsMessage.getSourcename() + " output file delition completed");

    }

    public void deleteInputFiles(SqsMessage sqsMessage) throws IOException {
        logger.info(sqsMessage.getSourcename() + " Input file delition started");
        Files.deleteIfExists(Paths.get(sqsMessage.getInputpath() + sqsMessage.getInsert_filename()));
        logger.info(sqsMessage.getSourcename() + " Input file delition completed");
    }

    public void sendMessageToMatchSQS(SqsMessage sqsMessage, AmazonSNS snsconn, AmazonSQS sqsconn) throws AmazonServiceException,IOException {
        Gson gson = new Gson();
        JsonObject jsonMessage = gson.fromJson(ReadSQS.bodyFromMessage, JsonObject.class);
        jsonMessage.remove("payload_type");
        jsonMessage.addProperty("valid_standardized_address_response_filename", sqsMessage.getValid_filename() + sqsMessage.getSourcename() + delimiter + sqsMessage.getCurrentDatasetDate() + delimiter + sqsMessage.getPreviousDatasetDate() + fileExtension);
        jsonMessage.addProperty("invalid_standardized_address_response_filename", sqsMessage.getInvalid_filename() + sqsMessage.getSourcename() + delimiter + sqsMessage.getCurrentDatasetDate() + delimiter + sqsMessage.getPreviousDatasetDate() + fileExtension);
        jsonMessage.addProperty("payload_type", "TO_MATCH");
        jsonMessage.addProperty("valid_file_location", sqsMessage.getEnv() + sqsMessage.getSourcename() + "/" + sqsMessage.getStandardized_address());
        jsonMessage.addProperty("bucket", sqsMessage.getBucket());
        jsonMessage.addProperty("sourcename", sqsMessage.getSourcename());
        jsonMessage.addProperty("valid_file_count", recordCount(sqsMessage.getvalidfilelocation()));
        jsonMessage.addProperty("invalid_file_count", recordCount(sqsMessage.getinvalidfilelocation()));

        String message = jsonMessage.toString();

        SendMessageRequest sendMessageRequest = new SendMessageRequest(commonProperties.getMatchSQSurl(), message);
        sendMessageRequest.setMessageGroupId(sqsMessage.getSourcename() + "_Match");
        sqsconn.sendMessage(sendMessageRequest);
        deleteOutPutFiles(sqsMessage);
        snsconn.publish(commonProperties.getSNSUrl(), sqsMessage.getSourcename() + " match file is Ready " + message, sqsMessage.getSourcename() + " Address standadization successful on " + sqsMessage.getEnv());
    }
}

