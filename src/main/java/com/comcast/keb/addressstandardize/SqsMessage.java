package com.comcast.keb.addressstandardize;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.File;

@XmlRootElement
public class SqsMessage {

    private String bucket;
    private String raw_key;
    private String incremental_key;
    private String tranformed_key;
    private String archive_key;
    private String valid_filename;
    private String invalid_filename;
    private String outputpath;
    private String inputpath;
    private String standardized_address;
    private String standardized_fallout;
    private String valid_string_filename;
    private String valid_address_key;
    private String addressline1;
    private String addressline2;
    private String city;
    private String state;
    private String zip;
    private String delimiter;
    private String sourcename;
    private String splitdelimiter;
    private String payload_type;
    private String insert_filename;
    private String insert_recordcount;
    private String env;
    private String update_filename;
    private String update_recordcount;
    private String delete_filename;
    private String delete_recordcount;
    private String filedelimiter = "_";
    private String fileExtension = ".csv";
    private String forwardslash = "/";

    public String[] getarrayofinsertfilename(){
        return insert_filename.split(filedelimiter);
    }

    public String getCurrentDatasetDate(){
        return getarrayofinsertfilename()[getarrayofinsertfilename().length - 3];

    }
    public String getPreviousDatasetDate (){
        return getarrayofinsertfilename()[getarrayofinsertfilename().length - 2];

    }


    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getRaw_key() {
        return raw_key;
    }

    public void setRaw_key(String raw_key) {
        this.raw_key = raw_key;
    }

    public String getIncremental_key() {
        return incremental_key;
    }

    public void setIncremental_key(String incremental_key) {
        this.incremental_key = incremental_key;
    }

    public String getTranformed_key() {
        return tranformed_key;
    }

    public void setTranformed_key(String tranformed_key) {
        this.tranformed_key = tranformed_key;
    }

    public String getArchive_key() {
        return archive_key;
    }

    public void setArchive_key(String archive_key) {
        this.archive_key = archive_key;
    }

    public String getValid_filename() {
        return valid_filename;
    }

    public void setValid_filename(String valid_filename) {
        this.valid_filename = valid_filename;
    }

    public String getInvalid_filename() {
        return invalid_filename;
    }

    public void setInvalid_filename(String invalid_filename) {
        this.invalid_filename = invalid_filename;
    }

    public String getOutputpath() {
        return outputpath;
    }

    public void setOutputpath(String outputpath) {
        this.outputpath = outputpath;
    }

    public String getInputpath() {
        return inputpath;
    }

    public void setInputpath(String inputpath) {
        this.inputpath = inputpath;
    }

    public String getStandardized_address() {
        return standardized_address;
    }

    public void setStandardized_address(String standardized_address) {
        this.standardized_address = standardized_address;
    }

    public String getStandardized_fallout() {
        return standardized_fallout;
    }

    public void setStandardized_fallout(String standardized_fallout) {
        this.standardized_fallout = standardized_fallout;
    }

    public String getValid_string_filename() {
        return valid_string_filename;
    }

    public void setValid_string_filename(String valid_string_filename) {
        this.valid_string_filename = valid_string_filename;
    }

    public String getValid_address_key() {
        return valid_address_key;
    }

    public void setValid_address_key(String valid_address_key) {
        this.valid_address_key = valid_address_key;
    }

    public String getAddressline1() {
        return addressline1;
    }

    public void setAddressline1(String addressline1) {
        this.addressline1 = addressline1;
    }

    public String getAddressline2() {
        return addressline2;
    }

    public void setAddressline2(String addressline2) {
        this.addressline2 = addressline2;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getZip() {
        return zip;
    }

    public void setZip(String zip) {
        this.zip = zip;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getSourcename() {
        return sourcename;
    }

    public void setSourcename(String sourcename) {
        this.sourcename = sourcename;
    }

    public String getSplitdelimiter() {
        return splitdelimiter;
    }

    public void setSplitdelimiter(String splitdelimiter) {
        this.splitdelimiter = splitdelimiter;
    }

    public String getPayload_type() {
        return payload_type;
    }

    public void setPayload_type(String payload_type) {
        this.payload_type = payload_type;
    }

    public String getInsert_filename() {
        return insert_filename;
    }

    public void setInsert_filename(String insert_filename) {
        this.insert_filename = insert_filename;
    }

    public String getInsert_recordcount() {
        return insert_recordcount;
    }

    public void setInsert_recordcount(String insert_recordcount) {
        this.insert_recordcount = insert_recordcount;
    }

    public String getEnv() {
        return env;
    }

    public void setEnv(String env) {
        this.env = env;
    }

    public String getUpdate_filename() {
        return update_filename;
    }

    public void setUpdate_filename(String update_filename) {
        this.update_filename = update_filename;
    }

    public String getUpdate_recordcount() {
        return update_recordcount;
    }

    public void setUpdate_recordcount(String update_recordcount) {
        this.update_recordcount = update_recordcount;
    }

    public String getDelete_filename() {
        return delete_filename;
    }

    public void setDelete_filename(String delete_filename) {
        this.delete_filename = delete_filename;
    }

    public String getDelete_recordcount() {
        return delete_recordcount;
    }

    public void setDelete_recordcount(String delete_recordcount) {
        this.delete_recordcount = delete_recordcount;
    }


    public String getvalidfilelocation(){
        return outputpath +
                valid_filename +
                sourcename +
                filedelimiter +
                getCurrentDatasetDate() +
                filedelimiter +
                getPreviousDatasetDate() +
                fileExtension;

    }

    public String getvalidStringfilelocation(){
        return outputpath +
                valid_string_filename +
                sourcename +
                filedelimiter +
                getCurrentDatasetDate() +
                filedelimiter +
                getPreviousDatasetDate() +
                fileExtension;

    }

    public String getinvalidfilelocation(){
        return outputpath +
                invalid_filename +
                sourcename +
                filedelimiter +
                getCurrentDatasetDate() +
                filedelimiter +
                getPreviousDatasetDate() +
                fileExtension;

    }

    public String getvalids3filelocation(){
        return  env +
                sourcename +
                forwardslash +
                standardized_address +
                valid_filename +
                sourcename +
                filedelimiter +
                getCurrentDatasetDate() +
                filedelimiter +
                getPreviousDatasetDate() +
                fileExtension;
    }

    public String getinvalids3filelocation(){
        return  env +
                sourcename +
                forwardslash +
                standardized_fallout +
                invalid_filename +
                sourcename +
                filedelimiter +
                getCurrentDatasetDate() +
                filedelimiter +
                getPreviousDatasetDate() +
                fileExtension;
    }

    public String getvalidstrings3filelocation(){
          return  env +
                valid_address_key +
                valid_string_filename +
                sourcename+
                filedelimiter +
                getCurrentDatasetDate() +
                filedelimiter +
                getPreviousDatasetDate() +
                fileExtension;
    }

    public String getinserts3filelocation(){
        return env +
                sourcename +
                forwardslash +
                incremental_key +
                insert_filename;
    }

}
