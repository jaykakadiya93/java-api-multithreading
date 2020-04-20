package com.comcast.keb.addressstandardize.config;
import org.hibernate.validator.constraints.NotEmpty;
import org.springframework.beans.factory.annotation.Value;
//import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;
@Component
public class CommonProperties {
    @Value("${spring.SQS.url}")
    @NotEmpty
    private String SQSUrl;

    @Value("${spring.SNS.url}")
    @NotEmpty
    private String SNSUrl;



    @Value("${spring.NAX.url}")
    @NotEmpty
    private String NAXurl;



    @Value("${spring.matchSQS.url}")
    @NotEmpty
    private String matchSQSurl;

    @Value("${spring.env}")
    @NotEmpty
    private  String env;

    public String getSQSUrl() { return SQSUrl; }

    public void setSQSUrl(String SQSUrl) { this.SQSUrl = SQSUrl; }

    public String getSNSUrl() { return SNSUrl; }

    public void setSNSUrl(String SNSUrl) { this.SNSUrl = SNSUrl; }

    public String getNAXurl() { return NAXurl; }

    public void setNAXurl(String NAXurl) { this.NAXurl = NAXurl; }

    public String getMatchSQSurl() { return matchSQSurl; }

    public void setMatchSQSurl(String matchSQSurl) { this.matchSQSurl = matchSQSurl; }

    public String getEnv() {
        return env;
    }

    public void setEnv(String env) {
        this.env = env;
    }

}
