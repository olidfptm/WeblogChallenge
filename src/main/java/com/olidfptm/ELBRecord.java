package com.olidfptm;

import java.io.Serializable;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ELBRecord implements Serializable{

    private static Logger logger = Logger.getLogger("ELBRecord");

    private String requestTimestamp;
    private String elbName;
    private String requestIp;
    private int requestPort;
    private String backendIp;
    private int backendPort;
    private double requestProcessingTime;
    private double backendProcessingTime;
    private double clientResponseTime;
    private String elbResponseCode;
    private String backendResponseCode;
    private long receivedBytes;
    private long sentBytes;
    private String requestVerb;
    private String url;
    private String protocol;
    private String userAgent;
    private String sslCipher;
    private String sslProtocol;

    public ELBRecord(){

    }

    static ELBRecord EMPTY = new ELBRecord();
    public static ELBRecord getEmptyRecord(){
        return EMPTY;
    }

    public ELBRecord(String requestTimestamp, String elbName, String requestIp, int requestPort, String backendIp, int backendPort, double requestProcessingTime, double backendProcessingTime, double clientResponseTime, String elbResponseCode, String backendResponseCode, long receivedBytes, long sentBytes, String requestVerb, String url, String protocol, String userAgent, String sslCipher, String sslProtocol) {
        this.requestTimestamp = requestTimestamp;
        this.elbName = elbName;
        this.requestIp = requestIp;
        this.requestPort = requestPort;
        this.backendIp = backendIp;
        this.backendPort = backendPort;
        this.requestProcessingTime = requestProcessingTime;
        this.backendProcessingTime = backendProcessingTime;
        this.clientResponseTime = clientResponseTime;
        this.elbResponseCode = elbResponseCode;
        this.backendResponseCode = backendResponseCode;
        this.receivedBytes = receivedBytes;
        this.sentBytes = sentBytes;
        this.requestVerb = requestVerb;
        this.url = url;
        this.protocol = protocol;
        this.userAgent = userAgent;
        this.sslCipher = sslCipher;
        this.sslProtocol = sslProtocol;
    }

    public String getRequestTimestamp() {
        return requestTimestamp;
    }

    public void setRequestTimestamp(String requestTimestamp) {
        this.requestTimestamp = requestTimestamp;
    }

    public String getElbName() {
        return elbName;
    }

    public void setElbName(String elbName) {
        this.elbName = elbName;
    }

    public String getRequestIp() {
        return requestIp;
    }

    public void setRequestIp(String requestIp) {
        this.requestIp = requestIp;
    }

    public int getRequestPort() {
        return requestPort;
    }

    public void setRequestPort(int requestPort) {
        this.requestPort = requestPort;
    }

    public String getBackendIp() {
        return backendIp;
    }

    public void setBackendIp(String backendIp) {
        this.backendIp = backendIp;
    }

    public int getBackendPort() {
        return backendPort;
    }

    public void setBackendPort(int backendPort) {
        this.backendPort = backendPort;
    }

    public double getRequestProcessingTime() {
        return requestProcessingTime;
    }

    public void setRequestProcessingTime(double requestProcessingTime) {
        this.requestProcessingTime = requestProcessingTime;
    }

    public double getBackendProcessingTime() {
        return backendProcessingTime;
    }

    public void setBackendProcessingTime(double backendProcessingTime) {
        this.backendProcessingTime = backendProcessingTime;
    }

    public double getClientResponseTime() {
        return clientResponseTime;
    }

    public void setClientResponseTime(double clientResponseTime) {
        this.clientResponseTime = clientResponseTime;
    }

    public String getElbResponseCode() {
        return elbResponseCode;
    }

    public void setElbResponseCode(String elbResponseCode) {
        this.elbResponseCode = elbResponseCode;
    }

    public String getBackendResponseCode() {
        return backendResponseCode;
    }

    public void setBackendResponseCode(String backendResponseCode) {
        this.backendResponseCode = backendResponseCode;
    }

    public long getReceivedBytes() {
        return receivedBytes;
    }

    public void setReceivedBytes(long receivedBytes) {
        this.receivedBytes = receivedBytes;
    }

    public long getSentBytes() {
        return sentBytes;
    }

    public void setSentBytes(long sentBytes) {
        this.sentBytes = sentBytes;
    }

    public String getRequestVerb() {
        return requestVerb;
    }

    public void setRequestVerb(String requestVerb) {
        this.requestVerb = requestVerb;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public String getSslCipher() {
        return sslCipher;
    }

    public void setSslCipher(String sslCipher) {
        this.sslCipher = sslCipher;
    }

    public String getSslProtocol() {
        return sslProtocol;
    }

    public void setSslProtocol(String sslProtocol) {
        this.sslProtocol = sslProtocol;
    }

    //This reg ex was taken from the AWS Docs
    static String ELB_REG_EX = "([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*)[:\\-]([0-9]*) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) ([-0-9]*) ([-0-9]*) \"([^ ]*) ([^ ]*) (- |[^ ]*)\" (\"[^\"]*\") ([A-Z0-9-]+) ([A-Za-z0-9.-]*)$";
    private static final Pattern PATTERN = Pattern.compile(ELB_REG_EX);

    public static ELBRecord parseRow(String s){

        Matcher m = PATTERN.matcher(s);
        if (!m.find()) {
            logger.warning("Skipping log record due to invalid format");
            return ELBRecord.getEmptyRecord();
        }
        try{
            return new ELBRecord(
                    m.group(1),
                    m.group(2),
                    m.group(3),
                    Integer.parseInt(m.group(4)),
                    m.group(5),
                    Integer.parseInt(m.group(6)),
                    Double.parseDouble(m.group(7)),
                    Double.parseDouble(m.group(8)),
                    Double.parseDouble(m.group(9)),
                    m.group(10),
                    m.group(11),
                    Long.parseLong(m.group(12)),
                    Long.parseLong(m.group(13)),
                    m.group(14),
                    m.group(15),
                    m.group(16),
                    m.group(17),
                    m.group(18),
                    m.group(19)
            );
        }catch (Exception e){
            logger.warning("Skipping log record due to invalid format" + e.getMessage());
            return ELBRecord.getEmptyRecord();
        }

    }

}
