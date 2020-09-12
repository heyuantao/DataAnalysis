package util;


import org.stringtemplate.v4.ST;
import scala.Serializable;
import sun.management.BaseOperatingSystemImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author he_yu
 * 用于解析华三 F1070防火墙的日志，防火墙的日志主要为NAT的日志。在这个日志中可以收取源地址，目的地址和端口的一系列信息
 * 相应的样例文件放在resources目录中
 */
public class F1070LogParser implements Serializable {

    public static String sourceIpAddressRegEx       = "SrcIPAddr\\(\\d*\\)=((2[0-4]\\d|25[0-5]|[01]?\\d\\d?)\\.){3}(2[0-4]\\d|25[0-5]|[01]?\\d\\d?)";
    public static String destinationIpAddressRegEx  = "DstIPAddr\\(\\d*\\)=((2[0-4]\\d|25[0-5]|[01]?\\d\\d?)\\.){3}(2[0-4]\\d|25[0-5]|[01]?\\d\\d?)";
    public static String sourcePortRegEx            = "SrcPort\\(\\d*\\)=(\\d*)";
    public static String destinationPortRegEx       = "DstPort\\(\\d*\\)=(\\d*)";
    public static String protocolRegEx              = "Protocol\\(\\d*\\)=(\\w*)";


    String sourceIp         = "";
    String destinationIp    = "";
    String sourcePort       = "";
    String destinationPort  = "";
    String protocol         = "";
    String time             = "";

    public F1070LogParser(){ }

    private String parseSourceIp(String content){
        Pattern p = Pattern.compile(F1070LogParser.sourceIpAddressRegEx);
        Matcher m = p.matcher(content);
        if(m.find()){
            String matchedString = m.group(0);
            String[] splitMatchedString = matchedString.split("=");
            String ipString = splitMatchedString[1];
            return ipString;
        }else{
            return "";
        }
    }

    private String parseDestinationIp(String content){
        Pattern p = Pattern.compile(F1070LogParser.destinationIpAddressRegEx);
        Matcher m = p.matcher(content);
        if(m.find()){
            String matchedString = m.group(0);
            String[] splitMatchedString = matchedString.split("=");
            String ipString = splitMatchedString[1];
            return ipString;
        }else{
            return "";
        }
    }

    private String parseSourcePort(String content){
        Pattern p = Pattern.compile(F1070LogParser.sourcePortRegEx);
        Matcher m = p.matcher(content);
        if(m.find()){
            String matchedString = m.group(0);
            String[] splitMatchedString = matchedString.split("=");
            String ipString = splitMatchedString[1];
            return ipString;
        }else{
            return "";
        }
    }

    private String parseDestinationPort(String content){
        Pattern p = Pattern.compile(F1070LogParser.destinationPortRegEx);
        Matcher m = p.matcher(content);
        if(m.find()){
            String matchedString = m.group(0);
            String[] splitMatchedString = matchedString.split("=");
            String ipString = splitMatchedString[1];
            return ipString;
        }else{
            return "";
        }
    }

    private String parseProtocal(String content){
        Pattern p = Pattern.compile(F1070LogParser.protocolRegEx);
        Matcher m = p.matcher(content);
        if(m.find()){
            String matchedString = m.group(0);
            String[] splitMatchedString = matchedString.split("=");
            String ipString = splitMatchedString[1];
            return ipString;
        }else{
            return "";
        }
    }

    private String parseTime(String content){
        String[] splitedString = content.split(" ");
        return splitedString[0];
    }

    public F1070LogParser parseLine(String content){
        this.sourceIp = parseSourceIp(content);
        this.destinationIp = parseDestinationIp(content);
        this.sourcePort = parseSourcePort(content);
        this.destinationPort = parseDestinationPort(content);
        this.protocol = parseProtocal(content);
        this.time = parseTime(content);
        return this;
    }

    /**
     * 验证解析的内容是否正确,即该行是否含有预期的信息，如果没有则认为该行可能是错误的信息行
     * @return
     */
    public Boolean isValid(){
        if(this.sourceIp.equals("")){
            return Boolean.FALSE;
        }
        if(this.destinationIp.equals("")){
            return Boolean.FALSE;
        }


        return Boolean.TRUE;
    }


    /**
     * 功能测试代码，测试程序输出结果是否正确
     * @param args
     */
    public static void main(String args[]){
        String rawContent = "2020-08-31T12:07:36.000Z 172.16.103.12 F1070-B %%10FILTER/6/FILTER_ZONE_IPV4_EXECUTION: SrcZoneName(1025)=Trust;DstZoneName(1035)=Untrust;Type(1067)=ACL;SecurityPolicy(1072)=Trust-UnTrust;RuleID(1078)=2;Protocol(1001)=UDP;Application(1002)=dns;SrcIPAddr(1003)=172.17.6.24;SrcPort(1004)=40012;DstIPAddr(1007)=114.114.114.114;DstPort(1008)=53;MatchCount(1069)=1;Event(1048)=Permit;";

        F1070LogParser parser = new F1070LogParser();
        parser.parseLine(rawContent);
        System.out.println(parser.getSourceIp()+":"+parser.getSourcePort());
        System.out.println(parser.getDestinationIp()+":"+parser.getDestinationPort());
        System.out.println(parser.getProtocol());
    }

    public String getSourceIp() {
        return sourceIp;
    }

    public String getDestinationIp() {
        return destinationIp;
    }

    public String getSourcePort() {
        return sourcePort;
    }

    public String getDestinationPort() {
        return destinationPort;
    }

    public String getProtocol() {
        return protocol;
    }
}
