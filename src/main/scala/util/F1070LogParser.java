package util;


import scala.Serializable;

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
    public static String regEx="((2[0-4]\\d|25[0-5]|[01]?\\d\\d?)\\.){3}(2[0-4]\\d|25[0-5]|[01]?\\d\\d?)";
    //public static String srcIpAddrRegEx = "SrcIPAddr(\\d+)=((2[0-4]\\d|25[0-5]|[01]?\\d\\d?)\\.){3}(2[0-4]\\d|25[0-5]|[01]?\\d\\d?)";
    public static String srcIpAddrRegEx  = "SrcIPAddr\\(\\d*\\)=((2[0-4]\\d|25[0-5]|[01]?\\d\\d?)\\.){3}(2[0-4]\\d|25[0-5]|[01]?\\d\\d?)";
    public static String distIpAddrRegEx = "DstIPAddr\\(\\d*\\)=((2[0-4]\\d|25[0-5]|[01]?\\d\\d?)\\.){3}(2[0-4]\\d|25[0-5]|[01]?\\d\\d?)";

    List<String> ipList = null;

    public F1070LogParser(){
    }

    public void parseSourceIp(String content){
        //String sourceip = null;
        Pattern p = Pattern.compile(F1070LogParser.srcIpAddrRegEx);
        Matcher m = p.matcher(content);
        while(m.find()){
            System.out.println(m.group(0));
        }
        //return sourceIp;
    }

    public void parseDistIp(String content){
        //String sourceip = null;
        Pattern p = Pattern.compile(F1070LogParser.distIpAddrRegEx);
        Matcher m = p.matcher(content);
        while(m.find()){
            System.out.println(m.group(0));
        }
        //return sourceIp;
    }

    public F1070LogParser parseLine(String content){
        ipList = new ArrayList<String>();

        Pattern p = Pattern.compile(F1070LogParser.regEx);
        Matcher m = p.matcher(content);
        while (m.find()) {
            String result = m.group();
            ipList.add(result);
        }
        return this;
    }

    /**
     * 验证解析的内容是否正确,即该行是否含有预期的信息，如果没有则认为该行可能是错误的信息行
     * @return
     */
    public Boolean isValid(){
        if(ipList.size()!=3){
            return Boolean.FALSE;
        }else{
            return Boolean.TRUE;
        }
    }

    public String getSourceIp(){
        return ipList.get(0);
    }

    /**
     * 功能测试代码，测试程序输出结果是否正确
     * @param args
     */
    public static void main(String args[]){
        String rawContent = "2020-08-31T12:07:36.000Z 172.16.103.12 F1070-B %%10FILTER/6/FILTER_ZONE_IPV4_EXECUTION: SrcZoneName(1025)=Trust;DstZoneName(1035)=Untrust;Type(1067)=ACL;SecurityPolicy(1072)=Trust-UnTrust;RuleID(1078)=2;Protocol(1001)=UDP;Application(1002)=dns;SrcIPAddr(1003)=172.17.6.24;SrcPort(1004)=40012;DstIPAddr(1007)=114.114.114.114;DstPort(1008)=53;MatchCount(1069)=1;Event(1048)=Permit;";

        F1070LogParser parser = new F1070LogParser();
        //parser.parseLine(rawContent);
        //System.out.println(parser.ipList);
        //parser.parseSourceIp(rawContent);
        parser.parseDistIp(rawContent);
    }
}
