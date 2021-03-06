package util;


import scala.Serializable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author he_yu
 * 用于解析Nginx日志.
 */
public class NginxLogParser implements Serializable {

    public static String visitedInformationRegEx    = "\"(GET|POST|PUT|DELETE|OPTIONS|CONNECT|PATCH|HEAD) (.*)";

    String method = "";
    String url = "";

    public NginxLogParser(){ }

    private String parseVisitedInformation(String content){
        Pattern p = Pattern.compile(NginxLogParser.visitedInformationRegEx);
        Matcher m = p.matcher(content);
        if(m.find()){
            String matchedString = m.group(0);
            return matchedString;
        }else{
            return "";
        }
    }


    public NginxLogParser parseLine(String content){
        String visitedInformation = parseVisitedInformation(content);
        try{
            String[] visitedInformationArray = visitedInformation.split(" ");
            this.method = visitedInformationArray[0];
            this.url = visitedInformationArray[1];
            return this;
        }catch (Exception ex){
            this.method = "";
            this.url = "";
            return this;
        }
    }

    /**
     * 验证解析的内容是否正确,即该行是否含有预期的信息，如果没有则认为该行可能是错误的信息行
     * @return
     */
    public Boolean isValid(){
        if(this.method.equals("")){
            return Boolean.FALSE;
        }
        if(this.url.equals("")){
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }


    /**
     * 功能测试代码，测试程序输出结果是否正确
     * @param args
     */
    public static void main(String args[]) throws IOException {
        NginxLogParser nginxLogParser = new NginxLogParser();

        FileReader fileReader = new FileReader("./data/examples/nginx-2020.08.head.txt");
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String line;
        while((line=bufferedReader.readLine())!=null){
            nginxLogParser.parseLine(line);
            System.out.println(nginxLogParser.getMethod());
            System.out.println(nginxLogParser.getUrl());
            //System.out.println(nginxLogParser.getVisitedInformation());
        }
    }

    public String getMethod() {
        return method;
    }

    public String getUrl() {
        return url;
    }

    @Override
    public String toString(){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(this.method);
        stringBuilder.append(" ");
        stringBuilder.append(this.url);
        return stringBuilder.toString();
    }
}
