package AnhuiDianxinYiqi;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TongJiMultiGroupBy {

    public static void main(String[] args) {
       // String[] groupTypes = {"ty","aid","st","hour",amid};
        //group By 条件  ，
        String[]  groups = {"amid","hour"}; // group by amid,hour
        //日志文件
        String fileLocal = "C:\\Users\\Administrator\\Desktop\\adst-request.log.2016-10-27";
        readFileByLines(fileLocal,groups);
    }
    public static void readFileByLines(String fileName, String[] groupTypes) {
        File in = new File(fileName);
        //结果输出位置
        File out = new File("d:/out.txt");
        BufferedReader reader = null;
        BufferedWriter writer = null;
        try {
            //   System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(in));
            writer = new BufferedWriter(new FileWriter(out,true));
            String tempString = null;
            int line = 1;
            int lineOfAll = 1;
            Map<String, Integer> exposure = new HashMap<>();
            Map<String, Set<String>> touch = new HashMap<>();
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                //    System.out.println("line " + line + ": " + tempString);
                if(tempString.length() <= 55){
                    continue;
                }
                String ty,aid,st,devid,ven,hour,amid;
                String group = "";
                //ven=900111
                ven = "";
                try {
                    ven = tempString.substring(tempString.indexOf("ven=")+4);
                    ven = ven.substring(0,ven.indexOf("&"));
                } catch (Exception e) {
                    System.out.println("line :" + line);
                    System.out.println("lineOfAll :" + lineOfAll);
                    e.printStackTrace();
                }
                //安徽电信过滤
                if("900111".equals(ven)){
//                if(true){

                    ty = tempString.substring(tempString.indexOf(" - ty=")+6);
                    ty = ty.substring(0, ty.indexOf("&"));

                    aid = tempString.substring(tempString.indexOf("aid=")+4);
                    aid = aid.substring(0, aid.indexOf("&"));

                    st = tempString.substring(tempString.indexOf("st=")+3);
                    st = st.substring(0, st.indexOf("&"));

                    devid = tempString.substring(tempString.indexOf("devid=")+6);
                    devid = devid.substring(0, devid.indexOf("&"));

                    amid = tempString.substring(tempString.indexOf("amid=")+5);
                    amid = amid.substring(0, amid.indexOf("&"));
                    //[2016-10-27 11:41:28]
                    hour = tempString.substring(20,22);
                    //       System.out.println("ty:"+ty+"   st:"+st+"  devid:"+devid+"  aid:"+aid);
                    for (String groupType : groupTypes) {
                        if("ty".equals(groupType)){
                            group += ty + ",";
                        }else if("aid".equals(groupType)){
                            group += aid + ",";
                        }else if("st".equals(groupType)){
                            group += st + ",";
                        }else if("hour".equals(groupType)){
                            group += hour + ",";
                        }else if ("amid".equals(groupType)) {
                            group += amid + ",";
                        }
                    }
                    if(!exposure.containsKey(group)){
                        exposure.put(group, 0);
                    }
                    exposure.put(group,exposure.get(group)+1);
                    Set set = touch.get(group);
                    if(null == set){
                        set = new HashSet<String>();
                        touch.put(group, set);
                    }
                    set.add(devid);
                    line++;
                }
                lineOfAll++;
            }
            String outLine = null;
            String groupByTitle = "";
            for (String type : groupTypes) {
                    groupByTitle+=type+"-";
            }

            System.out.println("-------------------group by"+groupByTitle+"-----------------------"+line);
            writer.write("-------------------group by"+groupByTitle+"-----------------------"+line+"\n");
            outLine= String.format("%-15s\t%4s\t%4s\n",groupByTitle,"曝光","触达");
            System.out.print(outLine);
            writer.write(outLine);
            Set<String> keySet = exposure.keySet();
            for (String key :
                    keySet) {
                outLine = String.format("%-15s\t%4s\t%4s\n", key, exposure.get(key), touch.get(key).size());
                System.out.print(outLine);
                writer.write(outLine);
            }
            reader.close();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e1) {
                }
            }
        }
    }

}
