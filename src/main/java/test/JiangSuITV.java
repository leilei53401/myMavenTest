package test;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import tools.CommonTools;
import tools.DbUtil;
import tools.IPPoolUtil;
import tools.Nets;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JiangSuITV {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		File file = new File("d:\\tmp\\jiangsu.txt");
		// 处理省份
		Map<String, String> map = DbUtil.getProvinceCodeByProName("江苏");
		String pid = map.get("pid");
		String pcode = map.get("code");
		System.out.println("pid:" + pid + ",pcode:" + pcode);
		// 处理地市，获取地市缓存
		Map<String, Map<String, String>> cityMap = DbUtil.getCityCodeByPid(pid);
		Map<String, String> tmpMap = cityMap.get("南京");
		System.out.println("city test : " + tmpMap.toString());

		String currCity = "";
		String cityId = "";
		String cityCode = "";
		
		LineIterator it = null;
		String line = "";
		
		List<HashMap<String,String>> ipList = new ArrayList<HashMap<String,String>>();
		
		HashMap<String,String> ipMap = null;

		try {
			it = FileUtils.lineIterator(file, "UTF-8");
			while (it.hasNext()) {
				line = it.nextLine();
				
				if (line.indexOf("/") > 0) {
					// ip和掩码
					int pos = line.indexOf("/");
					String ip = line.substring(0, pos);
					String mask = line.substring(pos+1);
					int intMask = Integer.parseInt(mask);
					System.out.println("ip=" + ip + " , mask = " + mask);
					Nets nets = IPPoolUtil.getEndIP(ip, intMask);
					String NetSeg1 = nets.getStartIP();
					String NetSeg2 = nets.getEndIP();
					long longIpStart = CommonTools.ipToDecimal(NetSeg1);
					long longIpEnd = CommonTools.ipToDecimal(NetSeg2);
					
					//记录数据
					
					/*
					  `TopoID` int(10) unsigned NOT NULL AUTO_INCREMENT,
					  `OrgSeg1` varchar(30) DEFAULT NULL COMMENT '点分式ip起始地址',
					  `OrgSeg2` varchar(30) DEFAULT NULL COMMENT '点分式ip结束地址',
					  `Mask` int(10) unsigned DEFAULT NULL COMMENT '掩码值',
					  `NetSeg1` varchar(30) DEFAULT NULL COMMENT '计算后点分式ip小值',
					  `NetSeg2` varchar(30) DEFAULT NULL COMMENT '计算后点分式ip大值',
					  `NetSeg1Dec` double(20,0) DEFAULT NULL COMMENT '十进制ip小值',
					  `NetSeg2Dec` double(20,0) DEFAULT NULL COMMENT '十进制ip大值',
					  `ProvinceID` int(10) unsigned NOT NULL  COMMENT '省id',
					  `ProvinceName` varchar(30) DEFAULT NULL COMMENT '省份名称',
					  `ProvinceCode` int(10) unsigned DEFAULT NULL COMMENT '省份编号',
					  `AreaID` int(10) unsigned NOT NULL COMMENT '市id',
					  `AreaName` varchar(30) DEFAULT NULL COMMENT '地市名称',
					  `AreaCode` int(10) unsigned DEFAULT NULL COMMENT '地市编码',
					  `ven` varchar(20) DEFAULT NULL COMMENT 'B2B厂家客户编号',
					  `createtime` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间',
					 */
					
					//记录每一条数据
					ipMap = new HashMap<String,String>();
					//原始ip数据
					ipMap.put("OrgSeg1", ip);
					ipMap.put("OrgSeg2", "");
					ipMap.put("Mask", mask);
					//转化后IP
					ipMap.put("NetSeg1", NetSeg1);
					ipMap.put("NetSeg2", NetSeg2);
					//转化为十进制后ip
					ipMap.put("NetSeg1Dec", longIpStart+"");
					ipMap.put("NetSeg2Dec", longIpEnd+"");
					//处理地域
					ipMap.put("ProvinceID", pid);
					ipMap.put("ProvinceName", "江苏");
					ipMap.put("ProvinceCode", pcode);
					ipMap.put("AreaID", cityId);
					ipMap.put("AreaName", currCity);
					ipMap.put("AreaCode", cityCode);
					ipMap.put("ven", "900109");
					
					System.out.println("======result=========:"+ ipMap.toString());
					
					ipList.add(ipMap);
					
				

				} else {
					// 地市名称
					currCity = line.trim();
					Map<String, String> oneMap = cityMap.get(currCity);
					cityId = oneMap.get("areaid");
					cityCode = oneMap.get("code");
					System.out.println("currCity=" + currCity + " , cityId = "
							+ cityId + ", cityCode=" + cityCode);
					continue;

				}
			
			}
			
			//导入数据库
			DbUtil.insertJiangsuItvIpDatabases(ipList);
		
		} catch (Exception e) {
			System.out.println("err line = " + line);
			e.printStackTrace();
		} finally {
			LineIterator.closeQuietly(it);
		}

	}
}
