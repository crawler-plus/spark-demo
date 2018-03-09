package site.it4u.project.utils;

import com.ggstar.util.ip.IpHelper;

/**
 * IP解析工具类
 */
public class IpUtils {
    public static String getCity(String ip) {
        String city = IpHelper.findRegionByIp(ip);
        return city;
    }
}
