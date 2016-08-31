package com.apache.flume.filesource.util;

import com.apache.flume.filesource.common.Constants;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

/**
 * @author XuYexin
 * @Description :
 * @date 2016/8/10
 */
public class IpUtil {
    /**
     * 获取内网地址
     * @return
     */
    public static String getLocalIp() {
        Enumeration<NetworkInterface> netInterfaces = null;
        try {
            netInterfaces = NetworkInterface.getNetworkInterfaces();
        } catch (final SocketException ex) {

        }
        String localIpAddress = Constants.DEFAULT_LOCAL_IP;
        while (netInterfaces.hasMoreElements()) {
            NetworkInterface netInterface = netInterfaces.nextElement();
            Enumeration<InetAddress> ipAddresses = netInterface.getInetAddresses();
            while (ipAddresses.hasMoreElements()) {
                InetAddress ipAddress = ipAddresses.nextElement();
                if (isLocalIpAddress(ipAddress)) {
                    localIpAddress = ipAddress.getHostAddress();
                    System.out.println("private: " + localIpAddress);
                }
            }
        }
        return localIpAddress;
    }

    private static boolean isLocalIpAddress(final InetAddress ipAddress) {
        return ipAddress.isSiteLocalAddress() && !ipAddress.isLoopbackAddress() && !isV6IpAddress(ipAddress);
    }

    private static boolean isV6IpAddress(final InetAddress ipAddress) {
        return ipAddress.getHostAddress().contains(":");
    }
}
