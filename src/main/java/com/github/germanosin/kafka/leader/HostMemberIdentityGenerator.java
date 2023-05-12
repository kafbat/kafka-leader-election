package com.github.germanosin.kafka.leader;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Collections;
import java.util.Enumeration;

public class HostMemberIdentityGenerator {

  public static HostMemberIdentity generate() {
    return generate("127.0.0.1");
  }

  public static HostMemberIdentity generate(String defaultAddress) {
    return HostMemberIdentity.builder()
        .host(host(defaultAddress))
        .build();
  }

  private static String host(String defaultAddress) {
    String ipv6 = null;

    try {
      final Enumeration<NetworkInterface> interfaces =
          NetworkInterface.getNetworkInterfaces();
      for (NetworkInterface networkInterface : Collections.list(interfaces)) {
        if (!networkInterface.isLoopback()) {
          final Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
          for (InetAddress inetAddress : Collections.list(inetAddresses)) {
            if (inetAddress instanceof Inet6Address) {
              ipv6 = inetAddress.getHostAddress();
            } else if (inetAddress instanceof Inet4Address) {
              return inetAddress.getHostAddress();
            }
          }
        }
      }

      return ipv6 !=  null ? ipv6 : defaultAddress;

    } catch (Exception e) {
      throw new RuntimeException("Ip address not found", e);
    }
  }
}
