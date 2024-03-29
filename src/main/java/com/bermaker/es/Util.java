package com.bermaker.es;

public final class Util {

  public static void setSSLTrustStore(String storeFilePath, String storePassword) {
    System.setProperty("javax.net.ssl.trustStore", storeFilePath);
    System.setProperty("javax.net.ssl.trustStorePassword", storePassword);
  }
}
