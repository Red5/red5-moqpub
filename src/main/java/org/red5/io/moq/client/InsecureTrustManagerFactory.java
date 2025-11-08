package org.red5.io.moq.client;

import io.netty.handler.ssl.util.SimpleTrustManagerFactory;

import javax.net.ssl.*;
import java.security.KeyStore;
import java.security.cert.X509Certificate;

/**
 * Insecure trust manager that accepts all certificates.
 * WARNING: Only use for development/testing!
 */
public class InsecureTrustManagerFactory extends SimpleTrustManagerFactory {
    public static final TrustManagerFactory INSTANCE = new InsecureTrustManagerFactory();

    private static final TrustManager TRUST_MANAGER = new X509TrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
            // Accept all
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
            // Accept all
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    };

    @Override
    protected void engineInit(KeyStore keyStore) {
        // No initialization needed
    }

    @Override
    protected void engineInit(ManagerFactoryParameters managerFactoryParameters) {
        // No initialization needed
    }

    @Override
    protected TrustManager[] engineGetTrustManagers() {
        return new TrustManager[]{TRUST_MANAGER};
    }
}
