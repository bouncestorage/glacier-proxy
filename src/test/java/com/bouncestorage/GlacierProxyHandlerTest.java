package com.bouncestorage;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class GlacierProxyHandlerTest {
    @Test
    public void testVaultsRE() {
        assertThat("/123456789123/vaults").matches(GlacierProxyHandler.VAULTS_RE);
        assertThat("/-/vaults").matches(GlacierProxyHandler.VAULTS_RE);
    }
}
