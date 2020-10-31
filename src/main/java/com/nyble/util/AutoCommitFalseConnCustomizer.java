package com.nyble.util;

import com.mchange.v2.c3p0.AbstractConnectionCustomizer;

import java.sql.Connection;

public class AutoCommitFalseConnCustomizer extends AbstractConnectionCustomizer {

    @Override
    public void onCheckOut(Connection c, String parentDataSourceIdentityToken) throws Exception {
        super.onCheckOut(c, parentDataSourceIdentityToken);
        c.setAutoCommit(false);
    }
}
