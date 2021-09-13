package com.mware.core.security;

import com.mware.core.user.User;

public class NopAuditService implements AuditService {
    @Override
    public void auditLogin(User user) {

    }

    @Override
    public void auditLogout(String userId) {

    }

    @Override
    public void auditAccessDenied(String message, User user, Object resourceId) {

    }

    @Override
    public void auditGenericEvent(User user, String workspaceId, AuditEventType type, String name, String value) {

    }
}
