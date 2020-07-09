package org.apache.doris.http.controller;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.http.HttpAuthManager;
import org.apache.doris.http.exception.UnauthorizedException;
import org.apache.doris.mysql.privilege.PaloPrivilege;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.http.HttpAuthManager.SessionValue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class BaseController {

    private static final Logger LOG = LogManager.getLogger(BaseController.class);

    public static final String PALO_SESSION_ID = "PALO_SESSION_ID";
    private static final int PALO_SESSION_EXPIRED_TIME = 3600 * 24; // one day

    // We first check cookie, if not admin, we check http's authority header
    public boolean checkAuthWithCookie(HttpServletRequest request, HttpServletResponse response) {
        if (!needPassword()) {
            return true;
        }
        if (checkCookie(request, response)) {
            return true;
        }
        // cookie is invalid.
        ActionAuthorizationInfo authInfo;
        try {
            authInfo = getAuthorizationInfo(request);
            UserIdentity currentUser = checkPassword(authInfo);
            if (needAdmin()) {
                checkGlobalAuth(currentUser, PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ADMIN_PRIV,
                        PaloPrivilege.NODE_PRIV), CompoundPredicate.Operator.OR));
            }
            SessionValue value = new SessionValue();
            value.currentUser = currentUser;
            addSession(request, response, value);

            ConnectContext ctx = new ConnectContext(null);
            ctx.setQualifiedUser(authInfo.fullUserName);
            ctx.setRemoteIP(authInfo.remoteIp);
            ctx.setCurrentUserIdentity(currentUser);
            ctx.setCatalog(Catalog.getCurrentCatalog());
            ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
            ctx.setThreadLocalInfo();

            return true;
        } catch (UnauthorizedException e) {
            //response.appendContent("Authentication Failed. <br/> " + e.getMessage());
            Map<String,Object> map = new HashMap<>();
            map.put("code",500);
            map.put("msg","Authentication Failed.");
            return false;
        }
    }

    protected void addSession(HttpServletRequest request, HttpServletResponse response, SessionValue value) {
        String key = UUID.randomUUID().toString();
        Cookie cookie = new Cookie(PALO_SESSION_ID, key);
        cookie.setMaxAge(PALO_SESSION_EXPIRED_TIME);
        response.addCookie(cookie);
        HttpAuthManager.getInstance().addSessionValue(key, value);
    }


    private boolean checkCookie(HttpServletRequest request, HttpServletResponse response) {
        String sessionId = getCookieValue(request,PALO_SESSION_ID,response);
        HttpAuthManager authMgr = HttpAuthManager.getInstance();
        if (!Strings.isNullOrEmpty(sessionId)) {
            SessionValue sessionValue = authMgr.getSessionValue(sessionId);
            if (sessionValue == null) {
                return false;
            }
            if (Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(sessionValue.currentUser,
                    PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ADMIN_PRIV,
                            PaloPrivilege.NODE_PRIV),
                            CompoundPredicate.Operator.OR))) {
                updateCookieAge(request, PALO_SESSION_ID, PALO_SESSION_EXPIRED_TIME,response);

                ConnectContext ctx = new ConnectContext(null);
                ctx.setQualifiedUser(sessionValue.currentUser.getQualifiedUser());
                ctx.setRemoteIP(request.getRemoteHost());
                ctx.setCurrentUserIdentity(sessionValue.currentUser);
                ctx.setCatalog(Catalog.getCurrentCatalog());
                ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
                ctx.setThreadLocalInfo();
                return true;
            }
        }
        return false;
    }

    public String getCookieValue(HttpServletRequest request, String cookieName, HttpServletResponse response){
        Cookie[] cookies = request.getCookies();
        if(cookies != null) {
            for (Cookie cookie : cookies) {
                if (cookie.getName() != null && cookie.getName().equals(cookieName)) {
                    String sessionId = cookie.getValue();
                    return sessionId;
                }
            }
        }
        return null;
    }


    public void updateCookieAge(HttpServletRequest request, String cookieName, int age,HttpServletResponse response) {
        Cookie[] cookies = request.getCookies();
        for(Cookie cookie :cookies){
            if (cookie.getName() != null && cookie.getName().equals(cookieName)) {
                cookie.setMaxAge(age);
                response.addCookie(cookie);
            }
        }

    }

    // return true if this Action need to check password.
    // Currently, all sub actions need to check password except for MetaBaseAction.
    // if needPassword() is false, then needAdmin() should also return false
    public boolean needPassword() {
        return true;
    }

    // return true if this Action need Admin privilege.
    public boolean needAdmin() {
        return true;
    }


    public static class ActionAuthorizationInfo {
        public String fullUserName;
        public String remoteIp;
        public String password;
        public String cluster;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("user: ").append(fullUserName).append(", remote ip: ").append(remoteIp);
            sb.append(", password: ").append(password).append(", cluster: ").append(cluster);
            return sb.toString();
        }
    }

    protected void checkGlobalAuth(UserIdentity currentUser, PrivPredicate predicate) throws UnauthorizedException {
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(currentUser, predicate)) {
            throw new UnauthorizedException("Access denied; you need (at least one of) the "
                    + predicate.getPrivs().toString() + " privilege(s) for this operation");
        }
    }

    protected void checkDbAuth(UserIdentity currentUser, String db, PrivPredicate predicate)
            throws UnauthorizedException {
        if (!Catalog.getCurrentCatalog().getAuth().checkDbPriv(currentUser, db, predicate)) {
            throw new UnauthorizedException("Access denied; you need (at least one of) the "
                    + predicate.getPrivs().toString() + " privilege(s) for this operation");
        }
    }

    protected void checkTblAuth(UserIdentity currentUser, String db, String tbl, PrivPredicate predicate)
            throws UnauthorizedException {
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(currentUser, db, tbl, predicate)) {
            throw new UnauthorizedException("Access denied; you need (at least one of) the "
                    + predicate.getPrivs().toString() + " privilege(s) for this operation");
        }
    }

    // return currentUserIdentity from Doris auth
    protected UserIdentity checkPassword(ActionAuthorizationInfo authInfo)
            throws UnauthorizedException {
        List<UserIdentity> currentUser = Lists.newArrayList();
        if (!Catalog.getCurrentCatalog().getAuth().checkPlainPassword(authInfo.fullUserName,
                authInfo.remoteIp, authInfo.password, currentUser)) {
            throw new UnauthorizedException("Access denied for "
                    + authInfo.fullUserName + "@" + authInfo.remoteIp);
        }
        Preconditions.checkState(currentUser.size() == 1);
        return currentUser.get(0);
    }

    public ActionAuthorizationInfo getAuthorizationInfo(HttpServletRequest request)
            throws UnauthorizedException {
        ActionAuthorizationInfo authInfo = new ActionAuthorizationInfo();
        if (!parseAuthInfo(request, authInfo)) {
            LOG.info("parse auth info failed, Authorization header {}, url {}",
                    request.getHeaders("Authorization"), request.getRequestURI());
            throw new UnauthorizedException("Need auth information.");

        }
        LOG.debug("get auth info: {}", authInfo);
        return authInfo;
    }

    private boolean parseAuthInfo(HttpServletRequest request, ActionAuthorizationInfo authInfo) {
        String encodedAuthString = request.getAttribute("Authorization").toString();
        int index = encodedAuthString.indexOf(":");
        authInfo.fullUserName = encodedAuthString.substring(0, index);
        final String[] elements = authInfo.fullUserName.split("@");
        if (elements != null && elements.length < 2) {
            authInfo.fullUserName = ClusterNamespace.getFullName(SystemInfoService.DEFAULT_CLUSTER,
                    authInfo.fullUserName);
            authInfo.cluster = SystemInfoService.DEFAULT_CLUSTER;
        } else if (elements != null && elements.length == 2) {
            authInfo.fullUserName = ClusterNamespace.getFullName(elements[1], elements[0]);
            authInfo.cluster = elements[1];
        }
        authInfo.password = encodedAuthString.substring(index + 1);
        authInfo.remoteIp = request.getRemoteHost();

        return true;
    }

    protected int checkIntParam(String strParam) {
        return Integer.parseInt(strParam);
    }

    protected long checkLongParam(String strParam) {
        return Long.parseLong(strParam);
    }

}