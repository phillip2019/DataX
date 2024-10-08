package com.alibaba.datax.plugin.reader.hivereader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.Callable;

import static com.alibaba.datax.plugin.reader.hivereader.Constant.MAX_TRY_TIMES;

/**
 * @author daixinxuan
 * @date 2019/10/29 10:54
 */
public class DBUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DBUtil.class);

    public static final String HIVE_CONF_FORMAT = "hiveconf:%s";
    private DBUtil() {}

    public static ResultSet query(Connection conn, String sql)
            throws SQLException {
        // 默认3600 s 的query Timeout
        return query(conn, sql, Constant.SOCKET_TIMEOUT_INSECOND);
    }

    private static ResultSet query(Connection conn, String sql, int queryTimeout)
            throws SQLException {
        // make sure autocommit is off
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        return query(stmt, sql);
    }

    public static ResultSet query(Statement stmt, String sql)
            throws SQLException {
        return stmt.executeQuery(sql);
    }

    public static Connection getConnection(final String url, final String user, final String pass,
                                           final Configuration taskConfig) {
        try {
            return RetryUtil.executeWithRetry(new Callable<Connection>() {
                @Override
                public Connection call() throws Exception {
                    return DBUtil.connect(url, user, pass, taskConfig);
                }
            }, MAX_TRY_TIMES, 1000L, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    HiveReaderErrorCode.CONN_DB_ERROR,
                    String.format("数据库连接失败. 因为根据您配置的连接信息:%s获取数据库连接失败. 请检查您的配置并作出修改.", url), e);
        }

    }

    private static synchronized Connection connect(String url, String user, String pass, Configuration taskConfig) {
        Properties prop = new Properties();
        prop.put("user", user);
        prop.put("password", pass);

        return connect(url, prop, taskConfig);
    }

    private static Connection connect(String url, Properties prop, Configuration taskConfig) {
        boolean haveKerberos = taskConfig.getBool(Key.HAVE_KERBEROS, false);
        if (haveKerberos) {
            String kerberosKeytabFilePath = taskConfig.getString(Key.KERBEROS_KEYTAB_FILE_PATH);
            String kerberosPrincipal = taskConfig.getString(Key.KERBEROS_PRINCIPAL);
            kerberosAuthentication(kerberosPrincipal, kerberosKeytabFilePath);
        }

        // 设置应用名和执行引擎
        String hiveExecutionEngine = taskConfig.getString(Key.HIVE_EXECUTION_ENGINE, "mr");
        prop.put(String.format(HIVE_CONF_FORMAT, Key.HIVE_EXECUTION_ENGINE).replace("_", "."), hiveExecutionEngine);
        String appName = taskConfig.getString(Key.MAPRED_JOB_NAME, "export_hive_data2xx");
        prop.put(String.format(HIVE_CONF_FORMAT, Key.MAPRED_JOB_NAME).replace("_", "."), appName);
        prop.put(String.format(HIVE_CONF_FORMAT, Key.SPARK_APP_NAME).replace("_", "."), appName);
        LOG.info("set hive execution engine: {}", hiveExecutionEngine);
        LOG.info("set mapred job name: {}", appName);
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            DriverManager.setLoginTimeout(Constant.TIMEOUT_SECONDS);
            return DriverManager.getConnection(url, prop);
        } catch (Exception e) {
            throw DataXException.asDataXException(HiveReaderErrorCode.CONN_DB_ERROR, " 具体错误信息为：", e);
        }
    }

    private static void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath) {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
        } catch (IOException e) {
            String message = String.format("kerberos认证失败,请确定kerberosKeytabFilePath[%s]和kerberosPrincipal[%s]填写正确",
                    kerberosKeytabFilePath, kerberosPrincipal);
            throw DataXException.asDataXException(HiveReaderErrorCode.KERBEROS_LOGIN_ERROR, message, e);
        }
    }

    public static void closeDBResources(Statement stmt, Connection conn) {
        closeDBResources(null, stmt, conn);
    }

    public static void closeDBResources(ResultSet rs, Statement stmt,
                                        Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException unused) {
                LOG.error("关闭result set失败，错误详情为: ", unused);
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException unused) {
                LOG.error("关闭Statement失败，错误详情为: ", unused);
            }
        }

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException unused) {
                LOG.error("关闭数据库连接conn失败，错误详情为: ", unused);
            }
        }
    }

}
