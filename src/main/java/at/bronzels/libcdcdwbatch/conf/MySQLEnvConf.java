package at.bronzels.libcdcdwbatch.conf;

/**
 * @author Simo
 */
public class MySQLEnvConf {
    private String connUri;
    private String userName;
    private String password;
    private String dbName;

    public MySQLEnvConf(String connUri, String userName, String password, String dbName) {
        this.connUri = connUri;
        this.userName = userName;
        this.password = password;
        this.dbName = dbName;
    }

    public String getConnUri() {
        return connUri;
    }

    public void setConnUri(String connUri) {
        this.connUri = connUri;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
}