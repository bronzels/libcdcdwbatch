package at.bronzels.libcdcdwbatch.conf;

/**
 * @author Simo
 */
public class MongoEnvConf {
    private String url;

    public MongoEnvConf(String url) {
        this.url = url;
    }

    public String getUrl() {
        return this.url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
