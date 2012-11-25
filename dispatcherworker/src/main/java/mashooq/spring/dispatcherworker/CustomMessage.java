package mashooq.spring.dispatcherworker;

public class CustomMessage {
    private String id;
    private Integer version;
    private String payload;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    @Override
    public String toString() {
        return "Message{" +
                "id='" + id + '\'' +
                ", version=" + version +
                ", payload='" + payload + '\'' +
                '}';
    }
}
