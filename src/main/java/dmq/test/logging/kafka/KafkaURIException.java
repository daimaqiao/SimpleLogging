package dmq.test.logging.kafka;

// 表示kafka uri格式不正确
public class KafkaURIException extends Exception {
    private final String errorMessage;
    public String getErrorMessage() {
        return errorMessage;
    }

    public KafkaURIException(String badUri) {
        this(badUri, (Exception)null);
    }
    public KafkaURIException(String badUri, String message) {
        super(String.format("[%s] %s", badUri, message));
        errorMessage= super.getMessage();
    }
    public KafkaURIException(String badUri, Throwable throwable) {
        super(String.format("Bad kafka uri (uri= %s).", badUri), throwable);
        errorMessage= super.getMessage();
    }

}
