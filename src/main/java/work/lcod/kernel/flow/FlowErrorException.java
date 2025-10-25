package work.lcod.kernel.flow;

/**
 * Exception carrying LCOD error metadata (code/message/data) for flow helpers.
 */
public final class FlowErrorException extends RuntimeException {
    private final String code;
    private final Object data;

    public FlowErrorException(String code, String message, Object data) {
        super(message);
        this.code = code;
        this.data = data;
    }

    public String code() {
        return code;
    }

    public Object data() {
        return data;
    }
}
