package work.lcod.kernel.flow;

import java.util.LinkedHashMap;
import java.util.Map;

final class FlowErrorUtils {
    private FlowErrorUtils() {}

    static Map<String, Object> normalize(Throwable error) {
        if (error instanceof FlowSignalException) {
            throw (FlowSignalException) error;
        }
        if (error instanceof FlowErrorException fe) {
            return toMap(fe.code(), fe.getMessage(), fe.data());
        }
        if (error == null) {
            return toMap("unexpected_error", "Unexpected error", null);
        }
        var message = error.getMessage() != null && !error.getMessage().isBlank()
            ? error.getMessage()
            : "Unexpected error";
        return toMap("unexpected_error", message, null);
    }

    private static Map<String, Object> toMap(String code, String message, Object data) {
        var map = new LinkedHashMap<String, Object>();
        map.put("code", code);
        map.put("message", message);
        if (data != null) {
            map.put("data", data);
        }
        return map;
    }
}
