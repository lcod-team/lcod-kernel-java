package work.lcod.kernel.flow;

/**
 * Lightweight runtime exception used internally to emulate break/continue semantics.
 */
public final class FlowSignalException extends RuntimeException {
    private final FlowSignal signal;

    private FlowSignalException(FlowSignal signal) {
        super(signal.name());
        this.signal = signal;
    }

    public static FlowSignalException of(FlowSignal signal) {
        return new FlowSignalException(signal);
    }

    public static FlowSignalException continueSignal() {
        return of(FlowSignal.CONTINUE);
    }

    public static FlowSignalException breakSignal() {
        return of(FlowSignal.BREAK);
    }

    public FlowSignal signal() {
        return signal;
    }
}
