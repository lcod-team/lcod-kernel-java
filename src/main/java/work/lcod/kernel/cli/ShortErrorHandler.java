package work.lcod.kernel.cli;

import picocli.CommandLine;

/**
 * Keeps CLI failures short and focused on the root cause.
 */
final class ShortErrorHandler implements CommandLine.IExecutionExceptionHandler {
    @Override
    public int handleExecutionException(
        Exception ex,
        CommandLine commandLine,
        CommandLine.ParseResult parseResult
    ) {
        String message = ex.getMessage();
        if (message == null || message.isBlank()) {
            message = ex.getClass().getSimpleName();
        }
        commandLine.getErr().println(commandLine.getColorScheme().errorText(message));
        if (Boolean.getBoolean("lcod.debug")) {
            ex.printStackTrace(commandLine.getErr());
        }
        return commandLine.getCommandSpec().exitCodeOnExecutionException();
    }
}
