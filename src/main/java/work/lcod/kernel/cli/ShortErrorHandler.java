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
        commandLine.getErr().println(commandLine.getColorScheme().errorText(ex.getMessage()));
        if (Boolean.getBoolean("lcod.debug")) {
            ex.printStackTrace(commandLine.getErr());
        }
        return commandLine.getCommandSpec().exitCodeOnExecutionException();
    }
}
