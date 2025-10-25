package work.lcod.kernel.cli;

import picocli.CommandLine;

/**
 * Entry point for the {@code java -jar} distribution.
 */
public final class Main {
    private Main() {}

    public static void main(String[] args) {
        int exitCode = new CommandLine(new LcodRunCommand())
            .setExecutionExceptionHandler(new ShortErrorHandler())
            .execute(args);
        System.exit(exitCode);
    }
}
