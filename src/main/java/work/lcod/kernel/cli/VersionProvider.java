package work.lcod.kernel.cli;

import picocli.CommandLine;

final class VersionProvider implements CommandLine.IVersionProvider {
    @Override
    public String[] getVersion() {
        String implementationVersion = Main.class.getPackage().getImplementationVersion();
        String version = implementationVersion != null ? implementationVersion : "development";
        return new String[] { "lcod-run (java) " + version };
    }
}
