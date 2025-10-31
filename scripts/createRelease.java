import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.regex.Pattern;

public class createRelease {

    public static void createNewRelease(ProgramOptions opts) throws IOException, InterruptedException {

    }

    public static void createPatchRelease(ProgramOptions opts) throws IOException, InterruptedException {

    }

    public static void main(String[] args) throws IOException, InterruptedException {

        var opts = ProgramOptions.parseArguments(args);

        if (branch().equals("main")) {

        } else if (branch().startsWith("release/v")) {
            
        }


        var branch = branch();
        if (!opts.force() && !branch.equals("main")) {
            System.err.println("CreateRelease failed: Can only make a release from main branch, currently on %s branch"
                    .formatted(branch));
            System.exit(1);
        }

        if (!opts.force() && !isClean()) {
            System.err.println("CreateRelease failed: local git repo not clean");
            System.exit(1);
        }

        var local = commitHash("HEAD");
        var remote = commitHash(String.format("origin/%s", branch));
        if (!opts.force() && !local.equals(remote)) {
            System.err.println("CreateRelease failed: local branch %1$s not equal to origin/%1$s".formatted(branch));
            System.exit(1);
        }

        Version releaseVersion;
        if (opts.version() == null) {
            var tagVer = Version.parseVersion(getLatestTag());
            releaseVersion = new Version(tagVer.major, tagVer.minor + 1, 0);
        } else {
            releaseVersion = Version.parseVersion(opts.version());
        }

        var releaseBranch = String.format("release/v%d.%d", releaseVersion.major, releaseVersion.minor);

        if (opts.dryRun()) {
            System.out.println("dry run: createRelease would have created %s tag and %s branch on commit %s"
                    .formatted(releaseVersion, releaseBranch, local));
        } else {
            System.out.println("Creating release tag %s".formatted(releaseVersion));
            CommandResult.runCommand("git", "tag", "-a", releaseVersion.toString(), "-m",
                    "Release version %s".formatted(releaseVersion));
            System.out.println("Creating release branch %s".formatted(releaseBranch));
            CommandResult.runCommand("git", "branch", releaseBranch);

            if (opts.pushOrigin()) {
                System.out.println("pushing %s tag and %s branch to origin".formatted(releaseVersion, releaseBranch));
                CommandResult.runCommand("git", "push", "origin", releaseVersion.toString());
                CommandResult.runCommand("git", "push", "origin", releaseBranch);
            }
        }
    }

    private static Boolean _isClean;

    public static boolean isClean() throws IOException, InterruptedException {
        if (_isClean == null) {
            var result = CommandResult.runCommand("git", "status", "--porcelain");
            _isClean = result.exitCode() == 0 && result.stdout().isBlank();
        }
        return Objects.requireNonNull(_isClean);
    }

    private static String _branch;

    public static String branch() throws IOException, InterruptedException {
        if (_branch == null) {
            var result = CommandResult.runCommand("git", "rev-parse", "--abbrev-ref", "HEAD");
            if (result.exitCode() != 0) {
                throw new RuntimeException(String.format("exit code %d %s", result.exitCode()));
            }
            _branch = result.stdout();
        }

        return Objects.requireNonNull(_branch);
    }

    private static String _latestTag;

    public static String getLatestTag() throws IOException, InterruptedException {
        if (_latestTag == null) {
            var result = CommandResult.runCommand("git", "describe", "--abbrev=0", "--tags");
            if (result.exitCode() != 0) {
                throw new RuntimeException(String.format("exit code %d %s", result.exitCode(), result.stderr()));
            }
            _latestTag = result.stdout();
        }
        return Objects.requireNonNull(_latestTag);
    }

    public static String commitHash(String commit) throws IOException, InterruptedException {
        var result = CommandResult.runCommand("git", "rev-parse", commit);
        if (result.exitCode() != 0) {
            throw new RuntimeException(String.format("exit code %d %s", result.exitCode(), result.stderr()));
        }
        return result.stdout();
    }

    record ProgramOptions(
            boolean dryRun,
            boolean pushOrigin,
            boolean force,
            String version) {

        public static ProgramOptions parseArguments(String[] args) {
            boolean dryRun = false;
            boolean pushOrigin = true;
            boolean force = false;
            String version = null;

            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--dry-run":
                        dryRun = true;
                        break;
                    case "--no-push":
                        pushOrigin = false;
                        break;
                    case "--push":
                        pushOrigin = true;
                        break;
                    case "--force":
                        force = true;
                        break;
                    case "--version":
                        version = args[++i];
                        break;
                }
            }

            return new ProgramOptions(dryRun, pushOrigin, force, version);
        }
    }

    record Version(int major, int minor, int patch) {
        @Override
        public final String toString() {
            return String.format("%d.%d.%d", major(), minor(), patch());
        }

        public static Version parseVersion(String tag) {
            var regex = Pattern.compile("v?(\\d+)\\.(\\d+)\\.(\\d+)");
            var matcher = regex.matcher(tag);
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Invalid version format: " + tag);
            }
            int major = Integer.parseInt(matcher.group(1));
            int minor = Integer.parseInt(matcher.group(2));
            int patch = Integer.parseInt(matcher.group(3));
            return new Version(major, minor, patch);
        }
    }

    record CommandResult(int exitCode, String stdout, String stderr) {
        public static CommandResult runCommand(String... command) throws IOException, InterruptedException {
            ProcessBuilder pb = new ProcessBuilder(command);
            pb.redirectErrorStream(false); // keep stdout and stderr separate
            Process process = pb.start();

            // Threads to read stdout and stderr concurrently
            StringBuilder stdout = new StringBuilder();
            StringBuilder stderr = new StringBuilder();

            Thread tOut = new Thread(() -> readStream(process.getInputStream(), stdout));
            Thread tErr = new Thread(() -> readStream(process.getErrorStream(), stderr));

            tOut.start();
            tErr.start();

            int exitCode = process.waitFor();

            tOut.join();
            tErr.join();

            return new CommandResult(exitCode, stdout.toString().trim(), stderr.toString().trim());
        }

        static void readStream(InputStream stream, StringBuilder out) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    out.append(line).append(System.lineSeparator());
                }
            } catch (IOException ignored) {
            }
        }

    }

}
