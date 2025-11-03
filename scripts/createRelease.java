import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.regex.Pattern;

public class createRelease {

    public static void createNewRelease(ProgramOptions opts, String local) throws IOException, InterruptedException {
        assert (branch().equals("main"));

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

    public static void createPatchRelease(ProgramOptions opts, String local) throws IOException, InterruptedException {
        assert (branch().startsWith("release/v"));

        if (opts.version() != null) {
            throw new IllegalArgumentException("can't specify version argument when creating a patch release");
        }

        var tagVer = Version.parseVersion(getLatestTag());
        Version releaseVersion = new Version(tagVer.major, tagVer.minor, tagVer.patch + 1);

        if (opts.dryRun()) {
            System.out.println(
                    "dry run: createRelease would have created %s tag on commit %s".formatted(releaseVersion, local));
        } else {
            System.out.println("Creating release tag %s".formatted(releaseVersion));
            CommandResult.runCommand("git", "tag", "-a", releaseVersion.toString(), "-m",
                    "Release version %s".formatted(releaseVersion));

            if (opts.pushOrigin()) {
                System.out.println("pushing %s tag to origin".formatted(releaseVersion));
                CommandResult.runCommand("git", "push", "origin", releaseVersion.toString());
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        var opts = ProgramOptions.parseArguments(args);

        try {
            if (!opts.force() && !isClean()) {
                throw new IllegalArgumentException("local git repo not clean");
            }

            var branch = branch();
            var local = commitHash("HEAD");
            var remote = commitHash(String.format("origin/%s", branch));
            if (!opts.force() && !local.equals(remote)) {
                throw new IllegalArgumentException("local branch %1$s not equal to origin/%1$s".formatted(branch));
            }

            if (opts.main() || branch.equals("main")) {
                createNewRelease(opts, local);
            } else if (opts.release() || branch.startsWith("release/v")) {
                createPatchRelease(opts, local);
            } else {
                throw new IllegalArgumentException(
                        "Can only create releases from main or release branch, currently on %s branch"
                                .formatted(branch));
            }
        } catch (IllegalArgumentException e) {
            System.err.println("createRelease failed: %s".formatted(e.getMessage()));
            System.exit(1);
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
            System.err.println("commitHash failed %d %s".formatted(result.exitCode(), result.stderr()));
            return null;
        }
        return result.stdout();
    }

    record ProgramOptions(
            boolean main,
            boolean release,
            boolean dryRun,
            boolean pushOrigin,
            boolean force,
            String version) {

        public static ProgramOptions parseArguments(String[] args) {

            boolean main = false;
            boolean release = false;
            boolean dryRun = false;
            boolean pushOrigin = false;
            boolean force = false;
            String version = null;

            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--main":
                        main = true;
                        break;
                    case "--release":
                        release = true;
                        break;
                    case "--dry-run":
                        dryRun = true;
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

            return new ProgramOptions(main, release, dryRun, pushOrigin, force, version);
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
