import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Pattern;

public class createRelease {
    public static void main(String[] args) throws IOException, InterruptedException {

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

        var branch = branch();
        if (!force && !branch.equals("main")) {
            System.err.println("CreateRelease failed: Can only make a release from main branch, currently on %s branch"
                    .formatted(branch));
            System.exit(1);
        }

        if (!force && !isClean()) {
            System.err.println("CreateRelease failed: local git repo not clean");
            System.exit(1);
        }

        var local = commitHash("HEAD");
        var remote = commitHash(String.format("origin/%s", branch));
        if (!force && !local.equals(remote)) {
            System.err.println("CreateRelease failed: local branch %1$s not equal to origin/%1$s".formatted(branch));
            System.exit(1);
        }

        Version releaseVersion;
        if (version == null) {
            var tagVer = parseVersion(getLatestTag());
            releaseVersion = new Version(tagVer.major, tagVer.minor + 1, 0);
        } else {
            releaseVersion = parseVersion(version);
        }

        var releaseBranch = String.format("release/v%d.%d", releaseVersion.major, releaseVersion.minor);

        if (dryRun) {
            System.out.println("dry run: createRelease would have created %s tag and %s branch on commit %s".formatted(releaseVersion, releaseBranch, local));
        } else {
            System.out.println("Creating release tag %s".formatted(releaseVersion));
            runCommand("git", "tag", "-a", releaseVersion.toString(), "-m", "Release version %s".formatted(releaseVersion));
            System.out.println("Creating release branch %s".formatted(releaseBranch));
            runCommand("git", "branch", releaseBranch);
          
            if (pushOrigin) {
                System.out.println("pushing %s tag and %s branch to origin".formatted(releaseVersion, releaseBranch));
                runCommand("git", "push", "origin", releaseVersion.toString());
                runCommand("git", "push", "origin", releaseBranch);
            }
        }
    }

    public static boolean isClean() throws IOException, InterruptedException {
        var result = runCommand("git", "status", "--porcelain");
        return result.exitCode() == 0 && result.stdout().isBlank();
    }

    public static String branch() throws IOException, InterruptedException {
        var result = runCommand("git", "rev-parse", "--abbrev-ref", "HEAD");
        if (result.exitCode() != 0) {
            throw new RuntimeException(String.format("exit code %d %s", result.exitCode()));
        }
        return result.stdout();
    }

    public static String commitHash(String commit) throws IOException, InterruptedException {
        var result = runCommand("git", "rev-parse", commit);
        if (result.exitCode() != 0) {
            throw new RuntimeException(String.format("exit code %d %s", result.exitCode(), result.stderr()));
        }
        return result.stdout();
    }

    public static String getLatestTag() throws IOException, InterruptedException {
        var result = runCommand("git", "describe", "--abbrev=0", "--tags");
        if (result.exitCode() != 0) {
            throw new RuntimeException(String.format("exit code %d %s", result.exitCode(), result.stderr()));
        }
        return result.stdout();
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

    record Version(int major, int minor, int patch) {
        @Override
        public final String toString() {
            return String.format("%d.%d.%d", major(), minor(), patch());
        }
    }

    record CommandResult(int exitCode, String stdout, String stderr) {
    }

    static CommandResult runCommand(String... command) throws IOException, InterruptedException {
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
