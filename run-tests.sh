#!/usr/bin/env bash
# Compiles all sources under src/ with javac (JDK 8 language level) and runs
# the test suite. No Maven/Gradle/JUnit required.
set -euo pipefail
cd "$(dirname "$0")"

if ! command -v javac >/dev/null 2>&1; then
    echo "error: javac not found; please install a JDK 8 or newer" >&2
    exit 1
fi

OUT=out
rm -rf "$OUT"
mkdir -p "$OUT"

SOURCES=$(find src -name '*.java' | sort)

if javac --help 2>&1 | grep -q -- '--release'; then
    # JDK 9+: pin the compilation to the Java 8 platform.
    javac -encoding UTF-8 --release 8 -d "$OUT" $SOURCES
else
    # JDK 8.
    javac -encoding UTF-8 -source 8 -target 8 -d "$OUT" $SOURCES
fi

java -cp "$OUT" com.gsb.quota.TestRunner
