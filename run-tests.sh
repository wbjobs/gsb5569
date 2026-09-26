#!/bin/sh
# Builds all sources under src/ with javac (JDK 8 language level, no external
# dependencies) and runs the self-contained test suite in TestMain.
set -e
cd "$(dirname "$0")"

if [ -n "$JAVA_HOME" ] && [ -x "$JAVA_HOME/bin/javac" ]; then
    JAVAC="$JAVA_HOME/bin/javac"
    JAVA="$JAVA_HOME/bin/java"
else
    JAVAC=javac
    JAVA=java
fi

if ! command -v "$JAVAC" >/dev/null 2>&1; then
    echo "error: javac not found; please install a JDK 8+ or set JAVA_HOME" >&2
    exit 1
fi

OUT=build/classes
rm -rf build
mkdir -p "$OUT"

SOURCES=$(find src -name '*.java' | sort)

if "$JAVAC" --help 2>&1 | grep -q -- '--release'; then
    "$JAVAC" --release 8 -Xlint:all -d "$OUT" $SOURCES
else
    "$JAVAC" -source 1.8 -target 1.8 -Xlint:all -d "$OUT" $SOURCES
fi

"$JAVA" -cp "$OUT" com.gsb.quota.TestMain
