#!/usr/bin/env sh

if [ "${NO_RUN}" != "1" ] && [ "${NO_RUN}" != "true" ]; then

    set -ex

    CARGO=cargo
    if [ "${CROSS}" = "1" ]; then
        export CARGO_NET_RETRY=5
        export CARGO_NET_TIMEOUT=10

        cargo install cross --git "https://github.com/cross-rs/cross" --rev "c7dee4d008475ce1c140773cbcd6078f4b86c2aa"
        CARGO=cross

        cargo clean
    fi

    # If a test crashes, we want to know which one it was.
    export RUST_TEST_THREADS=1
    export RUST_BACKTRACE=1

    # test monoio mod
    cd "${PROJECT_DIR}"/monoio

    # default features
    "${CARGO}" test --target "${TARGET}" --no-default-features --features "async-cancel,bytes,macros,utils"
    "${CARGO}" test --target "${TARGET}" --no-default-features --features "async-cancel,bytes,macros,utils" --release

    # enable sync
    "${CARGO}" test --target "${TARGET}" --no-default-features --features "async-cancel,bytes,macros,utils,sync"

    # TODO: fix linker error on loongarch64
    if [ "${TARGET}" != "loongarch64-unknown-linux-gnu" ]; then
        "${CARGO}" test --target "${TARGET}" --no-default-features --features "async-cancel,bytes,macros,utils,sync" --release
    fi

    if [ "${TARGET}" = "x86_64-unknown-linux-gnu" ] || [ "${TARGET}" = "i686-unknown-linux-gnu" ]; then
        "${CARGO}" test --target "${TARGET}"
        "${CARGO}" test --target "${TARGET}" --release
    fi

    if [ "${CHANNEL}" = "nightly" ] && ([ "${TARGET}" = "x86_64-unknown-linux-gnu" ] || [ "${TARGET}" = "i686-unknown-linux-gnu" ]); then
        "${CARGO}" test --target "${TARGET}" --all-features
        "${CARGO}" test --target "${TARGET}" --all-features --release
    fi

    # todo maybe we should test examples here ?
fi
