---
title: Platform Support
date: 2021-11-24 20:00:00
author: ihciah
---

# Platform Support

Monoio supports Linux only, and uses io_uring as its sole IO mechanism for both network and file operations. The kernel must support io_uring and every opcode Monoio uses (Linux 5.6+; some optional features, such as `splice`, `mkdirat`, `unlinkat`, `renameat` and `symlinkat`, require newer kernels, see their notes in `Cargo.toml`). You can check this at runtime with `monoio::utils::detect_uring()`.

See [memlock](/docs/en/memlock.md) for the memlock limit io_uring needs.

If you want to develop on macOS or Windows and deploy on Linux, you can use a Linux VM, a container or WSL.
