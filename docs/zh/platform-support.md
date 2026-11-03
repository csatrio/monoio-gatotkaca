---
title: 平台支持
date: 2021-11-24 20:00:00
author: ihciah
---

# 平台支持

Monoio 仅支持 Linux，网络和文件 IO 均只使用 io_uring。内核需要支持 io_uring 以及 Monoio 使用到的所有 opcode（Linux 5.6+；部分可选特性如 `splice`、`mkdirat`、`unlinkat`、`renameat`、`symlinkat` 需要更新的内核，详见 `Cargo.toml` 中的说明）。可以在运行时通过 `monoio::utils::detect_uring()` 检查。

io_uring 需要的 memlock 限制参见 [memlock](/docs/zh/memlock.md)。

如果你想在 macOS 或 Windows 上开发，在 Linux 上部署，可以使用 Linux 虚拟机、容器或 WSL。
