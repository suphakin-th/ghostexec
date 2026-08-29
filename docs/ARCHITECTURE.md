# GhostExec — OS-Native Serverless Architecture

> A self-spawning, self-destroying serverless runtime built entirely on raw OS primitives. No cloud dependency. No container runtime. Pure OS.

---

## Table of Contents

1. [Design Philosophy](#design-philosophy)
2. [Architecture Overview](#architecture-overview)
3. [Layer Reference](#layer-reference)
   - [Trigger Layer](#1-trigger-layer)
   - [Orchestration Layer](#2-orchestration-layer)
   - [Concurrent Ingress + Resource Isolation](#concurrent-ingress--resource-isolation)
   - [Spawn Engine](#3-spawn-engine)
   - [Working Memory](#4-working-memory--tmp)
     - [Memory Safety Boundary](#memory-safety-boundary)
   - [Transport Layer](#5-transport-layer)
   - [Execution Layer](#6-execution-layer)
   - [DB Sink Layer](#db-sink-layer)
   - [Self-Destruct Chain](#7-self-destruct-chain)
4. [Race-Freedom: Transfer + DB Write](#race-freedom-transfer--db-write)
5. [Job Lifecycle Workflow](#job-lifecycle-workflow)
6. [Swimming Lane — Actor View](#swimming-lane--actor-view)
7. [Core Primitives Deep Dive](#core-primitives-deep-dive)
   - [Temp Process](#temp-process)
   - [Dynamo Atomic](#dynamo-atomic)
   - [systemd + systemctl](#systemd--systemctl)
   - [sys_tmp](#sys_tmp)
   - [Symlink Router](#symlink-router)
   - [KVM Executor](#kvm-executor)
   - [Master Daemon](#master-daemon)
   - [Master Daemon in Rust](#master-daemon-in-rust)
   - [Prolog Classifier](#prolog-classifier)
   - [Cron Timer](#cron-timer)
   - [SSH / SCP / CP Transport](#ssh--scp--cp-transport)
8. [Self-Spawn Mechanism](#self-spawn-mechanism)
9. [Data Manipulation Pattern](#data-manipulation-pattern)
10. [Cross-Platform Support](#cross-platform-support)
11. [Self-Destruct Implementation](#self-destruct-implementation)

---

## Design Philosophy

GhostExec treats the operating system as the serverless runtime. Every abstraction that a cloud provider normally supplies — ephemeral execution, state management, data routing, secure transport, lifecycle control — is replaced by a raw OS primitive that has been available on every Unix-like system for decades.

**Key properties:**

- **Zero-copy data routing** — payloads are `rename()`'d, never `cp`'d
- **Atomic state** — every state transition uses `O_EXCL` + `rename()` (compare-and-swap on the filesystem)
- **Ephemeral by default** — every workspace lives on a private `tmpfs` mount and is destroyed on completion
- **Self-managing** — jobs spawn themselves via `systemd-run --transient` and destroy themselves via `ExecStopPost=`
- **Declarative routing** — Prolog rules classify jobs and route them to the correct executor without imperative branching
- **Cross-platform** — the same binary registers as a `systemd` service on Linux, a `launchd` plist on macOS, and a Win32 service on Windows

---

## Architecture Overview

```
┌────────────────────────────────────────────────────────┐
│                    TRIGGER LAYER                       │
│        Cron timer │ SSH event │ File watch │ API signal│
└────────────────────────────────┬───────────────────────┘
                                 │
                                 ▼
┌────────────────────────────────────────────────────────┐
│                 ORCHESTRATION LAYER                    │
│   Master daemon (watchdog)  │  Prolog classifier       │
└───────────────┬────────────────────────────────────────┘
                │
        ┌───────┼───────┐
        ▼       ▼       ▼
┌──────────┐ ┌──────┐ ┌────────────────┐
│ Fork/exec│ │ KVM  │ │systemd transient│   ← SPAWN ENGINE
└──────────┘ └──────┘ └────────────────┘
        │       │           │
        └───────┴───────────┘
                │
                ▼
┌────────────────────────────────────────────────────────┐
│       WORKING MEMORY  /tmp/{uuid}/                     │
│   sys_tmp (O_TMPFILE)│Dynamo atomic│Symlink router     │
└────────────────────────────────┬───────────────────────┘
                                 │
                                 ▼
┌────────────────────────────────────────────────────────┐
│                   TRANSPORT LAYER                      │
│        SSH config bypass │ SCP remote │ CP local       │
└────────────────────────────────┬───────────────────────┘
                                 │
                                 ▼
┌────────────────────────────────────────────────────────┐
│                  EXECUTION LAYER                       │
│         Linux systemd │ macOS launchd │ Windows svc    │
└────────────────────────────────┬───────────────────────┘
                                 │
                                 ▼
┌────────────────────────────────────────────────────────┐
│                  DB SINK LAYER  (optional)              │
│   claim output.json (rename, one winner) │ db_write()   │
│   touch state/PERSISTED  ← gates self-destruct's wait   │
└────────────────────────────────┬───────────────────────┘
                                 │
                                 ▼
┌────────────────────────────────────────────────────────┐
│              SELF-DESTRUCT CHAIN                       │
│   wait state/PERSISTED (bounded) │ kill virtiofsd       │
│   umount tmpfs THEN rm -rf │ systemctl stop │ KVM destroy│
│                 unlink all symlinks                    │
└────────────────────────────────────────────────────────┘
```

---

## Layer Reference

### 1. Trigger Layer

Four event sources wake the master daemon:

| Trigger | Mechanism | OS API |
|---------|-----------|--------|
| **Cron timer** | `systemd .timer` unit or `crond` | `timer_create()`, `SIGALRM` |
| **SSH event** | `ForceCommand` or `AuthorizedKeysCommand` hook in `sshd_config` | `sshd(8)` |
| **File watch** | Directory change notification | `inotify` (Linux), `kqueue` (macOS), `ReadDirectoryChangesW` (Windows) |
| **API signal** | Named pipe or Unix domain socket | `mkfifo(3)`, `socket(2)` |

All triggers deliver a **job descriptor** (JSON) to the master daemon. A single named pipe cannot be the *only* ingress path once concurrency matters: a FIFO has one reader, so throughput is capped at how fast one `read()` loop can drain it, and it cannot bind to multiple network ports at once. `/run/ghostexec/jobs.fifo` remains the ingress for local/SSH-hook triggers (low volume, no port needed), but network-facing triggers (API signal) bind directly as `epoll`-registered listener sockets — see [Concurrent Ingress](#concurrent-ingress--resource-isolation) below for how the daemon accepts from many sources at once without funneling everything through one pipe.

**Example job descriptor:**

```json
{
  "job_id": "7f3a9c21-...",
  "executor": "auto",
  "isolation": "namespace",
  "memory_mb": 256,
  "payload_path": "/var/spool/ghostexec/incoming/payload.tar.gz",
  "target": "localhost",
  "timeout_s": 30
}
```

---

### 2. Orchestration Layer

#### Master Daemon

The master daemon is a persistent process (`Type=notify` systemd service) that:

1. Polls `/run/ghostexec/jobs.fifo` for incoming job descriptors
2. Validates the descriptor — including a **size check before parsing**: POSIX only guarantees atomic, non-interleaved writes to a pipe up to `PIPE_BUF` (4096 bytes on Linux). A descriptor at or under that size is safe on a FIFO with multiple concurrent writers by construction — no framing needed. A descriptor that could exceed it (a large embedded payload path list, a wide fan-out batch) must never be written to the FIFO directly, because two concurrent writers near the boundary can interleave and produce a torn, unparseable message with no way to tell which bytes belong to which sender. Anything that might cross `PIPE_BUF` goes over `/run/ghostexec/control.sock` (a `SOCK_STREAM` Unix domain socket) instead, length-prefixed so the reader always knows exactly how many bytes make up one message regardless of how the kernel chooses to chunk the stream:

```c
// Length-prefixed framing over control.sock — required whenever a
// descriptor cannot be guaranteed to fit in one PIPE_BUF-sized write.
// [4-byte big-endian length][JSON descriptor, exactly that many bytes]
uint32_t len_be = htonl((uint32_t)descriptor_len);
write(control_sock_fd, &len_be, sizeof(len_be));
write(control_sock_fd, descriptor_json, descriptor_len);

// Reader side: read exactly 4 bytes, then read exactly that many more —
// never read() into a shared buffer and hope a newline or EOF delimits
// one message, since a stream socket has no message boundaries of its
// own the way a FIFO write under PIPE_BUF does.
```

3. Assigns a UUID and creates `/tmp/{uuid}/state/QUEUED` (atomic, `O_EXCL`)
4. Passes the descriptor to the Prolog classifier
5. Forks the appropriate executor based on classification
6. Monitors child PIDs via `waitpid()` with `SIGCHLD`
7. Writes heartbeats to `/tmp/{uuid}/state/` via atomic `rename()`

```c
// Daemon main loop (simplified) — single ingress path.
// This form is correct for the FIFO alone (one reader is fine when
// there is exactly one writer class: local/SSH-hook triggers) but does
// not scale to multiple concurrent network sources — see below.
while (1) {
    ssize_t n = read(fifo_fd, buf, sizeof(buf));
    if (n > 0) {
        job_t *job = parse_descriptor(buf, n);
        char uuid[37];
        gen_uuid(uuid);
        create_workspace(uuid);          // mkdir + mount tmpfs
        stage_payload(job, uuid);        // rename() payload in
        executor_t exe = classify(job);  // pure Prolog classification
        pid_t pid = spawn(exe, uuid);    // fork/KVM/systemd-run
        track_pid(pid, uuid);
    }
}
```

---

### Concurrent Ingress + Resource Isolation

A blocking `read()` on one FIFO serializes every job descriptor through a single reader — fine for low-volume local triggers, wrong once the daemon must accept from **multiple network sources at the same time** while giving each source a bounded, fair share of CPU and RAM. Two independent, low-level mechanisms solve the two independent problems here; neither alone is sufficient:

| Problem | Wrong tool for it | Right tool | Why |
|---|---|---|---|
| Accepting many concurrent connections fast | More reader threads blocked on `read()` | `epoll` (Linux) / `kqueue` (BSD/macOS) / IOCP (Windows) event loop | One thread (or a small fixed pool) can watch thousands of fds and only wake for the ones with actual data — no thread-per-connection, no busy-polling |
| Stopping one source from starving another of CPU/RAM | Thread priority / `nice` | `cgroups v2` (Linux) per source | A misbehaving or high-volume source gets a **hard ceiling**, not just lower scheduling priority — priority alone still lets it consume everything when nothing else is contending |

These compose cleanly because they operate on different resources: `epoll` governs *which fd gets serviced next* (I/O readiness), `cgroups` governs *how much CPU/RAM a job is allowed to use once it's running* (execution ceiling). Using only `epoll` gets you fast, fair *acceptance* of work but no limit on what that work then consumes. Using only `cgroups` caps consumption but leaves you back at one blocking reader deciding who gets in the door.

```c
// event_loop.c — one thread accepts from N listener sockets + the FIFO
// via epoll; each accepted job is dispatched to a small worker pool so
// no single slow classify()/spawn() call blocks the acceptance of the
// next event. This is the pure-function boundary in practice: the loop
// itself does no job logic — it only routes (fd ready) -> (worker).

int epfd = epoll_create1(0);

// Register every ingress source once, at startup — the FIFO and each
// bound network port are just fds to epoll; it does not care which is
// which.
register_fd(epfd, fifo_fd,        EPOLLIN);
register_fd(epfd, api_tcp_fd,     EPOLLIN);   // e.g. :7420, API signal
register_fd(epfd, control_sock_fd, EPOLLIN);  // /run/ghostexec/control.sock

struct epoll_event events[MAX_EVENTS];
while (1) {
    int n = epoll_wait(epfd, events, MAX_EVENTS, -1);
    for (int i = 0; i < n; i++) {
        int fd = events[i].data.fd;
        // Dispatch to a bounded worker pool — never call classify()/
        // spawn() inline on the epoll thread, or one slow job blocks
        // acceptance of every other ready fd behind it.
        worker_pool_submit(fd);
    }
}
```

**Resource isolation is set once per source, at registration, not per job:**

```bash
# One cgroup per ingress source (e.g. per listener port), created when
# the daemon binds that source. Every job spawned from jobs arriving on
# this source is placed into the same cgroup, so the ceiling applies to
# the source's aggregate load — one noisy or hostile source cannot starve
# jobs that arrived through a different port.

CGROUP="/sys/fs/cgroup/ghostexec/source-${PORT}"
mkdir -p "${CGROUP}"
echo "400M"   > "${CGROUP}/memory.max"    # hard ceiling, not a suggestion
echo "50000 100000" > "${CGROUP}/cpu.max" # 50% of one core, hard quota

# When spawn() forks the worker for a job that arrived on this source:
echo "${WORKER_PID}" > "${CGROUP}/cgroup.procs"
```

`spawn()` in the [Spawn Engine](#3-spawn-engine) section already creates the process (fork/KVM/systemd-run); this only adds one line at spawn time — write the new PID into the source's cgroup — so the existing spawn mechanics are unchanged, they simply happen inside a resource boundary that was established once per source rather than recomputed per job.

#### Prolog Classifier

The Prolog engine evaluates the job descriptor and unifies to an executor type. Rules are defined in `/etc/ghostexec/rules.pl`.

The classifier is written as a **pure, total function**: for any well-formed job descriptor there is exactly one matching clause — never zero, never more than one. This is enforced two ways: (1) each rule guards on a range that partitions the input space (no two guards can both be true for the same descriptor), and (2) every clause head ends in a cut (`!`) so Prolog commits to the first match instead of leaving choice points that a caller could backtrack into and get a second, contradictory answer. A classifier that can return two different answers for the same input is not a function — it is exactly the kind of hidden nondeterminism functional design is supposed to rule out.

**Priority order (first match wins, evaluated top to bottom):**

1. `target \= localhost` → `systemd_transient` — remote execution always needs the service wrapper, regardless of isolation/memory. This is checked first because it's an orthogonal axis (transport) that overrides the choice made on the other two axes (isolation, memory).
2. `isolation = vm` → `kvm_executor`
3. `memory_mb > 512` → `kvm_executor`
4. everything else → `fork_exec`

```prolog
% /etc/ghostexec/rules.pl
% classify/2 is a pure function: classify(+Descriptor, -Executor).
% Total and deterministic — every well-formed Descriptor unifies with
% exactly one Executor. The cut (!) on each clause commits to the first
% matching guard; no clause below it is ever considered once one fires.

classify(Desc, systemd_transient) :-
    get_dict(target, Desc, Target),
    Target \= localhost,
    !.

classify(Desc, kvm_executor) :-
    get_dict(isolation, Desc, vm),
    !.

classify(Desc, kvm_executor) :-
    get_dict(memory_mb, Desc, Mem),
    Mem > 512,
    !.

classify(_Desc, fork_exec).  % total default — always matches, never fails
```

**Why this is safe where the original two-rules-file version was not:** the earlier draft had a bare catch-all (`route(_Desc, fork_exec).`) with no cut, sitting alongside `kvm_executor`/`systemd_transient` clauses that could *also* match the same descriptor (e.g. `isolation=vm` and `target=remote` set together). Without a cut, a caller that backtracks — `findall/3`, or a retry loop, or a future refactor that iterates solutions instead of taking the first one — can walk into a second, different executor for the identical job. The `!` after each guard removes that possibility structurally: once a guard succeeds, Prolog is committed, so `classify/2` behaves as a mathematical function (single input → single output) rather than a search over possibilities that happens to usually look like one.

---

### 3. Spawn Engine

Three execution backends, selected by the Prolog classifier:

#### Fork / Exec (namespace isolation)

```c
#define _GNU_SOURCE
#include <sched.h>
#include <unistd.h>

int flags = CLONE_NEWPID | CLONE_NEWNS | CLONE_NEWNET | CLONE_NEWUTS;

pid_t pid = clone(child_fn, stack_top, flags | SIGCHLD, &args);
if (pid < 0) { perror("clone"); exit(1); }

// Inside child_fn:
// mount the job tmpfs, then execve() the worker binary
mount("tmpfs", "/tmp/{uuid}", "tmpfs", MS_PRIVATE, "size=256m");
execve("/usr/lib/ghostexec/worker", argv, envp);
```

**Isolation strength is not uniform across executors, and that asymmetry is a design decision, not an oversight.** `fork_exec` is the [Prolog classifier](#2-orchestration-layer)'s *default* — the executor most jobs get — yet namespace isolation (`CLONE_NEWPID`/`NEWNS`/`NEWNET`/`NEWUTS`) shares one kernel with the host: an unpatched kernel vulnerability in this path is a host compromise. `kvm_executor` gets a real hardware-virtualized boundary via QEMU; `fork_exec` does not, and cannot, by construction — no amount of additional namespace flags turns a shared kernel into a separate one. If GhostExec runs payloads whose trust level is not fully known — which "serverless" implies — the *default* path being the *weaker* one is backwards from a security posture, and should be either flipped (untrusted-by-default → `kvm_executor` unless a job explicitly opts into the cheaper path) or, at minimum, hardened as far as namespace isolation allows:

```c
// Minimum hardening for the fork_exec default: seccomp filters and
// capability dropping, applied in child_fn BEFORE execve(). Namespaces
// alone isolate *views* of PIDs/mounts/network; they do not restrict
// *which syscalls* the process may issue against the kernel it still
// shares with the host. This closes that gap as far as it can be
// closed without a second kernel — it does not make fork_exec equal
// to kvm_executor, only less exposed than doing nothing.

// 1. Drop every capability the worker does not need. A job that only
// reads its workspace and writes output.json needs none of these.
cap_t caps = cap_get_proc();
cap_clear(caps);
cap_set_proc(caps);

// 2. Install a seccomp allowlist — deny-by-default, permit only the
// syscalls the worker's contract actually requires (read/write/openat
// on paths under /tmp/{uuid}, exit, a small fixed set beyond that).
// Anything not explicitly allowed terminates the process rather than
// executing — the same "total function, no undefined behavior on
// unmatched input" discipline used in the Prolog classifier, applied
// to the syscall boundary instead of the routing boundary.
scmp_filter_ctx ctx = seccomp_init(SCMP_ACT_KILL);
seccomp_rule_add(ctx, SCMP_ACT_ALLOW, SCMP_SYS(read),  0);
seccomp_rule_add(ctx, SCMP_ACT_ALLOW, SCMP_SYS(write), 0);
seccomp_rule_add(ctx, SCMP_ACT_ALLOW, SCMP_SYS(openat),0);
seccomp_rule_add(ctx, SCMP_ACT_ALLOW, SCMP_SYS(close), 0);
seccomp_rule_add(ctx, SCMP_ACT_ALLOW, SCMP_SYS(exit_group), 0);
seccomp_load(ctx);

// 3. Only now execve() — the process that runs job logic has already
// lost every capability and every syscall it wasn't explicitly given.
mount("tmpfs", "/tmp/{uuid}", "tmpfs", MS_PRIVATE, "size=256m");
execve("/usr/lib/ghostexec/worker", argv, envp);
```

The [Prolog classifier](#2-orchestration-layer) can also route on a `trust_level` field the same way it routes on `isolation`/`memory_mb`/`target` — a fourth, orthogonal guard clause (`trust_level = untrusted → kvm_executor`) rather than a special case bolted on elsewhere, keeping the "one clause, one concern, first match wins" structure intact.

#### KVM Executor (VM isolation)

```bash
# Launch QEMU microVM with shared /tmp/{uuid} via virtio-fs
qemu-system-x86_64 \
  -M microvm,x-option-roms=off \
  -m "${MEMORY_MB}M" \
  -smp 1 \
  -kernel /var/lib/ghostexec/vmlinux \
  -initrd /var/lib/ghostexec/initrd.img \
  -append "root=/dev/vda rw console=ttyS0" \
  -chardev socket,id=char0,path=/tmp/${UUID}/virtiofs.sock \
  -device vhost-user-fs-pci,queue-size=1024,chardev=char0,tag=jobfs \
  -netdev user,id=net0 \
  -device virtio-net-device,netdev=net0 \
  -nographic -daemonize \
  -pidfile /tmp/${UUID}/vm.pid
```

#### systemd Transient (service isolation)

```bash
systemd-run \
  --transient \
  --unit="ghostexec-${UUID}" \
  --property="RuntimeDirectory=ghostexec/${UUID}" \
  --property="MemoryMax=${MEMORY_MB}M" \
  --property="CPUQuota=100%" \
  --property="ExecStopPost=/usr/lib/ghostexec/cleanup.sh ${UUID}" \
  --property="RemoveIPC=yes" \
  /usr/lib/ghostexec/worker "${UUID}"
```

---

### 4. Working Memory (`/tmp`)

Every job gets a private UUID workspace on a `tmpfs` mount. The filesystem IS the message bus.

```
/tmp/{uuid}/
├── state/
│   ├── QUEUED          ← O_EXCL lock, created by daemon
│   ├── RUNNING         ← renamed from QUEUED when job starts
│   ├── DONE            ← renamed from RUNNING on completion (CAS)
│   └── PERSISTED       ← (DB sink only) O_EXCL marker, gates self-destruct
├── input               ← payload, moved in via rename() (write-once for job lifetime)
├── input.staging       ← (Transport Layer, transient) pre-rename landing spot
├── classified/
│   └── fork_exec       ← symlink → ../input (zero-copy routing)
├── output.json         ← result, written atomically by worker
├── output.claimed      ← (DB sink only, transient) output.json after claim-rename
├── log                 ← worker stdout/stderr
├── vm.pid              ← (KVM path only) QEMU PID
└── virtiofsd.pid        ← (KVM path only) virtiofsd server PID, for cleanup
```

**Key operations:**

| Operation | Syscall | Why |
|-----------|---------|-----|
| Create workspace | `mkdir()` + `mount("tmpfs")` | Private ephemeral storage, kernel-managed |
| Stage payload | `rename()` | Zero-copy, atomic, POSIX-guaranteed |
| Route to executor | `symlink()` | Points without copying |
| State transition | `O_EXCL open` + `rename()` | Compare-and-swap semantics |
| Destroy workspace | `umount()` + `rm -rf` | Kernel reclaims memory instantly |

#### Memory Safety Boundary

"Destroyed" and "unrecoverable" are not automatically the same claim, and the difference matters the moment any job handles genuinely sensitive data (credentials, keys, the untrusted-payload case from the [fork_exec hardening](#3-spawn-engine)). `umount()` on a `tmpfs` reclaims RAM in one atomic kernel step regardless of file count or open fds — that part is guaranteed, already fixed to run before `rm -rf` (see [Self-Destruct Chain](#7-self-destruct-chain)). What it does **not** guarantee is that the data never existed outside RAM: under memory pressure, the kernel can swap `tmpfs` pages to disk like any other anonymous memory, and destroying the mount does not touch that swap copy. A workspace can be fully "destroyed" by every check this document defines and still have a forensically recoverable copy sitting in swap.

This is a **mount-time property, set once, not a runtime process that has to watch anything happen** — the fix belongs at workspace creation, not in a supervisor loop:

```bash
# Linux 6.3+: tmpfs can be told never to swap its pages, directly at
# mount time. This is the simplest fix and needs nothing watching it
# afterward — the kernel enforces it structurally for the mount's life.
mount -t tmpfs -o size=256m,mode=0700,uid=$(id -u ghostexec),noswap \
      "ghostexec-${UUID}" "/tmp/${UUID}"
```

```c
// Pre-6.3 kernels (no noswap flag): the WORKER calls mlock() on its
// own address space right after mapping the workspace, before reading
// any sensitive payload into it. This is per-process, at-allocation
// time — the same "one atomic call establishes the guarantee, nothing
// needs to keep checking it" discipline as rename()-as-CAS elsewhere
// in this document, just applied to memory pages instead of filenames.
#include <sys/mman.h>
mlockall(MCL_CURRENT | MCL_FUTURE); // pages stay resident, never swapped
madvise(workspace_addr, workspace_len, MADV_DONTDUMP); // excluded from core dumps too
```

**Why this doesn't need a daemon:** swap exclusion is enforced by the kernel from the moment the mount (or `mlock()` call) succeeds — there's no window where it's true "most of the time" that needs monitoring, the way a leaked `virtiofsd` process needs a PID file and an explicit kill. Reaching for a supervisor here would add exactly the kind of unnecessary abstraction this document's philosophy argues against: the OS already provides the guarantee for free at the primitive level, so use the primitive.

**Where a daemon-adjacent role does earn its place:** not *making* swap exclusion happen, but *auditing that `create_workspace()` applied it correctly, every time* — a policy check, not a mechanism. See [Master Daemon in Rust](#master-daemon-in-rust) below for why that specific role, alongside the rest of the daemon's spawn/track/cgroup responsibilities, is a good fit for a memory-safe implementation language rather than hand-written C.

---

### 5. Transport Layer

The transport layer moves job payloads between nodes. The routing decision:

```
target == localhost  →  CP  (atomic rename on same fs)
target in LAN       →  SCP (key-based, compressed)
target is remote    →  SSH (ControlMaster multiplexed)
```

#### SSH Config Bypass

`~/.ssh/config` (or `/etc/ssh/ssh_config`) eliminates per-connection auth overhead:

```sshconfig
Host ghostexec-*
    ControlMaster     auto
    ControlPath       /run/ghostexec/ssh-%r@%h:%p
    ControlPersist    300
    BatchMode         yes
    IdentityFile      /etc/ghostexec/id_ed25519
    StrictHostKeyChecking accept-new
    Compression       yes

# Multi-hop example
Host worker-remote-*
    ProxyJump         bastion.internal
    User              ghostexec
```

#### Transport Commands

The local case was already race-free: write to a `.tmp` path, then `rename()` into the final name — the workspace never has a partially-written `input` visible to a worker that starts reading early. The SCP and SSH cases as originally written did **not** have this property: both wrote directly to the final `/tmp/${UUID}/input` path on the remote end, so a worker on that remote node polling for `input` to appear could open it while the transfer was still in flight — mid-copy, not-yet-flushed, or (worse) truncated by a dropped connection — and read a corrupt or partial payload. This is the same zero-copy-staging discipline as the local `rename()` case, applied across the network: **the destination filename only ever appears once the bytes behind it are complete**, everywhere.

```bash
# Local (same host, same filesystem)
cp --no-preserve=all "${PAYLOAD}" "/tmp/${UUID}/input.tmp"
mv "/tmp/${UUID}/input.tmp" "/tmp/${UUID}/input"

# LAN (SCP with BatchMode) — write to input.staging on the remote,
# THEN a separate remote rename() into the final name. scp itself has
# no atomic-rename mode; without the trailing ssh + mv, a worker on
# ghostexec-worker-01 racing the transfer can open a half-written file.
scp -B -C -i /etc/ghostexec/id_ed25519 \
    "${PAYLOAD}" "ghostexec-worker-01:/tmp/${UUID}/input.staging" && \
ssh -F /etc/ghostexec/ssh_config -o BatchMode=yes ghostexec-worker-01 \
    "mv -T '/tmp/${UUID}/input.staging' '/tmp/${UUID}/input'"

# Remote (SSH through ControlMaster) — same staging discipline: the
# remote shell writes to .staging via the redirected cat, and only
# renames into place after the whole stream (and its exit status) is
# confirmed. `&&` is load-bearing: a partial stream must never rename.
ssh -o BatchMode=yes ghostexec-worker-remote-01 \
    "cat > '/tmp/${UUID}/input.staging' && mv -T '/tmp/${UUID}/input.staging' '/tmp/${UUID}/input'" \
    < "${PAYLOAD}"
```

**Why `rename()` on the remote side, not just a completed transfer, is the actual guarantee:** a transfer that finishes without error still isn't atomic from the *reader's* point of view unless the visible filename change is a single syscall. `scp`/`cat >` write bytes incrementally to whatever path they were given — the OS makes no promise that a concurrent `open()`+`read()` on that same path sees either "nothing" or "everything." `rename()` on the same filesystem is POSIX-guaranteed atomic: any reader either sees the old state (no `input` yet) or the new one (fully-written `input`), never an in-between. This is the identical CAS-on-filesystem idea already used for `state/QUEUED → RUNNING → DONE` ([Dynamo Atomic](#dynamo-atomic)), just applied to payload arrival instead of state transition — one mechanism, two use sites.

---

### 6. Execution Layer

The worker binary is cross-platform. It reads from the workspace, executes job logic, and writes results atomically.

```bash
# Worker reads input
INPUT=$(cat "/tmp/${UUID}/input")

# Execute job logic
RESULT=$(run_job "${INPUT}" 2>"/tmp/${UUID}/log")
EXIT_CODE=$?

# Write output atomically
cat > "/tmp/${UUID}/output.tmp" <<EOF
{
  "status": "$([ $EXIT_CODE -eq 0 ] && echo ok || echo error)",
  "exit_code": ${EXIT_CODE},
  "duration_ms": ${DURATION},
  "output": $(echo "${RESULT}" | jq -Rs .)
}
EOF
mv "/tmp/${UUID}/output.tmp" "/tmp/${UUID}/output.json"

# Signal completion via Dynamo CAS
mv "/tmp/${UUID}/state/RUNNING" "/tmp/${UUID}/state/DONE"
```

**Platform registration:**

| OS | Mechanism | Registration command |
|----|-----------|----------------------|
| Linux | `systemd` `Type=notify` unit | `systemctl enable ghostexec-worker` |
| macOS | `launchd` plist | `launchctl load ~/Library/LaunchAgents/ghostexec.plist` |
| Windows | Win32 Service | `sc create GhostExecWorker binPath= "C:\ghostexec\worker.exe"` |

---

### DB Sink Layer

`output.json` ([Working Memory](#4-working-memory--tmp)) is the canonical result — a schema-agnostic data frame, not a SQL row or a Mongo document by construction. Whether a given deployment writes it to Postgres, SQLite, or a document store is a decision made entirely at the sink, never upstream: nothing before this layer knows or cares which database exists, if any. This keeps the property the whole document has been building toward — every layer up to this one is a pure OS primitive with zero cloud/DB dependency — while still making the result durable and queryable for whoever needs it downstream.

**The race this introduces is new and specific: `output.json` has exactly one writer (the worker, once) and, without care, two independent, un-synchronized *deleters* of the same file** — the self-destruct chain (unconditional, fires on success/failure/timeout alike) and, implicitly, whoever is supposed to persist the result before it's gone. If the DB writer is slower than cleanup, or cleanup fires first because a job times out and `ExecStopPost=` runs immediately, the result is lost with no record it ever existed — silently, since cleanup treats a missing file as a non-error (`|| true` throughout). This is the same class of bug as the `virtiofsd` leak from earlier in this document: an implicit ordering assumption between two independent actors, uncaught until load reveals it.

**Fix: the DB sink claims `output.json` with the same `rename()`-as-CAS pattern already used for state transitions — claiming is what makes "has this been persisted yet" a single atomic fact instead of a race between two readers.** Self-destruct's own unlink of `output.json` (implicit in `rm -rf`) is gated on the claim already having happened:

```bash
# db_sink.sh — invoked from the SAME state-transition CAS that marks
# a job DONE, so persistence and self-destruct eligibility are ordered
# by the same mechanism rather than two mechanisms racing each other.

sink_and_claim() {
    local UUID="$1"
    local WORKSPACE="/tmp/${UUID}"

    # Claim output.json via rename() — this IS the CAS. Only one process
    # can win this rename (POSIX guarantees rename() is atomic and the
    # source either exists-and-moves or doesn't; two concurrent renames
    # of the same source can't both succeed). The loser's rename fails
    # with ENOENT and it simply does nothing further — there is no
    # "second copy" to reconcile, because there is only ever one target.
    if ! mv -T "${WORKSPACE}/output.json" "${WORKSPACE}/output.claimed" 2>/dev/null; then
        return 1   # someone else already claimed it, or it never existed
                    # (e.g. the job crashed before writing output.json —
                    # that is a legitimate outcome, not a race)
    fi

    # From here, ONLY this process holds output.claimed — safe to read
    # without any lock, since nothing else can also have won the rename.
    local FRAME
    FRAME=$(cat "${WORKSPACE}/output.claimed")

    # Write to the DB sink of this deployment's choice. The frame is
    # already structured JSON (status/exit_code/duration_ms/output from
    # the Execution Layer) — a SQL sink maps fields to columns, a NoSQL
    # sink stores the frame close to as-is. Either way this call is
    # idempotent-safe to retry on failure: output.claimed still exists
    # until this function's caller explicitly removes it below, so a
    # crash between claim and write can be retried without re-claiming.
    db_write "${UUID}" "${FRAME}" || return 1

    # Only after a CONFIRMED write does the claimed file get removed —
    # this is the signal to self-destruct that persistence is done.
    rm -f "${WORKSPACE}/output.claimed"

    # Mark the workspace persisted (atomic touch via O_EXCL, same CAS
    # discipline as state/QUEUED): self-destruct checks for this marker
    # and, if a DB sink is configured for this deployment, will not
    # unlink the workspace until it exists.
    ( set -o noclobber; > "${WORKSPACE}/state/PERSISTED" ) 2>/dev/null || true
}
```

**Ordering with self-destruct — the actual race-freedom guarantee:** the [Self-Destruct Chain](#7-self-destruct-chain)'s `ExecStopPost=` hook fires unconditionally and immediately when the worker exits, which is *before* an async DB sink (running as a separate consumer of the `DONE` state transition) can be guaranteed to have run. Rather than adding a sleep or a fixed delay — which is never actually race-free, only race-*unlikely* — cleanup.sh gates its destructive step on the same `PERSISTED` marker `sink_and_claim()` writes:

```bash
# cleanup.sh — added check, inserted before step 3 (unmount + rm -rf)
# in the existing Self-Destruct Chain. If no DB sink is configured for
# this deployment, GHOSTEXEC_DB_SINK_ENABLED is unset and this is a
# no-op — self-destruct behaves exactly as before for pure-OS-primitive
# deployments that never wanted a DB in the first place.
if [ -n "${GHOSTEXEC_DB_SINK_ENABLED:-}" ]; then
    WAITED=0
    while [ ! -f "${WORKSPACE}/state/PERSISTED" ] && [ "${WAITED}" -lt "${GHOSTEXEC_SINK_TIMEOUT_S:-10}" ]; do
        sleep 0.2
        WAITED=$((WAITED + 1))
    done
    if [ ! -f "${WORKSPACE}/state/PERSISTED" ]; then
        log "WARNING: destroying workspace before DB sink confirmed persistence (timeout ${GHOSTEXEC_SINK_TIMEOUT_S:-10}s) — result for ${UUID} may be lost"
    fi
fi
```

This bounded wait is not itself the race-freedom mechanism — the `rename()` claim is. The wait only decides *how long self-destruct is willing to delay* for a sink that's running slow; the claim is what guarantees that if the sink and self-destruct ever do run concurrently, exactly one of "sink persisted the frame" or "self-destruct logged a loss warning" happens, never a silent third outcome where the file vanishes mid-read. A deployment that cannot tolerate even a bounded wait can instead have cleanup's step 3 check for `output.claimed` (sink in progress) or `output.json` (sink hasn't started) and skip the `rm -rf` for those two files specifically, deferring only their removal to the sink's own `rm -f` — the rest of the workspace still destructs on schedule.

After job completion, the process removes every trace of itself. The chain is unconditional — it fires on success, failure, and timeout alike.

```bash
#!/usr/bin/env bash
# /usr/lib/ghostexec/cleanup.sh
# Called by ExecStopPost= — runs as the unit's user after the main process exits
set -euo pipefail
UUID="${1}"
WORKSPACE="/tmp/${UUID}"

# 1. Remove all routing symlinks
find "${WORKSPACE}/classified/" -type l -exec unlink {} \;

# 2. Kill virtiofsd and destroy the KVM VM — BEFORE touching the
# workspace, since virtiofsd holds an open fd on files inside it and
# the VM may still be reading/writing through the virtiofs mount.
# Killing the fs server after unmounting/removing its backing dir would
# leave it holding a dangling fd — a leaked process with nothing left
# to serve, invisible to `ps` review until someone notices memory drift.
if [ -f "${WORKSPACE}/vm.pid" ]; then
    if [ -f "${WORKSPACE}/virtiofsd.pid" ]; then
        kill "$(cat "${WORKSPACE}/virtiofsd.pid")" 2>/dev/null || true
    fi
    virsh destroy  "ghostexec-${UUID}" 2>/dev/null || true
    virsh undefine "ghostexec-${UUID}" 2>/dev/null || true
fi

# 3. Unmount BEFORE rm -rf: this is the reverse of the original
# ordering. Unmounting first detaches the tmpfs from the path in one
# atomic kernel operation and the kernel reclaims all backing RAM
# immediately, regardless of how many files or open fds were inside —
# rm -rf becomes unnecessary for the tmpfs case and is kept only as a
# fallback for non-tmpfs workspace paths (e.g. a misconfigured install).
umount "${WORKSPACE}" 2>/dev/null || umount -l "${WORKSPACE}" 2>/dev/null || true
rm -rf "${WORKSPACE}" 2>/dev/null || true

# 4. Remove the systemd transient unit record
systemctl reset-failed "ghostexec-${UUID}.service" 2>/dev/null || true

echo "ghostexec: ${UUID} destroyed"
```

The `ExecStopPost=` hook in the transient unit ensures this runs even if the worker crashes:

```ini
[Service]
ExecStart=/usr/lib/ghostexec/worker %i
ExecStopPost=/usr/lib/ghostexec/cleanup.sh %i
RuntimeDirectory=ghostexec/%i
RemoveIPC=yes
MemoryMax=256M
```

---

## Race-Freedom: Transfer + DB Write

A consolidated view of every place two independent actors can touch the same data concurrently, and what makes each one safe. All four follow the same underlying discipline — **the visible name of a piece of data only changes via a single atomic syscall, and readers only ever see either the fully-old or fully-new state, never a partial one** — applied at a different point in the pipeline each time:

| Site | Race without the fix | Mechanism | Where |
|---|---|---|---|
| **Transport (SCP/SSH)** | Remote worker opens `input` mid-transfer, reads a truncated/corrupt payload | Write to `input.staging` on the remote, `rename()` into `input` only after the full stream is confirmed | [Transport Layer](#5-transport-layer) |
| **Self-spawn UUID generation** | Two concurrent workers' `gen_uuid()` collide; both jobs corrupt the same workspace | CSPRNG (`getrandom()`, never time-seeded) + `mkdir()`-as-CAS with EEXIST-triggered retry | [Self-Spawn Mechanism](#self-spawn-mechanism) |
| **Symlink router swap** | Worker resolves the classify symlink, link is swapped mid-job (retry/reclassify path), worker's second read sees a different target | Resolve once at worker start, open the resolved path directly, never re-resolve; `input` itself is write-once for the job's life | [Symlink Router](#symlink-router) |
| **DB sink vs. self-destruct** | `cleanup.sh` unlinks `output.json` before an async DB writer reads it — result silently lost | `rename()` `output.json → output.claimed` as a one-winner claim; self-destruct gates its destructive step on a `PERSISTED` marker (bounded wait, not a race) | [DB Sink Layer](#db-sink-layer) |

**Why this generalizes rather than being four unrelated patches:** every race above has the same shape — one actor publishes data by giving it a name (a file appearing, a symlink pointing somewhere, a UUID being claimed), and a second, independent actor consumes or destroys based on that name. The fix is never a lock, a sleep, or a retry-until-it-works loop; it is always **making the publish step a single atomic rename/mkdir/getrandom call**, so there is no window in which the name exists but the data behind it is incomplete, and no window in which two actors can both believe they are first. This is the same idea the document already used for `state/QUEUED → RUNNING → DONE` ([Dynamo Atomic](#dynamo-atomic)) — race-freedom here isn't a new mechanism bolted on, it's the existing CAS-via-`rename()` discipline applied to every other place data changes hands.

---

## Job Lifecycle Workflow

```
① SIGNAL
   External trigger fires
         │
         ▼ (job descriptor → named pipe)
   Master daemon receives
         │
         ▼

② PREPARE
   Daemon assigns UUID
   Daemon writes QUEUED lock → /tmp/{uuid}/state/QUEUED
         │
         ▼ (descriptor passed to Prolog)
   Prolog classifies job → unifies executor type
         │
         ▼
   Data store stages payload:
     mkdir /tmp/{uuid}/  → mount tmpfs
     rename() payload   → /tmp/{uuid}/input
     symlink()          → /tmp/{uuid}/classified/{type}
         │
         ▼

③ EXECUTE
   Spawn engine creates process (fork/KVM/systemd-run)
         │
         ▼
   Transport moves payload (SSH/SCP/CP per target)
         │
         ▼
   Worker process runs:
     read  /tmp/{uuid}/input
     exec  job logic
     write /tmp/{uuid}/output.json
         │
         ▼

④ FINALIZE
   Dynamo atomic CAS:
     rename() RUNNING → DONE
         │
         ▼ (async trigger — DB sink, if configured, races self-destruct here)
   DB sink (if configured):
     rename() output.json → output.claimed   (one-winner claim)
     db_write(frame)
     touch state/PERSISTED                    (unblocks self-destruct's wait)
         │
         ▼
   Self-destruct chain fires:
     0. (if DB sink enabled) wait for state/PERSISTED, bounded timeout
     1. unlink all symlinks
     2. kill virtiofsd, THEN virsh destroy + undefine (KVM path)
     3. umount tmpfs FIRST, then rm -rf /tmp/{uuid}
     4. systemctl reset-failed (ExecStopPost=)

   Process gone — zero trace remains (result already durable in DB, if configured)
```

---

## Swimming Lane — Actor View

```
Actor          │ ① Signal    │ ② Prepare      │ ③ Execute        │ ④ Finalize
───────────────┼─────────────┼────────────────┼──────────────────┼──────────────
External       │ Job trigger │                │                  │
               │ cron·ssh·api│                │                  │
               │      │      │                │                  │
───────────────┼──────┼──────┼────────────────┼──────────────────┼──────────────
Orchestrate    │ Receives ──►│ UUID + route   │                  │
(daemon+prolog)│ dequeues    │ prolog classif.│                  │
               │             │      │         │                  │
───────────────┼─────────────┼──────┼─────────┼──────────────────┼──────────────
Data store     │             │ Stage payload  │                  │ CAS complete
(/tmp+dynamo)  │             │ rename+symlink │                  │ state → DONE
               │             │      └─────────┼──►(diagonal)    │     │
───────────────┼─────────────┼────────────────┼──────────────────┼─────┼────────
Compute        │             │                │ Spawn + run      │     │
(fork·kvm·svc) │             │                │ fork·kvm·svc     │     │
               │             │                │      │           │     │
───────────────┼─────────────┼────────────────┼──────┼───────────┼─────┼────────
Transport      │             │                │ Transport    ───►│ Self-destruct
(scp·cleanup)  │             │                │ SSH·SCP·CP       │ rm·stop·virsh
               │             │                │                  │ ▲
               │             │                │                  │ └── dashed CAS
```

**Reading the lane:**
- **Horizontal axis** = time (left to right across 4 phases)
- **Vertical axis** = which actor owns the work
- **Solid arrows** = direct sequential handoff
- **Dashed arrows** = async trigger (CAS completion → self-destruct)
- **L-shaped arrows** = phase boundary crossing (diagonal handoff)

---

## Core Primitives Deep Dive

### Temp Process

A **temp process** is a short-lived child created via `fork()` + `exec()` (or `clone()` with namespace flags). It exists only for the duration of the job and is cleaned up on `waitpid()`.

```c
// Create a temp process with full namespace isolation
int flags = CLONE_NEWPID   // new PID namespace
          | CLONE_NEWNS    // new mount namespace
          | CLONE_NEWNET   // new network namespace
          | CLONE_NEWUTS   // new hostname namespace
          | SIGCHLD;       // signal parent on exit

pid_t pid = clone(worker_main, stack_top, flags, &job_args);

// Parent registers the PID and returns to the daemon loop
register_child(pid, job->uuid);

// worker_main() (child):
// 1. Mount the job tmpfs
// 2. Drop privileges (setuid/setgid)
// 3. execve() the job binary
```

---

### Dynamo Atomic

**Dynamo atomic** implements compare-and-swap (CAS) on the filesystem — the same guarantee DynamoDB's `ConditionExpression` provides, but using POSIX syscalls.

The invariant: only one writer can transition a state file, and the transition is atomic.

```c
// atomic_cas(path, expected_state, new_state)
// Returns 0 on success, -1 if state did not match (concurrent update)
int atomic_cas(const char *workspace, const char *from, const char *to) {
    char from_path[PATH_MAX], to_path[PATH_MAX], lock_path[PATH_MAX];
    snprintf(from_path, sizeof(from_path), "%s/state/%s", workspace, from);
    snprintf(to_path,   sizeof(to_path),   "%s/state/%s", workspace, to);
    snprintf(lock_path, sizeof(lock_path), "%s/state/.lock", workspace);

    // Exclusive lock: fails if another writer holds it
    int lock_fd = open(lock_path, O_CREAT | O_EXCL | O_WRONLY, 0600);
    if (lock_fd < 0) return -1;  // concurrent CAS in progress

    // Verify expected state exists
    if (access(from_path, F_OK) != 0) {
        unlink(lock_path);
        return -1;  // state mismatch — expected state not present
    }

    // Atomic transition: rename is POSIX-guaranteed atomic on same filesystem
    int ret = rename(from_path, to_path);
    unlink(lock_path);
    return ret;
}

// Usage:
atomic_cas("/tmp/abc123", "QUEUED",  "RUNNING");  // job starts
atomic_cas("/tmp/abc123", "RUNNING", "DONE");     // job completes
```

---

### systemd + systemctl

`systemd` manages the lifecycle of GhostExec components. Two unit types are used:

**Persistent daemon unit** (`/etc/systemd/system/ghostexec.service`):

```ini
[Unit]
Description=GhostExec Master Daemon
After=network.target

[Service]
Type=notify
ExecStart=/usr/bin/ghostexec-daemon
Restart=always
RestartSec=1s
User=ghostexec
RuntimeDirectory=ghostexec
NotifyAccess=main

[Install]
WantedBy=multi-user.target
```

**Transient job unit** (generated at runtime by `systemd-run`):

```ini
# Auto-generated by systemd-run --transient
[Unit]
Description=GhostExec Job 7f3a9c21

[Service]
ExecStart=/usr/lib/ghostexec/worker 7f3a9c21
ExecStopPost=/usr/lib/ghostexec/cleanup.sh 7f3a9c21
RuntimeDirectory=ghostexec/7f3a9c21
MemoryMax=256M
CPUQuota=100%
RemoveIPC=yes
PrivateTmp=yes
```

**Control commands:**

```bash
# Start persistent daemon
systemctl start ghostexec

# Check transient job status
systemctl status ghostexec-7f3a9c21.service

# Job stops itself via ExecStopPost — but can be forced:
systemctl stop ghostexec-7f3a9c21.service

# Clear failed unit records
systemctl reset-failed 'ghostexec-*.service'
```

---

### sys_tmp

`sys_tmp` refers to the OS temporary filesystem layer: `/tmp` backed by `tmpfs` (RAM + swap), with `O_TMPFILE` for anonymous file creation.

```c
// Create an anonymous (unlinked) temp file — kernel manages it
int fd = open("/tmp", O_TMPFILE | O_RDWR, 0600);
// Write job payload to anonymous fd
write(fd, payload, payload_len);

// When ready to stage: link the anonymous file into the workspace
// This is the atomic "appear" operation — the file becomes visible at once
char proc_path[64], dest_path[256];
snprintf(proc_path, sizeof(proc_path), "/proc/self/fd/%d", fd);
snprintf(dest_path, sizeof(dest_path), "/tmp/%s/input", uuid);
linkat(AT_FDCWD, proc_path, AT_FDCWD, dest_path, AT_EMPTY_PATH);
close(fd);
```

**Mount a private tmpfs for each job workspace:**

```bash
UUID="7f3a9c21-..."
mkdir -p "/tmp/${UUID}"
mount -t tmpfs -o size=256m,mode=0700,uid=$(id -u ghostexec) \
      "ghostexec-${UUID}" "/tmp/${UUID}"
```

---

### Symlink Router

Symlinks act as **zero-copy data routing** within the workspace. The Prolog classifier writes a symlink that tells the executor where to find the input — no bytes are moved.

```bash
# After Prolog classification: executor_type = "kvm_executor"
mkdir -p "/tmp/${UUID}/classified"
ln -sf "../input" "/tmp/${UUID}/classified/kvm_executor"

# The KVM executor reads through the symlink:
# readlink /tmp/{uuid}/classified/kvm_executor → ../input
# Resolves to /tmp/{uuid}/input

# Atomic symlink replacement (swap routing without a gap):
ln -sf "../input" "/tmp/${UUID}/classified/.new_kvm_executor"
mv -Tf "/tmp/${UUID}/classified/.new_kvm_executor" \
       "/tmp/${UUID}/classified/kvm_executor"
# mv of a symlink is atomic on Linux (rename syscall)
```

**The swap itself is race-free — but "atomic swap" only means the link's *name* changes atomically, not that a worker mid-open through the old link is protected.** `classify/2` ([Prolog Classifier](#2-orchestration-layer)) is total and deterministic, so under the design as given a single UUID workspace is never classified twice with two different answers — the theoretical re-classify race the swap code defends against structurally cannot occur *if the classifier is only ever called once per job*. The real exposure is a **retry path**: if a caller re-runs classification after a transient failure (daemon restart mid-job, a future feature that reclassifies on timeout), a worker can be between `readlink()` and `open()` on the *old* target exactly when the swap lands. Symlink resolution itself is atomic per the kernel — no reader ever sees a half-written link — but the two-step "resolve, then open" sequence a worker performs is not, so the worker can resolve to `../input` under the old classification and then find a *different* file at that path if `input` itself was also replaced in the same window (not typical, since `input` is normally staged once and never rewritten — but worth stating as the actual invariant this design relies on, rather than leaving it implicit):

```bash
# Safe pattern for the worker side: resolve once, open the resolved
# TARGET path directly, and never re-resolve mid-job. This turns the
# TOCTOU window from "symlink could change between my two syscalls"
# into "symlink is read exactly once, at worker start, before the
# swap window matters at all."
TARGET=$(readlink -f "/tmp/${UUID}/classified/${EXECUTOR_TYPE}")
# From here on, the worker opens "${TARGET}" directly — never re-reads
# through classified/${EXECUTOR_TYPE} again for the life of this job.
INPUT=$(cat "${TARGET}")
```

The invariant this depends on: `input` is written exactly once via the [Working Memory](#4-working-memory--tmp) staging `rename()` and is never modified or replaced afterward for the life of a UUID — so once a worker has resolved the symlink, the target's *content* is stable even if the *symlink itself* is later swapped for an unrelated reason (e.g. a retry that re-runs `classify/2` and gets routed to a different executor type). If a future change ever needs `input` to be replaceable mid-job, that would reopen this TOCTOU window for real and would need the same `.new_` + atomic-rename treatment `input` itself, not just the symlink.

---

### KVM Executor

The KVM executor launches a QEMU microVM with `virtio-fs` to share the job workspace into the guest. The VM is fully isolated and destroys itself on exit.

```bash
# Full QEMU microVM launch
SOCKET="/tmp/${UUID}/virtiofs.sock"
VIRTIOFSD_PID_FILE="/tmp/${UUID}/virtiofsd.pid"

# 1. Start the virtiofsd server (shares /tmp/{uuid} into VM)
# Its PID is captured and persisted immediately — this file is the only
# record that the process exists, and cleanup.sh reads it to kill it.
# A backgrounded process with no recorded PID is unkillable by anything
# that didn't fork it directly, which is exactly the leak this avoids.
virtiofsd \
  --socket-path="${SOCKET}" \
  --shared-dir="/tmp/${UUID}" \
  --sandbox=none \
  --log-level=error \
  &
echo $! > "${VIRTIOFSD_PID_FILE}"

# 2. Launch the microVM
qemu-system-x86_64 \
  -M microvm,x-option-roms=off,pit=off,pic=off \
  -m "${MEMORY_MB}M" \
  -smp 1 \
  -kernel /var/lib/ghostexec/vmlinux \
  -initrd /var/lib/ghostexec/initrd.img \
  -append "root=/dev/vda rw console=ttyS0 panic=-1" \
  -chardev socket,id=vfs0,path="${SOCKET}" \
  -device vhost-user-fs-pci,queue-size=1024,chardev=vfs0,tag=jobfs \
  -serial stdio \
  -nographic \
  -no-reboot \
  -pidfile "/tmp/${UUID}/vm.pid"

# Guest startup mounts the shared filesystem:
# mount -t virtiofs jobfs /mnt/jobfs
# /mnt/jobfs/input → reads the job payload
# /mnt/jobfs/output.json → writes the result

# Cleanup — order matters: kill the fs server BEFORE destroying the VM,
# so the guest never observes a vanished virtiofs backend mid-syscall.
if [ -f "${VIRTIOFSD_PID_FILE}" ]; then
    kill "$(cat "${VIRTIOFSD_PID_FILE}")" 2>/dev/null || true
fi
virsh destroy  "ghostexec-${UUID}"
virsh undefine "ghostexec-${UUID}"
```

---

### Master Daemon

The master daemon is the only persistent process in GhostExec. It owns the lifecycle of all jobs.

```bash
# /etc/systemd/system/ghostexec.service
# Start:
systemctl start ghostexec

# The daemon:
# 1. Creates /run/ghostexec/jobs.fifo  (named pipe)
# 2. Binds /run/ghostexec/control.sock (control socket)
# 3. Enters poll() loop on the fifo fd
# 4. On each job: validate → UUID → classify (Prolog) → spawn → track

# PID file written to:
/run/ghostexec/daemon.pid

# Heartbeat (written every 30s via atomic_cas):
/run/ghostexec/heartbeat
```

---

### Master Daemon in Rust

Every other section of this document reaches for C when it needs a raw syscall — `clone()`, `mount()`, `rename()`, `getrandom()`, `epoll_wait()`. That stays true; Rust does not replace the *primitives*, it replaces the *hand-written glue between them* in the two places most exposed to the actual work this document has been hardening: **the master daemon core** (spawn/track/cgroup/epoll-based ingress) and the **security-sensitive path** (seccomp filter setup, capability dropping, swap-exclusion enforcement). The worker binary that runs arbitrary job logic stays whatever language a deployment already uses — that boundary is unaffected.

**Why these two places specifically, and not "rewrite everything in Rust":**

Nearly every race and leak fixed earlier in this document was a manual bookkeeping failure, not a logic error — `virtiofsd`'s PID never recorded, `rm -rf` running before `umount`, a `gen_uuid()` with no forced CSPRNG, a Prolog catch-all with no cut. These are exactly the class of bug Rust's ownership model catches at compile time rather than in production:

| Bug class already found in this document | What let it happen in C/bash | What Rust changes |
|---|---|---|
| `virtiofsd` leaked (PID never recorded/killed) | A `Popen`-style spawn with no compiler-enforced obligation to track or release the handle | A `Child` handle wrapped in a type whose `Drop` impl kills the process — forgetting to store the PID becomes a type the code won't compile without, not a runtime omission |
| `rm -rf` before `umount` (reversed safe order) | Ordering was a comment, not a constraint — nothing stopped a future edit from reordering the two lines back | An RAII guard around the mount whose `Drop` always unmounts before the guard's scope ends — the order is structural, not something a future contributor can silently invert |
| Two independent `cleanup.sh` copies drifting out of sync (caught earlier in this document as a duplication risk) | Bash has no module system enforcing "one definition, everywhere else calls it" | A single Rust function called from every call site; the compiler's dead-code/unused-import lints surface a stale duplicate immediately rather than months later |
| Buffer/size handling around `PIPE_BUF` framing | Manual `snprintf`/fixed-size buffers, one off-by-one from a memory-safety bug | `Vec<u8>`/slices with bounds checking built in — the length-prefixed framing logic can't read past what it was told to read |

None of this is about Rust being "safer" in the abstract — it's that the daemon's job is precisely bookkeeping (which PID belongs to which UUID, which cgroup a job is in, whether a mount was unmounted before removal, whether a seccomp filter was actually installed before `execve()`), and bookkeeping is what ownership/RAII/the borrow checker are built to make structurally hard to get wrong.

```rust
// Sketch: the daemon's core loop, epoll-driven ingress from Concurrent
// Ingress (../#concurrent-ingress--resource-isolation), each accepted
// job dispatched into a bounded worker pool — same shape as the C
// version, but PID tracking and cgroup membership are owned types
// instead of bare integers a caller could forget to clean up.

struct TrackedJob {
    uuid: Uuid,           // from getrandom() via the `rand` crate's OsRng —
                           // same CSPRNG requirement as gen_uuid() in C,
                           // enforced by the crate's own API surface
    child: std::process::Child,
    cgroup: CgroupHandle,  // Drop impl removes cgroup membership on exit
}

impl Drop for TrackedJob {
    fn drop(&mut self) {
        // Runs on every exit path — normal completion, error return,
        // early panic unwind — without a separate cleanup.sh call site
        // that could fall out of sync with this one, the way the two
        // near-duplicate cleanup.sh copies in this document did before
        // being reconciled.
        let _ = self.child.kill();
    }
}

fn admit_job(desc: &JobDescriptor, cgroups: &SourceCgroups) -> Result<(), RejectReason> {
    if desc.spawn_depth > MAX_SPAWN_DEPTH {
        return Err(RejectReason::DepthExceeded);
    }
    cgroups.take_token(&desc.source)?;   // fan-out rate limit, same
                                           // policy as the C admit_job()
    Ok(())
}
```

**What does not change:** the syscalls themselves, the filesystem layout, the atomic-rename-as-CAS discipline, the Prolog classifier, the wire format of a job descriptor. A Rust daemon calls the exact same `mount()`/`rename()`/`epoll_wait()`/`clone()` the C version does — via `libc` bindings or the `nix` crate, which are thin, zero-cost wrappers, not a new abstraction layer. This is still "the OS is the runtime"; only the language holding the bookkeeping together changed.

---

### Prolog Classifier

The classifier is a Prolog engine (SWI-Prolog recommended) invoked as a subprocess. The daemon calls it for each job and reads the unified executor type from stdout.

The subprocess call is deliberately a **pure I/O boundary**: stdin holds the one job descriptor, stdout holds the one executor name, nothing else is read or written, and the process exits. `classify_main/0` calls `classify/2` (defined once, in the [Prolog Classifier](#2-orchestration-layer) section above — this file does not redefine it) exactly once and takes its single, deterministic result. There is no `findall`, no retry-on-backtrack, so the cuts in `classify/2` are load-bearing here, not decorative: they're what guarantees this subprocess always prints exactly one line.

```bash
# Invocation from daemon
EXECUTOR=$(echo "${DESCRIPTOR_JSON}" | \
  swipl -q -t halt -g "consult('/etc/ghostexec/rules.pl'), \
    classify_main, halt" \
  2>/dev/null)

echo "Routing job ${UUID} to executor: ${EXECUTOR}"
```

```prolog
% /etc/ghostexec/rules.pl
% classify/2 is defined once — see the Prolog Classifier reference above.
% This file only adds the process entry point that wires stdin/stdout
% around that pure function; it introduces no additional routing logic,
% so there is exactly one place the routing decision can be made or changed.

:- use_module(library(http/json)).

% classify_main/0 — impure shell around the pure classify/2.
% Reads one JSON descriptor from stdin, prints one executor name to
% stdout. All I/O lives here; classify/2 itself touches neither stream.
classify_main :-
    read_term_from_atom_or_stream(current_input, Desc),
    classify(Desc, Executor),
    format("~w~n", [Executor]).
```

---

### Cron Timer

Cron triggers use `systemd .timer` units, which are more reliable and auditable than traditional `crond`.

```ini
# /etc/systemd/system/ghostexec-scheduled.timer
[Unit]
Description=GhostExec scheduled job trigger
After=ghostexec.service

[Timer]
OnCalendar=*-*-* *:00:00   # every hour
RandomizedDelaySec=30s
Persistent=true             # catch up if the system was down

[Install]
WantedBy=timers.target
```

```ini
# /etc/systemd/system/ghostexec-scheduled.service
[Unit]
Description=GhostExec trigger (one-shot)

[Service]
Type=oneshot
ExecStart=/usr/lib/ghostexec/trigger.sh --descriptor /etc/ghostexec/scheduled.json
```

```bash
# Enable and start the timer
systemctl enable --now ghostexec-scheduled.timer

# Check next trigger time
systemctl list-timers ghostexec-scheduled.timer
```

---

### SSH / SCP / CP Transport

#### SSH Config Bypass

The `~/.ssh/config` bypass eliminates interactive prompts and enables connection multiplexing:

```sshconfig
# /etc/ghostexec/ssh_config (passed via -F flag)

Host ghostexec-worker-*
    User                ghostexec
    IdentityFile        /etc/ghostexec/id_ed25519
    IdentitiesOnly      yes
    BatchMode           yes
    StrictHostKeyChecking accept-new
    ControlMaster       auto
    ControlPath         /run/ghostexec/ssh-%r@%h:%p.ctl
    ControlPersist      120
    Compression         yes
    ServerAliveInterval 10
    ServerAliveCountMax 3

Host ghostexec-worker-remote-*
    ProxyJump           bastion.corp.internal
    Port                2222
```

#### Transport Functions

```bash
# transport.sh — routing decision and execution

transport() {
    local UUID="$1" PAYLOAD="$2" TARGET="$3"

    if [ "${TARGET}" = "localhost" ]; then
        # Atomic local move (zero-copy)
        cp --no-preserve=all "${PAYLOAD}" "/tmp/${UUID}/input.staging"
        mv "/tmp/${UUID}/input.staging" "/tmp/${UUID}/input"

    elif is_lan_target "${TARGET}"; then
        # SCP with compression, key-based auth
        scp -F /etc/ghostexec/ssh_config \
            -B -C -q \
            "${PAYLOAD}" \
            "ghostexec-${TARGET}:/tmp/${UUID}/input"

    else
        # SSH with ControlMaster multiplexing (remote / cross-region)
        ssh -F /etc/ghostexec/ssh_config \
            -o BatchMode=yes \
            "ghostexec-${TARGET}" \
            "cat > /tmp/${UUID}/input" < "${PAYLOAD}"
    fi
}
```

---

## Self-Spawn Mechanism

GhostExec jobs can spawn sub-jobs by writing a new job descriptor to the daemon's named pipe. This is how recursive or fan-out workloads self-replicate — and it is also, unbounded, a fork bomb with extra steps: a worker that spawns 4 children where each spawns 4 more has no structural reason to ever stop. A self-spawning system that does not design the *stopping condition* has not designed self-spawning; it has designed half of it.

Two independent, composable limits close this, both derived purely from data already carried on the descriptor — no external counter service, no daemon-side global lock:

1. **Depth limit** — every descriptor carries `spawn_depth`, incremented by the parent (never by the child, and never mutated after creation — the field is set once at descriptor-construction time and is immutable for the life of that job). The daemon rejects any descriptor whose `spawn_depth` exceeds a configured ceiling, *before* assigning a UUID or touching the filesystem. This bounds how deep a recursive chain can go.
2. **Fan-out rate limit** — independent of depth, because a shallow-but-wide fan-out (one job spawning 10,000 children at depth 1) is just as dangerous as a deep chain. Each source's cgroup (see [Concurrent Ingress](#concurrent-ingress--resource-isolation)) already enforces a hard CPU/RAM ceiling; a token-bucket counter scoped to the same cgroup caps *spawn calls per second* from jobs running under it, so a fan-out storm hits a rate wall before it hits the memory wall.

```bash
# Inside a running worker: spawn a child job
# spawn_depth is read from THIS job's own descriptor (immutable, set
# once by whoever created this job) and incremented exactly once for
# the child — the increment is the only mutation in this function, and
# it produces a NEW value for the child rather than mutating the
# parent's own depth in place.
spawn_child() {
    local CHILD_PAYLOAD="$1"
    local PARENT_DEPTH="${GHOSTEXEC_SPAWN_DEPTH:-0}"
    local CHILD_DEPTH=$((PARENT_DEPTH + 1))

    local CHILD_DESCRIPTOR
    CHILD_DESCRIPTOR=$(jq -n \
        --arg payload "${CHILD_PAYLOAD}" \
        --argjson depth "${CHILD_DEPTH}" \
        '{ executor: "auto", isolation: "namespace", payload_path: $payload, target: "localhost", spawn_depth: $depth }')

    # Write to the daemon's named pipe — triggers a new job lifecycle.
    # The daemon enforces the depth ceiling and the cgroup's fan-out
    # rate limit on receipt, BEFORE any UUID or workspace is created —
    # rejection here costs nothing beyond parsing the descriptor.
    echo "${CHILD_DESCRIPTOR}" > /run/ghostexec/jobs.fifo

    # Fire-and-forget: the parent does not wait for the child.
}

# Example: fan-out into 4 parallel sub-jobs
for CHUNK in chunk1 chunk2 chunk3 chunk4; do
    spawn_child "/tmp/${UUID}/chunks/${CHUNK}"
done
```

```c
// Daemon-side rejection — runs before create_workspace()/stage_payload(),
// so a job over the limit never touches tmpfs or gets a UUID at all.
#define GHOSTEXEC_MAX_SPAWN_DEPTH 6

bool admit_job(const job_t *job) {
    if (job->spawn_depth > GHOSTEXEC_MAX_SPAWN_DEPTH) {
        log_reject(job, "spawn_depth exceeds ceiling");
        return false;
    }
    if (!cgroup_token_bucket_take(job->source_cgroup)) {
        log_reject(job, "fan-out rate limit exceeded for source");
        return false;
    }
    return true;
}
```

The master daemon handles re-entrance safely because each job gets an independent UUID workspace, the FIFO serialises concurrent writes, and — with the two limits above — every self-spawn chain is now provably finite: depth is bounded by `GHOSTEXEC_MAX_SPAWN_DEPTH` and width is bounded by the token bucket, so the total number of descendants any single root job can produce is bounded by their product, not unbounded.

**The remaining race under fan-out is UUID collision, not FIFO framing** — framing is already covered ([Master Daemon](#2-orchestration-layer)'s `PIPE_BUF`/`control.sock` split). With many workers self-spawning concurrently, each independently calls `gen_uuid()`; if two calls ever produce the same UUID, `create_workspace()` for the second collides with an in-flight job's live `/tmp/{uuid}/` directory — corrupting state for *both* jobs, since they'd share one `state/` and one `input`. This is a correctness property of the generator, not of the daemon's scheduling, so it has to be fixed at the source:

```c
// gen_uuid() MUST draw from a CSPRNG, not from time+PID or any other
// low-entropy seed. A time-seeded generator run by many concurrent
// workers spawned in the same tick (exactly the fan-out case in the
// example above — 4 children spawned in one for-loop, same wall-clock
// millisecond) has far less real entropy than its 128 bits suggest,
// and collision probability stops being "cryptographically negligible."
//
// getrandom() (Linux 3.17+, glibc 2.25+) reads straight from the
// kernel CSPRNG and cannot silently fall back to a weaker source the
// way reading /dev/urandom through a buffered stream sometimes can.
#include <sys/random.h>

void gen_uuid(char out[37]) {
    uint8_t b[16];
    if (getrandom(b, sizeof(b), 0) != sizeof(b)) {
        abort(); // fail loudly — a weak UUID here is a silent data
                 // corruption bug waiting for enough concurrent load
    }
    b[6] = (b[6] & 0x0F) | 0x40; // version 4
    b[8] = (b[8] & 0x3F) | 0x80; // variant 1 (RFC 4122)
    snprintf(out, 37,
        "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
        b[0],b[1],b[2],b[3], b[4],b[5], b[6],b[7], b[8],b[9],
        b[10],b[11],b[12],b[13],b[14],b[15]);
}
```

**Belt-and-suspenders at the collision point itself:** even with a correct CSPRNG, `create_workspace()` should treat "directory already exists" as a collision signal and retry with a fresh UUID rather than silently proceeding — `mkdir()` with no `O_EXCL`-equivalent check is exactly the kind of implicit "this can't happen" assumption that turns a one-in-2^122 event into a real incident the one time it does happen under enough concurrent fan-out:

```c
// create_workspace() — collision-safe by construction. mkdir() itself
// is atomic (EEXIST if the path exists), so this is a CAS on the
// directory namespace, same family as atomic_cas() on state files.
bool create_workspace(char uuid_out[37]) {
    for (int attempt = 0; attempt < 3; attempt++) {
        gen_uuid(uuid_out);
        char path[64];
        snprintf(path, sizeof(path), "/tmp/%s", uuid_out);
        if (mkdir(path, 0700) == 0) return true;   // success, no collision
        if (errno != EEXIST) return false;         // real error, not a collision
        // EEXIST: collision — retry with a new UUID rather than reusing
        // the colliding one, which would corrupt whichever job got there first
    }
    return false; // 3 collisions in a row is itself a signal something
                   // is wrong with the entropy source — treat as fatal,
                   // don't loop forever masking a broken gen_uuid()
}
```

---

## Data Manipulation Pattern

The core data pattern: **move to `/tmp`, classify, use, delete**. No data persists beyond the job boundary.

```
Stage 1: ARRIVE
  Payload lands in /var/spool/ghostexec/incoming/
  (written by trigger or remote SCP)

Stage 2: STAGE (atomic, zero-copy)
  rename() /var/spool/.../payload.tar.gz
         → /tmp/{uuid}/input
  (file appears atomically at destination — never partially visible)

Stage 3: CLASSIFY (no data movement)
  Prolog reads metadata fields from the job descriptor
  ln -sf ../input /tmp/{uuid}/classified/{executor_type}
  (symlink is the classification — no copy)

Stage 4: PROCESS (in-place)
  Worker reads /tmp/{uuid}/classified/{type} → resolves to ../input
  Job logic runs against /tmp/{uuid}/input
  Result written to /tmp/{uuid}/output.tmp
  rename() output.tmp → output.json (atomic publish)

Stage 5: COLLECT
  Caller reads /tmp/{uuid}/output.json before CAS fires

Stage 6: CAS + DESTROY
  rename() RUNNING → DONE   (atomic state transition)
  unlink all symlinks
  rm -rf /tmp/{uuid}
  umount /tmp/{uuid}        (kernel reclaims RAM instantly)
  /var/spool input already gone (was renamed in Stage 2)
  Nothing remains on disk
```

---

## Cross-Platform Support

The worker binary detects the OS at runtime and registers using the appropriate service mechanism:

```python
# ghostexec-register.py — run once at install time
import platform, subprocess, os, sys

OS = platform.system()

if OS == "Linux":
    unit = f"""
[Unit]
Description=GhostExec Worker
[Service]
Type=notify
ExecStart={sys.executable} -m ghostexec.worker
Restart=on-failure
[Install]
WantedBy=multi-user.target
"""
    with open("/etc/systemd/system/ghostexec-worker.service", "w") as f:
        f.write(unit)
    subprocess.run(["systemctl", "enable", "--now", "ghostexec-worker.service"])

elif OS == "Darwin":
    plist = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>        <string>com.ghostexec.worker</string>
  <key>ProgramArguments</key>
  <array><string>/usr/local/bin/ghostexec-worker</string></array>
  <key>RunAtLoad</key>    <true/>
  <key>KeepAlive</key>    <true/>
</dict>
</plist>"""
    plist_path = os.path.expanduser("~/Library/LaunchAgents/com.ghostexec.worker.plist")
    with open(plist_path, "w") as f:
        f.write(plist)
    subprocess.run(["launchctl", "load", plist_path])

elif OS == "Windows":
    subprocess.run([
        "sc", "create", "GhostExecWorker",
        "binPath=", r"C:\ghostexec\worker.exe",
        "start=", "auto",
        "DisplayName=", "GhostExec Worker"
    ])
    subprocess.run(["sc", "start", "GhostExecWorker"])
```

**Temporary directory mapping:**

| OS | `$TMPDIR` | tmpfs support |
|----|-----------|---------------|
| Linux | `/tmp` | `mount -t tmpfs` |
| macOS | `/var/folders/.../T/` | `hdiutil` RAM disk |
| Windows | `%TEMP%` | RAM disk via ImDisk / OSFMount |

---

## Self-Destruct Implementation

The complete cleanup sequence, guaranteed to run even if the worker crashes:

```bash
#!/usr/bin/env bash
# /usr/lib/ghostexec/cleanup.sh
# Invoked by systemd ExecStopPost= after worker exits (any exit code)
set -euo pipefail

UUID="${1:?UUID required}"
WORKSPACE="/tmp/${UUID}"

log() { logger -t "ghostexec-cleanup[${UUID}]" "$*"; }

log "Self-destruct initiated"

# 1. Unlink all routing symlinks (non-fatal if missing)
if [ -d "${WORKSPACE}/classified" ]; then
    find "${WORKSPACE}/classified" -maxdepth 1 -type l \
        -exec unlink {} \; 2>/dev/null || true
    log "Symlinks removed"
fi

# 2. Kill virtiofsd, THEN destroy the KVM VM (if this job used it).
# Order matters: virtiofsd is the process actually holding open fds on
# the workspace files; the VM is just its client over the socket. Kill
# the server first so nothing is still being served when the VM dies,
# then tear down the VM. Doing it in the other order — or forgetting
# virtiofsd entirely, as the earlier draft of this doc did — leaves an
# orphaned virtiofsd process per KVM job, quietly accumulating for the
# lifetime of the daemon since nothing else ever signals it to exit.
if [ -f "${WORKSPACE}/vm.pid" ]; then
    if [ -f "${WORKSPACE}/virtiofsd.pid" ]; then
        kill "$(cat "${WORKSPACE}/virtiofsd.pid")" 2>/dev/null \
            && log "virtiofsd killed"
    fi
    VM_NAME="ghostexec-${UUID}"
    virsh destroy  "${VM_NAME}" 2>/dev/null && log "VM destroyed"
    virsh undefine "${VM_NAME}" 2>/dev/null && log "VM undefined"
fi

# 3. Unmount BEFORE rm -rf (reverse of the original ordering).
# umount detaches the tmpfs atomically and the kernel reclaims all
# backing RAM in one step, independent of how many files or open fds
# existed under the mount. rm -rf first risks a partial removal (a
# busy fd, a lingering bind-mount from step 2) leaving pages the
# kernel won't reclaim until a later, possibly-forgotten umount runs.
# -l (lazy) is the fallback if something still references the mount.
umount "${WORKSPACE}" 2>/dev/null || \
    umount -l "${WORKSPACE}" 2>/dev/null || true
rm -rf "${WORKSPACE}" 2>/dev/null || true

log "Workspace destroyed"

# 4. Clear systemd transient unit record
systemctl reset-failed "ghostexec-${UUID}.service" 2>/dev/null || true

log "Self-destruct complete — ${UUID} is gone"
exit 0
```

**Trigger the self-destruct from the systemd unit:**

```ini
[Service]
ExecStart=/usr/lib/ghostexec/worker %i
ExecStopPost=/usr/lib/ghostexec/cleanup.sh %i
# ExecStopPost runs regardless of ExecStart exit code
```

---

## Quick Reference

| Primitive | Role | Key syscall / tool |
|-----------|------|-------------------|
| Temp process | Ephemeral worker | `clone()` with namespace flags |
| Dynamo atomic | State management | `O_EXCL open` + `rename()` |
| systemd | Lifecycle control | `systemd-run --transient` |
| systemctl | Service management | `ExecStopPost=` cleanup hook |
| sys_tmp | Working memory | `O_TMPFILE` + `tmpfs` mount |
| Symlink | Data routing | `symlink()` + `rename()` (atomic swap) |
| KVM executor | VM isolation | QEMU microVM + `virtiofsd` |
| Master daemon | Orchestrator | `poll()` on named pipe |
| Prolog | Job classifier | `swipl -q` subprocess |
| Cron | Scheduler | `systemd .timer` unit |
| SSH | Remote execution | ControlMaster + BatchMode |
| SCP | File transfer | `-B -C` key-based push |
| CP | Local staging | `--no-preserve` + `rename()` |

---

*GhostExec — spawn, execute, vanish.*
