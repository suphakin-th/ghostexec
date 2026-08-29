# GhostExec

**Run a job. Get the result. Nothing is left behind — not even in swap.**

GhostExec is a serverless-style job runner that uses nothing but your operating system: no Docker, no cloud account, no framework. Every job gets a private, memory-backed workspace, runs, saves its result wherever you want (a database, a file, a message queue), and then the workspace is destroyed so completely that even a memory-pressure swap copy can't survive it.

---

## The everyday problem

Every team that runs short-lived jobs — a script, a webhook handler, a data transform, a scheduled cleanup task — ends up solving the same five problems over and over, usually badly:

1. **"Where does the job's temp data live, and did we actually clean it up?"** — `/tmp` fills up, nobody remembers which files belong to which run, and eventually a cron job or a human has to `rm -rf` things by hand and hope nothing important gets caught in the blast radius.
2. **"Two jobs touched the same file at the same time — now what?"** — a script reads a file while another script is still writing it, and you get a corrupted result that fails silently, days later, in production.
3. **"We wanted a lightweight sandbox but ended up needing a whole container platform."** — Docker/Kubernetes solve isolation, but they're a lot of moving parts (and a lot of attack surface) for a job that runs for two seconds.
4. **"A job crashed halfway — did its temp files get cleaned up, or are they still sitting there?"** — most cleanup logic only runs on the happy path; crashes and timeouts leak.
5. **"Is the result actually safe once the job is done, or could it still be recovered from disk?"** — RAM-based temp storage sounds safe, until you remember the OS can swap those pages to disk under memory pressure, and "I deleted the folder" doesn't erase that copy.

None of these are exotic problems. They're the boring, everyday failure modes of "just write a script that does a thing and cleans up after itself" — and they usually get discovered the hard way, under load, months after the code was written.

## The solution

GhostExec answers each of the five problems with one underlying idea: **use the mechanisms the operating system already gives you, correctly, instead of building new abstractions on top.**

| Everyday problem | What GhostExec does about it |
|---|---|
| Temp data pollution / unclear ownership | Every job gets its own private, memory-only workspace (`tmpfs`), named by a random ID, that exists only for that job |
| Two jobs racing on the same file | Every handoff — a file arriving, a job's state changing, a result being saved — happens as a single atomic filesystem operation, so there's never a half-written state for another process to trip over |
| Wanting isolation without a whole container platform | Three isolation levels to choose from — lightweight process isolation, a full virtual machine, or a managed background service — picked automatically based on what the job actually needs |
| Cleanup not running after a crash | Cleanup is wired to the process lifecycle itself, not to "the happy path" — it runs whether the job succeeds, fails, or times out |
| "Deleted" not meaning "unrecoverable" | Workspaces are explicitly told never to swap to disk, so destroying them is actually destroying them — not just removing the easy-to-find copy |

## What the package does, concretely

- **Runs your job in a disposable sandbox** that's created fresh and destroyed completely, every single time — no shared state between runs, no cleanup script to remember to write.
- **Picks the right level of isolation automatically** — a quick trusted script gets a cheap, fast sandbox; an untrusted or heavy job gets a full virtual machine — based on simple rules you can read and edit yourself, not a black box.
- **Moves data in and out safely**, whether the job is on the same machine or talking to another server, without ever exposing a half-transferred file to something that might read it too early.
- **Saves the result wherever you need it** — a SQL database, a NoSQL store, a message queue — through one plain, well-defined result format, with a guarantee that the result is safely saved *before* its workspace is destroyed.
- **Lets jobs spawn other jobs** (for batch/fan-out work) without that turning into a runaway flood — there's a built-in limit on how deep and how wide that can go.
- **Runs the same way on Linux, macOS, and Windows**, using each OS's native service manager, so "how do I deploy this" doesn't change depending on the platform.

## Who this is for

Anyone currently gluing together cron jobs, one-off scripts, or a lightweight task queue by hand, and wondering whether their cleanup logic actually works under real load — without wanting to take on a full container/orchestration platform just to get "run this safely and clean up after it" right.

## Status

This repository currently contains the architecture design and full technical specification — see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the complete low-level reference (syscalls, race-condition proofs, and the reasoning behind every design decision). Implementation is in progress.

## License

**All rights reserved. This is not open source — but you're welcome to read it.**

This repository is shared publicly for portfolio and evaluation purposes: anyone may read the architecture, documentation, and code to assess the work. Using, copying, modifying, deploying, or distributing any of it beyond that requires prior written authorization from the Owner, in a specific, deliberately strict form — see [`LICENSE`](LICENSE) for the full terms.

To request authorization for use beyond reading, contact: suphakin.th@gmail.com
