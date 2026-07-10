# Updating Hermes while keeping the Mattermost slash-commands feature

This fork carries one local feature on top of upstream Hermes Agent:

> **Mattermost native slash commands + interactive approval buttons**
> (issues [#6296](https://github.com/NousResearch/hermes-agent/issues/6296)
> and [#27587](https://github.com/NousResearch/hermes-agent/issues/27587)).

The feature lives on the branch **`feature/mattermost-slash-commands`**. This
document is the exact procedure to pull upstream updates without losing it.

## TL;DR

```bash
git fetch upstream
git checkout main
git merge --ff-only upstream/main        # main stays a clean mirror of upstream
git push origin main

git checkout feature/mattermost-slash-commands
git rebase main                          # replay the feature on top of new upstream
# ...resolve conflicts if any (see below)...
git push --force-with-lease origin feature/mattermost-slash-commands
```

Then **revalidate** (see the last section).

## Remotes (one-time setup — already done)

```bash
git remote -v
# origin    git@github.com:JorisGriaud/hermes-agent.git   (your fork)
# upstream  https://github.com/NousResearch/hermes-agent.git
```

If `upstream` is missing:

```bash
git remote add upstream https://github.com/NousResearch/hermes-agent.git
```

## Why rebase (not merge) the feature branch

- **`main`**: keep it a *pristine mirror* of `upstream/main`. Never commit the
  feature here. Update it with a fast-forward only (`git merge --ff-only
  upstream/main`). If that fast-forward is ever refused, it means something
  landed on your `main` that isn't on upstream — investigate before forcing.
- **`feature/mattermost-slash-commands`**: **rebase** it onto the refreshed
  `main`. Rebasing keeps the three feature commits atomic and linear on top of
  whatever upstream ships, which is exactly what you want for repeatedly
  re-applying the feature and (optionally) proposing it upstream later. Merging
  upstream *into* the feature branch would instead accumulate merge commits and
  blur the atomic history.

## Step-by-step

```bash
# 1. Fetch upstream history
git fetch upstream

# 2. Fast-forward your main to upstream/main
git checkout main
git merge --ff-only upstream/main
git push origin main

# 3. Rebase the feature branch onto the new main
git checkout feature/mattermost-slash-commands
git rebase main
```

## Resolving conflicts

The feature touches these files. Conflicts, if any, will be here — the two
source files are the ones to watch, because upstream is most likely to edit
them:

| File | What the feature adds |
|------|-----------------------|
| `plugins/platforms/mattermost/adapter.py` | Callback aiohttp server, slash-command registration, `send_exec_approval`, `_handle_slash_command`, `_handle_interactive`, auth helper |
| `hermes_cli/commands.py` | `mattermost_slash_commands()` + helpers (mirrors the Telegram/Discord command builders) |
| `plugins/platforms/mattermost/plugin.yaml` | New `MATTERMOST_*` optional env vars |
| `website/docs/user-guide/messaging/mattermost.md` | "Slash commands & interactive approvals" section |
| `website/docs/user-guide/messaging/index.md` | Interactive-controls table |
| `website/docs/reference/environment-variables.md` | New `MATTERMOST_*` rows |
| `tests/gateway/test_mattermost_slash_commands.py` | New file — will not conflict |
| `tests/gateway/test_mattermost_approval_buttons.py` | New file — will not conflict |

> **Note:** the feature does **not** modify `gateway/run.py`. In this codebase
> `run.py` hosts no HTTP routes — each platform adapter runs its own aiohttp
> server (like the LINE / WhatsApp adapters). If a future upstream change moves
> HTTP routing into `run.py`, revisit that decision, but today there is nothing
> to conflict there.

When a conflict appears:

```bash
git status                     # see conflicted files
# edit each conflicted file, keep BOTH upstream's changes and the feature's
git add <file>
git rebase --continue
```

Guidance per file:

- **`plugins/platforms/mattermost/adapter.py`** — The feature's additions are
  self-contained blocks: the `__init__` callback-config block, the
  `_api_delete` helper, the callback-server / registration / approval methods
  (all under the "Interactive callbacks" and "Interactive approval buttons"
  comment banners), and the `connect()` / `disconnect()` hooks that start and
  stop the server. If upstream **refactored or renamed** `connect()`,
  `disconnect()`, `_api_post`, `_handle_ws_event`, or `build_source`, the
  re-insertion is not mechanical: **stop and re-read the new adapter** before
  resolving, so you re-wire the callback-server startup/teardown into the new
  shape rather than blindly keeping your old lines. Don't force a silent
  resolution here — a wrong merge can leave the server unstarted or the session
  closed before cleanup.
- **`hermes_cli/commands.py`** — The additions sit in their own "Mattermost
  native slash commands" section and reuse shared helpers
  (`_collect_gateway_skill_entries`, `_clamp_command_names`,
  `_telegram_effective_priority`). If upstream changed those helper signatures,
  update the Mattermost wrappers to match (the Telegram/Discord wrappers in the
  same file are the reference).
- **Docs / `plugin.yaml`** — Straightforward: keep both sides' rows/sections.

If a rebase conflict in `plugins/platforms/mattermost/adapter.py` is caused by
an upstream refactor and you're unsure how to re-wire it, **abort and ask for
help** rather than guessing:

```bash
git rebase --abort
```

## Revalidate after the rebase

1. **Run the feature's tests** (plus the existing Mattermost suite for
   regressions):

   ```bash
   pytest tests/gateway/test_mattermost_slash_commands.py \
          tests/gateway/test_mattermost_approval_buttons.py \
          tests/gateway/test_mattermost.py -q
   ```

   (Use the project's dev shell / venv — the tests need `pytest`,
   `pytest-asyncio`, and `aiohttp`.)

2. **Smoke-test the live callback server** end to end (no Mattermost server
   needed) with this throwaway script:

   ```python
   # /tmp/mm_smoke.py
   import asyncio, os
   os.environ.update(MATTERMOST_TOKEN="t", MATTERMOST_URL="https://mm.example.com",
                     MATTERMOST_PUBLIC_URL="http://127.0.0.1:8199",
                     MATTERMOST_WEBHOOK_HOST="127.0.0.1", MATTERMOST_WEBHOOK_PORT="8199",
                     MATTERMOST_ALLOWED_USERS="u1")
   from unittest.mock import AsyncMock, patch
   from gateway.config import PlatformConfig
   from plugins.platforms.mattermost.adapter import MattermostAdapter
   import aiohttp

   async def main():
       a = MattermostAdapter(PlatformConfig(enabled=True, token="t",
                                            extra={"url": "https://mm.example.com"}))
       a._session = AsyncMock(); a._session.closed = False
       await a._start_callback_server()
       a._exec_approval_state["aid"] = {"session_key": "s", "secret": "sec"}
       async with aiohttp.ClientSession() as cs, \
               patch("tools.approval.resolve_gateway_approval", return_value=1):
           r = await cs.post("http://127.0.0.1:8199/mattermost/action",
                             json={"user_id": "u1", "user_name": "@a",
                                   "context": {"approval_id": "aid", "choice": "once", "secret": "sec"}})
           print("action:", r.status, await r.json())
       await a._runner.cleanup()
   asyncio.run(main())
   ```

   ```bash
   PYTHONPATH=. python /tmp/mm_smoke.py
   # expect: action: 200 {'update': {'message': '✅ Approved once by @a', ...}}
   ```

3. **Live check against your Mattermost** (optional but recommended after a
   significant upstream bump): start `hermes gateway`, confirm the log line
   `Mattermost: callback server listening on ...`, type `/` in a channel to see
   the autocomplete menu, and trigger a dangerous command to confirm the
   approval buttons render and resolve. See
   `website/docs/user-guide/messaging/mattermost.md` →
   "Slash commands & interactive approvals".
