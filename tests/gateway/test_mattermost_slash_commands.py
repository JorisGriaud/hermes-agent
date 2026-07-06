"""Tests for Mattermost native slash-command registration + execution.

Covers issue #6296 (register COMMAND_REGISTRY as native Mattermost slash
commands with autocomplete). The interactive approval-button flow (#27587)
is tested in ``test_mattermost_approval_buttons.py``.
"""
import json
import sys
import unittest.mock as _mock

import pytest
from unittest.mock import AsyncMock

from gateway.config import PlatformConfig
from gateway.platforms.base import MessageType


@pytest.fixture(autouse=True)
def _real_aiohttp():
    """Ensure the genuine aiohttp is importable for our HTTP handlers.

    A sibling suite (``test_slack``) does
    ``sys.modules.setdefault("aiohttp", MagicMock())`` at import time; if
    aiohttp had not been imported yet, that installs a mock globally and turns
    ``web.json_response`` into a MagicMock. Restore the real module so these
    tests are order-independent.
    """
    mod = sys.modules.get("aiohttp")
    if isinstance(mod, _mock.NonCallableMock):
        for name in [m for m in list(sys.modules) if m == "aiohttp" or m.startswith("aiohttp.")]:
            del sys.modules[name]
        import aiohttp  # noqa: F401
        import aiohttp.web  # noqa: F401
    yield


# ---------------------------------------------------------------------------
# Command-list helper (hermes_cli.commands.mattermost_slash_commands)
# ---------------------------------------------------------------------------

class TestMattermostCommandHelper:
    def test_sanitize_preserves_hyphens_and_lowercases(self):
        from hermes_cli.commands import _sanitize_mattermost_name
        assert _sanitize_mattermost_name("/Reload-MCP") == "reload-mcp"
        assert _sanitize_mattermost_name("set home") == "set-home"
        assert _sanitize_mattermost_name("weird!!name") == "weirdname"
        assert _sanitize_mattermost_name("__x__") == "x"

    def test_core_commands_present_and_reserved_excluded(self):
        from hermes_cli.commands import mattermost_slash_commands
        entries, hidden = mattermost_slash_commands()
        triggers = [t for t, _d, _h in entries]
        # Operational core commands must be registrable.
        for expected in ("approve", "deny", "new", "model", "stop"):
            assert expected in triggers, f"missing core trigger {expected!r}"
        # Triggers shadowed by Mattermost built-ins must never be offered:
        # /help is rejected at creation, /status is silently shadowed at runtime.
        assert "help" not in triggers
        assert "status" not in triggers
        assert isinstance(hidden, int)

    def test_menu_max_commands_reads_config_and_clamps(self, monkeypatch):
        import hermes_cli.config as cfg
        from hermes_cli.commands import mattermost_menu_max_commands, _DEFAULT_MATTERMOST_MAX_COMMANDS

        # No config → default.
        monkeypatch.setattr(cfg, "read_raw_config", lambda: {})
        assert mattermost_menu_max_commands() == _DEFAULT_MATTERMOST_MAX_COMMANDS

        # Configured value is honored.
        monkeypatch.setattr(cfg, "read_raw_config", lambda: {
            "platforms": {"mattermost": {"extra": {"command_menu": {"max_commands": 40}}}}
        })
        assert mattermost_menu_max_commands() == 40

        # Out-of-range values are clamped to [1, 200].
        monkeypatch.setattr(cfg, "read_raw_config", lambda: {
            "platforms": {"mattermost": {"extra": {"command_menu": {"max_commands": 9999}}}}
        })
        assert mattermost_menu_max_commands() == 200

    def test_triggers_are_valid_and_unique(self):
        import re
        from hermes_cli.commands import mattermost_slash_commands
        entries, _ = mattermost_slash_commands()
        triggers = [t for t, _d, _h in entries]
        assert len(triggers) == len(set(triggers)), "duplicate triggers"
        for t in triggers:
            assert re.fullmatch(r"[a-z0-9_-]{1,32}", t), f"invalid trigger {t!r}"

    def test_cap_trims_only_skills_and_reports_hidden(self):
        from hermes_cli.commands import mattermost_slash_commands
        capped, hidden = mattermost_slash_commands(max_commands=6)
        assert len(capped) == 6
        assert hidden >= 0
        # Core operational commands survive an aggressive cap.
        triggers = [t for t, _d, _h in capped]
        assert "new" in triggers


# ---------------------------------------------------------------------------
# Adapter construction with callback config
# ---------------------------------------------------------------------------

def _make_adapter(monkeypatch, *, public_url="https://hermes.example.com", extra=None):
    monkeypatch.setenv("MATTERMOST_TOKEN", "test-token")
    monkeypatch.setenv("MATTERMOST_URL", "https://mm.example.com")
    if public_url is None:
        monkeypatch.delenv("MATTERMOST_PUBLIC_URL", raising=False)
    else:
        monkeypatch.setenv("MATTERMOST_PUBLIC_URL", public_url)
    from plugins.platforms.mattermost.adapter import MattermostAdapter
    config = PlatformConfig(
        enabled=True,
        token="test-token",
        extra={"url": "https://mm.example.com", **(extra or {})},
    )
    return MattermostAdapter(config)


class TestCallbackBase:
    def test_prefers_public_url(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, public_url="https://hermes.example.com/")
        assert adapter._callback_base() == "https://hermes.example.com"
        assert adapter._callbacks_enabled is True
        assert adapter._command_path == "/mattermost/command"
        assert adapter._action_path == "/mattermost/action"

    def test_disabled_when_only_wildcard_host(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, public_url=None)
        # No public URL and default 0.0.0.0 bind → nothing to call back to.
        assert adapter._callback_base() == ""
        assert adapter._callbacks_enabled is False

    def test_routable_host_fallback(self, monkeypatch):
        adapter = _make_adapter(
            monkeypatch, public_url=None,
            extra={"webhook_host": "hermes.internal", "webhook_port": 9000},
        )
        assert adapter._callback_base() == "http://hermes.internal:9000"


# ---------------------------------------------------------------------------
# Slash-command registration (idempotent)
# ---------------------------------------------------------------------------

class TestSlashRegistration:
    @pytest.mark.asyncio
    async def test_creates_missing_commands_with_correct_payload(self, monkeypatch):
        adapter = _make_adapter(monkeypatch)

        async def fake_get(path):
            if path == "users/me/teams":
                return [{"id": "team1"}]
            if path.startswith("commands?team_id=team1"):
                return []  # nothing registered yet
            return {}

        created = []

        async def fake_post(path, payload, **kw):
            assert path == "commands"
            created.append(payload)
            return {"id": f"cmd-{payload['trigger']}", "token": f"tok-{payload['trigger']}"}

        adapter._api_get = AsyncMock(side_effect=fake_get)
        adapter._api_post = AsyncMock(side_effect=fake_post)

        await adapter._register_slash_commands()

        assert created, "no commands were created"
        sample = created[0]
        assert sample["method"] == "P"
        assert sample["url"] == "https://hermes.example.com/mattermost/command"
        assert sample["auto_complete"] is True
        assert sample["trigger"] == sample["display_name"].lstrip("/")
        # Every created command's token is captured for inbound verification.
        assert any(t.startswith("tok-") for t in adapter._command_tokens)
        assert len(adapter._command_tokens) == len(created)

    @pytest.mark.asyncio
    async def test_skips_existing_and_readopts_token(self, monkeypatch):
        adapter = _make_adapter(monkeypatch)
        action_url = "https://hermes.example.com/mattermost/command"

        async def fake_get(path):
            if path == "users/me/teams":
                return [{"id": "team1"}]
            if path.startswith("commands?team_id=team1"):
                return [
                    {"trigger": "new", "url": action_url, "token": "existing-tok", "id": "c-new"},
                ]
            return {}

        posted = []

        async def fake_post(path, payload, **kw):
            posted.append(payload["trigger"])
            return {"id": f"cmd-{payload['trigger']}", "token": f"tok-{payload['trigger']}"}

        adapter._api_get = AsyncMock(side_effect=fake_get)
        adapter._api_post = AsyncMock(side_effect=fake_post)

        await adapter._register_slash_commands()

        # "new" already exists → never recreated, but its token is re-adopted.
        assert "new" not in posted
        assert "existing-tok" in adapter._command_tokens
        assert "c-new" in adapter._created_command_ids

    @pytest.mark.asyncio
    async def test_no_teams_skips_gracefully(self, monkeypatch):
        adapter = _make_adapter(monkeypatch)
        adapter._api_get = AsyncMock(return_value=[])
        adapter._api_post = AsyncMock()
        await adapter._register_slash_commands()
        adapter._api_post.assert_not_called()

    @pytest.mark.asyncio
    async def test_stale_url_command_recreated(self, monkeypatch):
        """A command whose URL is stale (e.g. MATTERMOST_PUBLIC_URL changed) is
        deleted and recreated pointing at the current callback URL."""
        adapter = _make_adapter(monkeypatch)
        new_url = "https://hermes.example.com/mattermost/command"

        async def fake_get(path):
            if path == "users/me/teams":
                return [{"id": "team1"}]
            if path.startswith("commands?team_id=team1"):
                return [{
                    "trigger": "new",
                    "url": "http://127.0.0.1:8066/mattermost/command",  # stale
                    "token": "old", "id": "c-old",
                }]
            return {}

        deleted, posted = [], []

        async def fake_delete(path):
            deleted.append(path)
            return True

        async def fake_post(path, payload, **kw):
            posted.append(payload["trigger"])
            return {"id": f"c-{payload['trigger']}", "token": f"t-{payload['trigger']}"}

        adapter._api_get = AsyncMock(side_effect=fake_get)
        adapter._api_delete = AsyncMock(side_effect=fake_delete)
        adapter._api_post = AsyncMock(side_effect=fake_post)

        await adapter._register_slash_commands()

        assert "commands/c-old" in deleted     # stale command removed
        assert "new" in posted                  # recreated with the new URL
        assert "t-new" in adapter._command_tokens
        assert new_url  # sanity


class TestCreateCommandPayload:
    @pytest.mark.asyncio
    async def test_description_truncated_to_mattermost_limits(self, monkeypatch):
        adapter = _make_adapter(monkeypatch)
        captured = {}

        async def fake_post(path, payload, **kw):
            captured["payload"] = payload
            return {"id": "c1", "token": "t1"}

        adapter._api_post = AsyncMock(side_effect=fake_post)
        long_desc = (
            "Compress conversation context (add 'here [N]' to keep recent N "
            "turns; --preview shows what would happen) and more padding here"
        )
        ok = await adapter._create_slash_command(
            "team1", "compress", long_desc, "[here N]",
            "https://hermes.example.com/mattermost/command",
        )
        assert ok is True
        p = captured["payload"]
        # Mattermost rejects Description > 64; autocomplete desc tolerates more.
        assert len(p["description"]) <= 64
        assert len(p["auto_complete_desc"]) <= 128
        assert p["method"] == "P"
        assert p["auto_complete"] is True

    @pytest.mark.asyncio
    async def test_duplicate_trigger_is_benign(self, monkeypatch):
        adapter = _make_adapter(monkeypatch)

        async def fake_post(path, payload, **kw):
            # Simulate Mattermost's duplicate-trigger 400.
            adapter._last_post_status = 400
            adapter._last_post_error = (
                '{"id":"api.command.duplicate_trigger.app_error",'
                '"message":"This trigger word is already in use."}'
            )
            return {}

        adapter._api_post = AsyncMock(side_effect=fake_post)
        ok = await adapter._create_slash_command("team1", "new", "desc", "", "url")
        assert ok is False  # not created, but no exception raised


# ---------------------------------------------------------------------------
# Slash-command execution handler
# ---------------------------------------------------------------------------

class _FakeRequest:
    def __init__(self, form):
        self._form = form

    async def post(self):
        return self._form


def _body(resp):
    return json.loads(resp.body)


class TestSlashCommandHandler:
    @pytest.mark.asyncio
    async def test_rejects_invalid_token(self, monkeypatch):
        adapter = _make_adapter(monkeypatch)
        adapter._command_tokens = {"good-token"}
        req = _FakeRequest({"token": "bad", "user_id": "u1", "command": "/status"})
        resp = await adapter._handle_slash_command(req)
        assert resp.status == 401

    @pytest.mark.asyncio
    async def test_authorized_command_dispatches(self, monkeypatch):
        monkeypatch.setenv("MATTERMOST_ALLOWED_USERS", "u1")
        adapter = _make_adapter(monkeypatch)
        adapter._command_tokens = {"good"}
        adapter._api_get = AsyncMock(return_value={})  # get_chat_info → default
        adapter.handle_message = AsyncMock()

        req = _FakeRequest({
            "token": "good", "user_id": "u1", "user_name": "@alice",
            "channel_id": "chan1", "command": "/model", "text": "openai:gpt-4o",
        })
        resp = await adapter._handle_slash_command(req)
        assert resp.status == 200

        # Dispatch is fire-and-forget on the loop — let the task run.
        import asyncio
        await asyncio.sleep(0.05)
        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.await_args.args[0]
        assert event.message_type == MessageType.COMMAND
        assert event.text == "/model openai:gpt-4o"
        assert event.source.user_id == "u1"

    @pytest.mark.asyncio
    async def test_unauthorized_user_not_dispatched(self, monkeypatch):
        monkeypatch.setenv("MATTERMOST_ALLOWED_USERS", "someone-else")
        adapter = _make_adapter(monkeypatch)
        adapter._command_tokens = {"good"}
        adapter.handle_message = AsyncMock()

        req = _FakeRequest({
            "token": "good", "user_id": "intruder",
            "channel_id": "chan1", "command": "/model",
        })
        resp = await adapter._handle_slash_command(req)
        body = _body(resp)
        assert "not authorized" in body["text"].lower()
        adapter.handle_message.assert_not_called()
