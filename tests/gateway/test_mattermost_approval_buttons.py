"""Tests for Mattermost interactive dangerous-command approval buttons.

Covers issue #27587 (interactive Allow Once / Allow Session / Always Allow /
Deny buttons with parity with Discord's ExecApprovalView).
"""
import json
import sys
import unittest.mock as _mock

import pytest
from unittest.mock import AsyncMock, patch

from gateway.config import PlatformConfig


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
    adapter = MattermostAdapter(config)
    # Pretend the HTTP client session is live.
    adapter._session = AsyncMock()
    adapter._session.closed = False
    return adapter


class _FakeRequest:
    def __init__(self, body):
        self._body = body

    async def json(self):
        return self._body


def _body(resp):
    return json.loads(resp.body)


# ---------------------------------------------------------------------------
# send_exec_approval
# ---------------------------------------------------------------------------

class TestSendExecApproval:
    @pytest.mark.asyncio
    async def test_posts_four_buttons_with_callback_urls(self, monkeypatch):
        adapter = _make_adapter(monkeypatch)
        posted = {}

        async def fake_post(chat_id, payload, metadata):
            posted["chat_id"] = chat_id
            posted["payload"] = payload
            return {"id": "post-1"}

        adapter._post_preserving_thread = AsyncMock(side_effect=fake_post)

        result = await adapter.send_exec_approval(
            chat_id="chan1",
            command="rm -rf /tmp/x",
            session_key="sess-123",
            description="file deletion",
        )
        assert result.success
        assert result.message_id == "post-1"

        attachments = posted["payload"]["props"]["attachments"]
        assert len(attachments) == 1
        actions = attachments[0]["actions"]
        names = [a["name"] for a in actions]
        assert names == ["Allow Once", "Allow Session", "Always Allow", "Deny"]

        choices = [a["integration"]["context"]["choice"] for a in actions]
        assert choices == ["once", "session", "always", "deny"]

        for a in actions:
            assert a["integration"]["url"] == "https://hermes.example.com/mattermost/action"
            assert a["integration"]["context"]["approval_id"]
            assert a["integration"]["context"]["secret"]

        # State is recorded so the callback can resolve it.
        assert len(adapter._exec_approval_state) == 1

    @pytest.mark.asyncio
    async def test_all_buttons_share_one_approval_id_and_secret(self, monkeypatch):
        adapter = _make_adapter(monkeypatch)
        adapter._post_preserving_thread = AsyncMock(return_value={"id": "post-1"})
        await adapter.send_exec_approval("chan1", "danger", "sess-1")
        actions = adapter._post_preserving_thread.await_args.args[1]["props"]["attachments"][0]["actions"]
        ids = {a["integration"]["context"]["approval_id"] for a in actions}
        secrets_ = {a["integration"]["context"]["secret"] for a in actions}
        assert len(ids) == 1
        assert len(secrets_) == 1

    @pytest.mark.asyncio
    async def test_falls_back_when_no_public_url(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, public_url=None)
        result = await adapter.send_exec_approval("chan1", "danger", "sess-1")
        assert not result.success
        assert "MATTERMOST_PUBLIC_URL" in result.error

    @pytest.mark.asyncio
    async def test_state_bounded(self, monkeypatch):
        from plugins.platforms.mattermost import adapter as mm
        adapter = _make_adapter(monkeypatch)
        adapter._post_preserving_thread = AsyncMock(return_value={"id": "p"})
        monkeypatch.setattr(mm, "_APPROVAL_STATE_MAX", 3)
        for i in range(5):
            await adapter.send_exec_approval("chan1", f"cmd{i}", f"sess-{i}")
        assert len(adapter._exec_approval_state) <= 3


# ---------------------------------------------------------------------------
# _handle_interactive (button click callback)
# ---------------------------------------------------------------------------

class TestHandleInteractive:
    def _seed(self, adapter, approval_id="aid1", secret="sekret", session_key="sess-1"):
        adapter._exec_approval_state[approval_id] = {
            "session_key": session_key,
            "secret": secret,
        }

    @pytest.mark.asyncio
    async def test_authorized_click_resolves_and_clears_buttons(self, monkeypatch):
        monkeypatch.setenv("MATTERMOST_ALLOWED_USERS", "u1")
        adapter = _make_adapter(monkeypatch)
        self._seed(adapter)

        req = _FakeRequest({
            "user_id": "u1", "user_name": "@alice",
            "context": {"approval_id": "aid1", "choice": "session", "secret": "sekret"},
        })
        with patch("tools.approval.resolve_gateway_approval", return_value=1) as res:
            resp = await adapter._handle_interactive(req)

        res.assert_called_once_with("sess-1", "session")
        body = _body(resp)
        # Buttons removed (attachments emptied) and decision recorded.
        assert body["update"]["props"]["attachments"] == []
        assert "Approved for this session" in body["update"]["message"]
        assert "alice" in body["update"]["message"]
        # Single-use — state popped.
        assert "aid1" not in adapter._exec_approval_state

    @pytest.mark.asyncio
    async def test_deny_choice_maps_through(self, monkeypatch):
        monkeypatch.setenv("MATTERMOST_ALLOWED_USERS", "u1")
        adapter = _make_adapter(monkeypatch)
        self._seed(adapter)
        req = _FakeRequest({
            "user_id": "u1", "user_name": "bob",
            "context": {"approval_id": "aid1", "choice": "deny", "secret": "sekret"},
        })
        with patch("tools.approval.resolve_gateway_approval", return_value=1) as res:
            resp = await adapter._handle_interactive(req)
        res.assert_called_once_with("sess-1", "deny")
        assert "Denied" in _body(resp)["update"]["message"]

    @pytest.mark.asyncio
    async def test_unauthorized_user_rejected(self, monkeypatch):
        monkeypatch.setenv("MATTERMOST_ALLOWED_USERS", "someone-else")
        adapter = _make_adapter(monkeypatch)
        self._seed(adapter)
        req = _FakeRequest({
            "user_id": "intruder",
            "context": {"approval_id": "aid1", "choice": "once", "secret": "sekret"},
        })
        with patch("tools.approval.resolve_gateway_approval") as res:
            resp = await adapter._handle_interactive(req)
        res.assert_not_called()
        assert "not authorized" in _body(resp)["ephemeral_text"].lower()
        # Approval remains pending — an intruder click must not consume it.
        assert "aid1" in adapter._exec_approval_state

    @pytest.mark.asyncio
    async def test_bad_secret_rejected(self, monkeypatch):
        monkeypatch.setenv("MATTERMOST_ALLOWED_USERS", "u1")
        adapter = _make_adapter(monkeypatch)
        self._seed(adapter, secret="real-secret")
        req = _FakeRequest({
            "user_id": "u1",
            "context": {"approval_id": "aid1", "choice": "once", "secret": "forged"},
        })
        with patch("tools.approval.resolve_gateway_approval") as res:
            resp = await adapter._handle_interactive(req)
        res.assert_not_called()
        assert resp.status == 403
        assert "aid1" in adapter._exec_approval_state

    @pytest.mark.asyncio
    async def test_stale_approval_id_reports_resolved(self, monkeypatch):
        monkeypatch.setenv("MATTERMOST_ALLOWED_USERS", "u1")
        adapter = _make_adapter(monkeypatch)
        # No seed → unknown approval_id.
        req = _FakeRequest({
            "user_id": "u1",
            "context": {"approval_id": "missing", "choice": "once", "secret": "x"},
        })
        with patch("tools.approval.resolve_gateway_approval") as res:
            resp = await adapter._handle_interactive(req)
        res.assert_not_called()
        body = _body(resp)
        assert body["update"]["props"]["attachments"] == []
        assert "already been resolved" in body["update"]["message"]

    @pytest.mark.asyncio
    async def test_invalid_choice_rejected(self, monkeypatch):
        monkeypatch.setenv("MATTERMOST_ALLOWED_USERS", "u1")
        adapter = _make_adapter(monkeypatch)
        self._seed(adapter)
        req = _FakeRequest({
            "user_id": "u1",
            "context": {"approval_id": "aid1", "choice": "nuke", "secret": "sekret"},
        })
        resp = await adapter._handle_interactive(req)
        assert resp.status == 400
