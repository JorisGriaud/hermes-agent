"""Mattermost gateway adapter.

Connects to a self-hosted (or cloud) Mattermost instance via its REST API
(v4) and WebSocket for real-time events.  No external Mattermost library
required — uses aiohttp which is already a Hermes dependency.

Environment variables:
    MATTERMOST_URL              Server URL (e.g. https://mm.example.com)
    MATTERMOST_TOKEN            Bot token or personal-access token
    MATTERMOST_ALLOWED_USERS    Comma-separated user IDs
    MATTERMOST_HOME_CHANNEL     Channel ID for cron/notification delivery
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import secrets
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from gateway.config import Platform, PlatformConfig
from gateway.platforms.helpers import MessageDeduplicator
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

logger = logging.getLogger(__name__)

# Mattermost post size limit (server default is 16383, but 4000 is the
# practical limit for readable messages — matching OpenClaw's choice).
MAX_POST_LENGTH = 4000

# Channel type codes returned by the Mattermost API.
_CHANNEL_TYPE_MAP = {
    "D": "dm",
    "G": "group",
    "P": "group",   # private channel → treat as group
    "O": "channel",
}

# Reconnect parameters (exponential backoff).
_RECONNECT_BASE_DELAY = 2.0
_RECONNECT_MAX_DELAY = 60.0
_RECONNECT_JITTER = 0.2

# Interactive-callback server (native slash commands + approval buttons).
# The Mattermost SERVER POSTs to these endpoints, so it must be able to reach
# the adapter — configure MATTERMOST_PUBLIC_URL (recommended) or a routable
# host:port.  Port 8066 sits next to Mattermost's own 8065 without colliding
# with the other Hermes webhook adapters (WhatsApp 8090, LINE 8646, …).
DEFAULT_MATTERMOST_WEBHOOK_PORT = 8066
DEFAULT_MATTERMOST_CALLBACK_PATH = "/mattermost"
# Cap the pending-approval map so never-clicked prompts can't grow unbounded.
_APPROVAL_STATE_MAX = 512

# Mattermost slash-command field limits (Command.IsValid, model/command.go):
# Description is rejected past 64 chars; the user-visible autocomplete
# description tolerates more (stored varchar(1024)).
_MM_DESCRIPTION_MAX = 64
_MM_AUTOCOMPLETE_DESC_MAX = 128


def _env_flag(name: str, default: bool) -> bool:
    """Read a boolean env var, falling back to *default* when unset."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def check_mattermost_requirements() -> bool:
    """Return True if the Mattermost adapter can be used."""
    token = os.getenv("MATTERMOST_TOKEN", "")
    url = os.getenv("MATTERMOST_URL", "")
    if not token:
        logger.debug("Mattermost: MATTERMOST_TOKEN not set")
        return False
    if not url:
        logger.warning("Mattermost: MATTERMOST_URL not set")
        return False
    try:
        import aiohttp  # noqa: F401
        return True
    except ImportError:
        logger.warning("Mattermost: aiohttp not installed")
        return False


class MattermostAdapter(BasePlatformAdapter):
    """Gateway adapter for Mattermost (self-hosted or cloud)."""

    splits_long_messages = True  # send() chunks via truncate_message(MAX_POST_LENGTH)

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.MATTERMOST)

        self._base_url: str = (
            config.extra.get("url", "")
            or os.getenv("MATTERMOST_URL", "")
        ).rstrip("/")
        self._token: str = config.token or os.getenv("MATTERMOST_TOKEN", "")

        self._bot_user_id: str = ""
        self._bot_username: str = ""

        # aiohttp session + websocket handle
        self._session: Any = None  # aiohttp.ClientSession
        self._ws: Any = None       # aiohttp.ClientWebSocketResponse
        self._ws_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        self._closing = False

        # Reply mode: "thread" to nest replies, "off" for flat messages.
        self._reply_mode: str = (
            config.extra.get("reply_mode", "")
            or os.getenv("MATTERMOST_REPLY_MODE", "off")
        ).lower()

        self._last_post_status: Optional[int] = None
        self._last_post_error: str = ""

        # Dedup cache (prevent reprocessing)
        self._dedup = MessageDeduplicator()

        # ------------------------------------------------------------------
        # Interactive callbacks: native slash commands + approval buttons.
        # ------------------------------------------------------------------
        # Mattermost delivers slash-command executions (method=P) and
        # interactive message button clicks as inbound HTTP POSTs, so the
        # adapter runs a small aiohttp server.  The Mattermost server must be
        # able to reach it — hence a public URL (MATTERMOST_PUBLIC_URL) or a
        # directly-routable host:port.  Without a reachable callback base the
        # server is not started and both features degrade gracefully (slash
        # commands stay unregistered; approval falls back to text /approve).
        _extra = config.extra or {}
        self._public_base_url: str = (
            os.getenv("MATTERMOST_PUBLIC_URL")
            or _extra.get("public_url", "")
            or ""
        ).rstrip("/")
        self._webhook_host: str = (
            _extra.get("webhook_host")
            or os.getenv("MATTERMOST_WEBHOOK_HOST", "0.0.0.0")
        )
        try:
            self._webhook_port: int = int(
                _extra.get("webhook_port")
                or os.getenv(
                    "MATTERMOST_WEBHOOK_PORT",
                    str(DEFAULT_MATTERMOST_WEBHOOK_PORT),
                )
            )
        except (TypeError, ValueError):
            self._webhook_port = DEFAULT_MATTERMOST_WEBHOOK_PORT

        _cb_path = str(
            _extra.get("callback_path")
            or os.getenv("MATTERMOST_CALLBACK_PATH", DEFAULT_MATTERMOST_CALLBACK_PATH)
        ).rstrip("/") or DEFAULT_MATTERMOST_CALLBACK_PATH
        if not _cb_path.startswith("/"):
            _cb_path = "/" + _cb_path
        self._callback_path: str = _cb_path
        self._command_path: str = f"{_cb_path}/command"
        self._action_path: str = f"{_cb_path}/action"
        self._health_path: str = f"{_cb_path}/health"

        # Register COMMAND_REGISTRY as native slash commands on connect.
        self._register_commands: bool = _env_flag("MATTERMOST_REGISTER_COMMANDS", True)
        # Optionally delete the commands we created on disconnect.  Default
        # off so restarts stay idempotent (re-registration skips existing
        # triggers and re-adopts their verification tokens).
        self._cleanup_commands: bool = _env_flag("MATTERMOST_CLEANUP_COMMANDS", False)
        # Optional team allowlist for command registration (comma-separated
        # team IDs).  Empty → register in every team the bot belongs to.
        _teams_raw = (
            _extra.get("team_ids")
            or os.getenv("MATTERMOST_TEAM_ID", "")
            or os.getenv("MATTERMOST_TEAM_IDS", "")
        )
        if isinstance(_teams_raw, list):
            self._configured_team_ids: List[str] = [
                str(t).strip() for t in _teams_raw if str(t).strip()
            ]
        else:
            self._configured_team_ids = [
                t.strip() for t in str(_teams_raw).split(",") if t.strip()
            ]

        # aiohttp callback server handles.
        self._app: Any = None       # aiohttp.web.Application
        self._runner: Any = None     # aiohttp.web.AppRunner
        self._site: Any = None       # aiohttp.web.TCPSite

        # Pending exec-approval state: approval_id → {session_key, secret}.
        self._exec_approval_state: Dict[str, Dict[str, str]] = {}
        # Slash-command verification tokens issued by Mattermost at command
        # creation.  Inbound slash requests must present one of these.
        self._command_tokens: set[str] = set()
        # IDs of commands we created this run (for optional cleanup).
        self._created_command_ids: List[str] = []

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    async def _api_get(self, path: str) -> Dict[str, Any]:
        """GET /api/v4/{path}."""
        import aiohttp
        if ".." in path:
            logger.error("MM API path traversal blocked: %s", path)
            return {}
        url = f"{self._base_url}/api/v4/{path.lstrip('/')}"
        try:
            async with self._session.get(url, headers=self._headers(), timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    logger.error("MM API GET %s → %s: %s", path, resp.status, body[:200])
                    return {}
                return await resp.json()
        except aiohttp.ClientError as exc:
            logger.error("MM API GET %s network error: %s", path, exc)
            return {}

    async def _api_post(
        self, path: str, payload: Dict[str, Any], *, log_errors: bool = True
    ) -> Dict[str, Any]:
        """POST /api/v4/{path} with JSON body.

        ``log_errors=False`` suppresses the ERROR log for >=400 responses so
        callers that expect benign 4xx (e.g. a duplicate slash-command
        trigger) can inspect ``self._last_post_status`` / ``_last_post_error``
        and log at their own level.
        """
        import aiohttp
        if ".." in path:
            logger.error("MM API path traversal blocked: %s", path)
            return {}
        url = f"{self._base_url}/api/v4/{path.lstrip('/')}"
        self._last_post_status = None
        self._last_post_error = ""
        try:
            async with self._session.post(
                url, headers=self._headers(), json=payload,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                self._last_post_status = resp.status
                if resp.status >= 400:
                    body = await resp.text()
                    self._last_post_error = body or ""
                    if log_errors:
                        logger.error("MM API POST %s → %s: %s", path, resp.status, body[:200])
                    return {}
                return await resp.json()
        except aiohttp.ClientError as exc:
            self._last_post_error = str(exc)
            if log_errors:
                logger.error("MM API POST %s network error: %s", path, exc)
            return {}

    async def _thread_root_for_send(
        self,
        reply_to: Optional[str],
        metadata: Optional[Dict[str, Any]],
    ) -> Optional[str]:
        """Resolve the Mattermost root_id from reply_to or metadata."""
        if self._reply_mode != "thread":
            return None
        candidate = reply_to
        if not candidate and isinstance(metadata, dict):
            candidate = metadata.get("thread_id") or metadata.get("root_id")
        if not candidate:
            return None
        return await self._resolve_root_id(str(candidate))

    def _last_post_failure_is_broken_thread_root(self) -> bool:
        """Return True only for clear invalid/missing Mattermost thread roots."""
        if self._last_post_status not in {400, 404}:
            return False
        body = (self._last_post_error or "").lower()
        if not body:
            return False
        rootish = any(marker in body for marker in ("root_id", "rootid", "root id", "thread", "post"))
        broken = any(marker in body for marker in ("invalid", "not found", "does not exist", "missing"))
        return rootish and broken

    async def _post_preserving_thread(
        self,
        chat_id: str,
        payload: Dict[str, Any],
        metadata: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Post once, optionally falling back flat for final notify content."""
        data = await self._api_post("posts", payload)
        if data or "root_id" not in payload:
            return data
        if not (isinstance(metadata, dict) and metadata.get("notify")):
            return data
        if not self._last_post_failure_is_broken_thread_root():
            return data

        flat_payload = dict(payload)
        flat_payload.pop("root_id", None)
        original = str(flat_payload.get("message") or "")
        flat_payload["message"] = (
            "⚠️ Mattermost thread delivery failed; posting final reply in channel.\n\n"
            + original
        ).strip()
        logger.warning(
            "Mattermost: falling back to flat channel delivery for notify-worthy post in %s",
            chat_id,
        )
        return await self._api_post("posts", flat_payload)

    async def _api_put(
        self, path: str, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """PUT /api/v4/{path} with JSON body."""
        import aiohttp
        if ".." in path:
            logger.error("MM API path traversal blocked: %s", path)
            return {}
        url = f"{self._base_url}/api/v4/{path.lstrip('/')}"
        try:
            async with self._session.put(
                url, headers=self._headers(), json=payload
            ) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    logger.error("MM API PUT %s → %s: %s", path, resp.status, body[:200])
                    return {}
                return await resp.json()
        except aiohttp.ClientError as exc:
            logger.error("MM API PUT %s network error: %s", path, exc)
            return {}

    async def _api_delete(self, path: str) -> bool:
        """DELETE /api/v4/{path}.  Returns True on success."""
        import aiohttp
        if ".." in path:
            logger.error("MM API path traversal blocked: %s", path)
            return False
        url = f"{self._base_url}/api/v4/{path.lstrip('/')}"
        try:
            async with self._session.delete(
                url, headers=self._headers(),
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    logger.error("MM API DELETE %s → %s: %s", path, resp.status, body[:200])
                    return False
                return True
        except aiohttp.ClientError as exc:
            logger.error("MM API DELETE %s network error: %s", path, exc)
            return False

    async def _upload_file(
        self, channel_id: str, file_data: bytes, filename: str, content_type: str = "application/octet-stream"
    ) -> Optional[str]:
        """Upload a file and return its file ID, or None on failure."""
        import aiohttp

        url = f"{self._base_url}/api/v4/files"
        form = aiohttp.FormData()
        form.add_field("channel_id", channel_id)
        form.add_field(
            "files",
            file_data,
            filename=filename,
            content_type=content_type,
        )
        headers = {"Authorization": f"Bearer {self._token}"}
        async with self._session.post(url, headers=headers, data=form, timeout=aiohttp.ClientTimeout(total=60)) as resp:
            if resp.status >= 400:
                body = await resp.text()
                logger.error("MM file upload → %s: %s", resp.status, body[:200])
                return None
            data = await resp.json()
            infos = data.get("file_infos", [])
            return infos[0]["id"] if infos else None

    # ------------------------------------------------------------------
    # Required overrides
    # ------------------------------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Connect to Mattermost and start the WebSocket listener."""
        import aiohttp

        if not self._base_url or not self._token:
            logger.error("Mattermost: URL or token not configured")
            return False

        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        )
        self._closing = False

        # Verify credentials and fetch bot identity.
        me = await self._api_get("users/me")
        if not me or "id" not in me:
            logger.error("Mattermost: failed to authenticate — check MATTERMOST_TOKEN and MATTERMOST_URL")
            await self._session.close()
            return False

        self._bot_user_id = me["id"]
        self._bot_username = me.get("username", "")
        logger.info(
            "Mattermost: authenticated as @%s (%s) on %s",
            self._bot_username,
            self._bot_user_id,
            self._base_url,
        )

        # Start WebSocket in background.
        self._ws_task = asyncio.create_task(self._ws_loop())

        # Stand up the interactive-callback HTTP server and register native
        # slash commands.  Both are purely additive: any failure here is
        # logged and never blocks the WebSocket transport — the bot keeps
        # working, only slash autocomplete / button approvals are affected.
        try:
            await self._start_callback_server()
            if self._register_commands and self._callbacks_enabled:
                await self._register_slash_commands()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Mattermost: interactive-callback setup failed (%s) — "
                "slash commands / button approvals may be unavailable", exc,
            )

        self._mark_connected()
        return True

    async def disconnect(self) -> None:
        """Disconnect from Mattermost."""
        self._closing = True

        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await self._ws_task
            except (asyncio.CancelledError, Exception):
                pass

        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()

        if self._ws:
            await self._ws.close()
            self._ws = None

        # Tear down the interactive-callback server, then (optionally) remove
        # the slash commands we created.  Both run before the session closes
        # so the cleanup DELETE calls still have a live HTTP client.
        if self._runner is not None:
            try:
                await self._runner.cleanup()
            except Exception:  # noqa: BLE001
                pass
            self._app = self._runner = self._site = None

        if (
            self._cleanup_commands
            and self._created_command_ids
            and self._session
            and not self._session.closed
        ):
            for cid in list(self._created_command_ids):
                if cid:
                    await self._api_delete(f"commands/{cid}")
            self._created_command_ids.clear()

        if self._session and not self._session.closed:
            await self._session.close()

        logger.info("Mattermost: disconnected")

    # ------------------------------------------------------------------
    # Interactive callbacks: HTTP server, slash-command registration,
    # slash-command execution, and authorization.
    # ------------------------------------------------------------------

    def _callback_base(self) -> str:
        """Absolute base URL the Mattermost server uses to reach this adapter.

        Prefers the explicit public URL; otherwise falls back to a directly
        routable host:port.  Returns ``""`` when the adapter is only bound to
        a wildcard/loopback host and no public URL is set — the Mattermost
        server would have no address to POST back to, so callbacks are
        disabled.
        """
        if self._public_base_url:
            return self._public_base_url
        host = (self._webhook_host or "").strip()
        if host in {"", "0.0.0.0", "::", "127.0.0.1", "localhost"}:
            return ""
        return f"http://{host}:{self._webhook_port}"

    @property
    def _callbacks_enabled(self) -> bool:
        """True when a reachable callback base URL can be constructed."""
        return bool(self._callback_base())

    async def _start_callback_server(self) -> None:
        """Start the aiohttp server that receives slash-command executions and
        interactive button clicks.

        No-op when no reachable callback base is configured — the bot still
        works over the WebSocket, only the callback-driven features are off.
        """
        if not self._callbacks_enabled:
            logger.info(
                "Mattermost: MATTERMOST_PUBLIC_URL not set (and no routable "
                "host) — native slash commands and button approvals are "
                "disabled; the text /approve flow still works."
            )
            return
        try:
            from aiohttp import web
        except ImportError:
            logger.warning("Mattermost: aiohttp.web unavailable — callbacks disabled")
            return
        try:
            app = web.Application()
            app.router.add_post(self._command_path, self._handle_slash_command)
            app.router.add_post(self._action_path, self._handle_interactive)
            app.router.add_get(self._health_path, self._handle_health)
            self._app = app
            self._runner = web.AppRunner(app)
            await self._runner.setup()
            self._site = web.TCPSite(self._runner, self._webhook_host, self._webhook_port)
            await self._site.start()
            logger.info(
                "Mattermost: callback server listening on %s:%d (command=%s, action=%s); "
                "Mattermost will reach it at %s",
                self._webhook_host, self._webhook_port,
                self._command_path, self._action_path, self._callback_base(),
            )
        except OSError as exc:
            logger.warning(
                "Mattermost: could not bind callback server on %s:%d (%s) — "
                "slash commands / button approvals disabled",
                self._webhook_host, self._webhook_port, exc,
            )
            self._app = self._runner = self._site = None

    async def _handle_health(self, request: Any) -> Any:
        """Liveness endpoint for the callback server."""
        from aiohttp import web
        return web.json_response({"status": "ok", "bot": self._bot_username})

    def _is_action_user_authorized(self, user_id: str) -> bool:
        """Return True if *user_id* may run commands / approve dangerous ones.

        Mirrors Discord's component-auth logic: allow-all opt-in, the
        Mattermost + gateway allowlists (plus the ``config.extra`` allowlist
        and a ``*`` wildcard), then the pairing store.  Fails closed when no
        allowlist is configured.
        """
        uid = str(user_id or "").strip()
        if not uid:
            return False
        if _env_flag("MATTERMOST_ALLOW_ALL_USERS", False) or _env_flag(
            "GATEWAY_ALLOW_ALL_USERS", False
        ):
            return True
        allowed = {
            u.strip()
            for u in os.getenv("MATTERMOST_ALLOWED_USERS", "").split(",")
            if u.strip()
        }
        allowed |= {
            u.strip()
            for u in os.getenv("GATEWAY_ALLOWED_USERS", "").split(",")
            if u.strip()
        }
        _extra_allowed = (self.config.extra or {}).get("allowed_users")
        if isinstance(_extra_allowed, list):
            allowed |= {str(u).strip() for u in _extra_allowed if str(u).strip()}
        elif isinstance(_extra_allowed, str):
            allowed |= {u.strip() for u in _extra_allowed.split(",") if u.strip()}
        if "*" in allowed or uid in allowed:
            return True
        try:
            from gateway.pairing import PairingStore
            if PairingStore().is_approved("mattermost", uid):
                return True
        except Exception:  # noqa: BLE001
            pass
        return False

    async def _dispatch_safely(self, event: MessageEvent) -> None:
        """Run handle_message in a background task without losing exceptions."""
        try:
            await self.handle_message(event)
        except Exception:  # noqa: BLE001
            logger.exception("Mattermost: error dispatching %s", event.message_type)

    async def _handle_slash_command(self, request: Any) -> Any:
        """Handle a native Mattermost slash-command execution (method=P).

        Mattermost POSTs ``application/x-www-form-urlencoded`` fields including
        ``token`` (the per-command verification token issued at creation),
        ``command`` (e.g. ``/model``), ``text`` (arguments), ``user_id``,
        ``channel_id``, and ``root_id``.  We verify the token, authorize the
        caller, rebuild the ``/command args`` string, and dispatch it through
        the same MessageEvent pipeline as WebSocket messages.  The reply is
        delivered asynchronously via the REST API, so we return an immediate
        200 to stay inside Mattermost's slash-command response deadline.
        """
        from aiohttp import web
        try:
            data = await request.post()
        except Exception:  # noqa: BLE001
            return web.json_response({"text": "Invalid request"}, status=400)

        token = str(data.get("token", ""))
        if self._command_tokens:
            if token not in self._command_tokens:
                logger.warning("Mattermost: rejected slash command with invalid token")
                return web.json_response(
                    {"response_type": "ephemeral", "text": "⛔ Unrecognized command token."},
                    status=401,
                )
        else:
            # No tokens captured (e.g. the list API omitted them for a
            # non-admin bot after a restart).  Fall back to user-based
            # authorization only and warn the operator.
            logger.warning(
                "Mattermost: no slash-command tokens captured — accepting on "
                "user authorization alone; grant the bot manage_slash_commands "
                "so verification tokens are returned."
            )

        user_id = str(data.get("user_id", ""))
        user_name = str(data.get("user_name", "")).lstrip("@")
        channel_id = str(data.get("channel_id", ""))
        root_id = str(data.get("root_id", "")) or None
        trigger = str(data.get("command", "")).strip()      # includes leading '/'
        text = str(data.get("text", "")).strip()

        if not self._is_action_user_authorized(user_id):
            return web.json_response(
                {"response_type": "ephemeral",
                 "text": "⛔ You are not authorized to use this bot."},
            )

        if not trigger:
            return web.json_response({}, status=200)
        command_text = f"{trigger} {text}".strip()

        chat_type = "channel"
        try:
            info = await self.get_chat_info(channel_id)
            chat_type = info.get("type", "channel")
        except Exception:  # noqa: BLE001
            pass

        source = self.build_source(
            chat_id=channel_id,
            chat_type=chat_type,
            user_id=user_id,
            user_name=user_name or user_id,
            thread_id=root_id,
            message_id="",
        )
        from gateway.platforms.base import resolve_channel_prompt
        channel_prompt = resolve_channel_prompt(self.config.extra, channel_id, None)
        event = MessageEvent(
            text=command_text,
            message_type=MessageType.COMMAND,
            source=source,
            raw_message=dict(data),
            message_id="",
            channel_prompt=channel_prompt,
        )
        asyncio.create_task(self._dispatch_safely(event))
        # Empty 200 body → Mattermost shows nothing; the real reply is posted
        # back to the channel by the command handler.
        return web.json_response({})

    # ------------------------------------------------------------------
    # Slash-command registration (idempotent)
    # ------------------------------------------------------------------

    async def _resolve_registration_teams(self) -> List[str]:
        """Return the team IDs to register commands in.

        Uses the configured allowlist when set, otherwise every team the bot
        currently belongs to.
        """
        if self._configured_team_ids:
            return self._configured_team_ids
        data = await self._api_get("users/me/teams")
        teams: List[str] = []
        if isinstance(data, list):
            for team in data:
                if isinstance(team, dict) and team.get("id"):
                    teams.append(str(team["id"]))
        return teams

    async def _list_team_commands(self, team_id: str) -> Dict[str, Dict[str, Any]]:
        """Return ``{trigger: command_object}`` for a team's custom commands."""
        data = await self._api_get(f"commands?team_id={team_id}&custom_only=true")
        result: Dict[str, Dict[str, Any]] = {}
        if isinstance(data, list):
            for cmd in data:
                if isinstance(cmd, dict) and cmd.get("trigger"):
                    result[str(cmd["trigger"])] = cmd
        return result

    async def _create_slash_command(
        self, team_id: str, trigger: str, description: str, hint: str, url: str
    ) -> bool:
        """Create one custom slash command; return True on success.

        Failures are non-fatal.  Mattermost caps ``Description`` at 64 chars
        (``Command.IsValid``) while the user-visible ``auto_complete_desc``
        tolerates more, so they are truncated independently.  A duplicate
        trigger (already registered) is expected on restart and logged quietly.
        """
        desc = " ".join((description or "").split())  # collapse whitespace → single line
        payload = {
            "team_id": team_id,
            "trigger": trigger,
            "method": "P",
            "url": url,
            "auto_complete": True,
            "auto_complete_desc": desc[:_MM_AUTOCOMPLETE_DESC_MAX],
            "auto_complete_hint": hint or "",
            "display_name": f"/{trigger}",
            "description": desc[:_MM_DESCRIPTION_MAX],
        }
        data = await self._api_post("commands", payload, log_errors=False)
        if not data or "id" not in data:
            err = self._last_post_error or ""
            if "duplicate_trigger" in err:
                logger.debug("Mattermost: /%s already registered — skipping", trigger)
            else:
                logger.warning(
                    "Mattermost: could not register /%s: %s", trigger, err[:160]
                )
            return False
        if data.get("token"):
            self._command_tokens.add(data["token"])
        self._created_command_ids.append(data["id"])
        return True

    async def _register_slash_commands(self) -> None:
        """Register COMMAND_REGISTRY as native Mattermost slash commands.

        Idempotent: existing triggers (created on a prior run, or built into
        Mattermost) are left untouched — only missing triggers are created,
        and existing commands that point at our callback URL have their
        verification tokens re-adopted so inbound executions still validate
        across restarts.  Requires the bot token to hold
        ``manage_slash_commands`` (or be a system-admin token).  Failures are
        logged and never block connect.
        """
        try:
            from hermes_cli.commands import mattermost_slash_commands
            entries, hidden = mattermost_slash_commands()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Mattermost: could not build slash command list: %s", exc)
            return

        team_ids = await self._resolve_registration_teams()
        if not team_ids:
            logger.warning(
                "Mattermost: bot is in no teams (or MATTERMOST_TEAM_ID unmatched) "
                "— skipping slash command registration"
            )
            return

        action_url = f"{self._callback_base()}{self._command_path}"
        total_created = 0
        total_updated = 0
        for team_id in team_ids:
            existing = await self._list_team_commands(team_id)
            for trigger, description, hint in entries:
                cmd = existing.get(trigger)
                if cmd is not None:
                    if str(cmd.get("url", "")).rstrip("/") == action_url.rstrip("/"):
                        # Same callback URL — re-adopt its verification token
                        # so inbound executions validate without recreating.
                        if cmd.get("token"):
                            self._command_tokens.add(str(cmd["token"]))
                        if cmd.get("id"):
                            self._created_command_ids.append(str(cmd["id"]))
                        continue
                    # Stale callback URL (e.g. MATTERMOST_PUBLIC_URL changed) —
                    # delete and recreate so the command points at us again.
                    if cmd.get("id"):
                        await self._api_delete(f"commands/{cmd['id']}")
                    if await self._create_slash_command(
                        team_id, trigger, description, hint, action_url
                    ):
                        total_updated += 1
                    continue
                if await self._create_slash_command(
                    team_id, trigger, description, hint, action_url
                ):
                    total_created += 1

        logger.info(
            "Mattermost: slash command registration complete — %d created, %d "
            "re-pointed across %d team(s); %d skill(s) over cap; %d token(s) active",
            total_created, total_updated, len(team_ids), hidden,
            len(self._command_tokens),
        )

    # ------------------------------------------------------------------
    # Interactive approval buttons (dangerous-command approval)
    # ------------------------------------------------------------------

    def _store_approval(self, approval_id: str, session_key: str, secret: str) -> None:
        """Record a pending approval, dropping the oldest entry past the cap."""
        if len(self._exec_approval_state) >= _APPROVAL_STATE_MAX:
            try:
                oldest = next(iter(self._exec_approval_state))
                self._exec_approval_state.pop(oldest, None)
            except StopIteration:
                pass
        self._exec_approval_state[approval_id] = {
            "session_key": session_key,
            "secret": secret,
        }

    async def send_exec_approval(
        self,
        chat_id: str,
        command: str,
        session_key: str,
        description: str = "dangerous command",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Post a dangerous-command approval prompt with interactive buttons.

        Renders a Mattermost message attachment with four action buttons —
        Allow Once / Allow Session / Always Allow / Deny — each carrying an
        ``integration.url`` pointing back at this adapter's callback server.
        Tapping a button resolves the waiting agent thread via
        ``tools.approval.resolve_gateway_approval`` — the same mechanism as
        the text ``/approve`` flow (mirrors Discord's ``ExecApprovalView``).

        Returns an error result when no reachable callback URL is configured
        so the gateway falls back to the plain-text approval prompt.
        """
        if not self._session or self._session.closed:
            return SendResult(success=False, error="Not connected")
        if not self._callbacks_enabled:
            return SendResult(
                success=False,
                error="Interactive approvals require MATTERMOST_PUBLIC_URL",
            )

        approval_id = uuid.uuid4().hex[:16]
        secret = secrets.token_urlsafe(16)
        action_url = f"{self._callback_base()}{self._action_path}"

        cmd = command or ""
        cmd_preview = cmd if len(cmd) <= 3500 else cmd[:3500] + "..."

        def _action(choice: str, name: str, style: Optional[str] = None) -> Dict[str, Any]:
            action: Dict[str, Any] = {
                "id": choice,
                "name": name,
                "integration": {
                    "url": action_url,
                    "context": {
                        "approval_id": approval_id,
                        "choice": choice,
                        "secret": secret,
                    },
                },
            }
            if style:
                action["style"] = style
            return action

        attachment = {
            "color": "#E0A800",
            "title": "⚠️ Command Approval Required",
            "text": f"```\n{cmd_preview}\n```\n**Reason:** {description}",
            "actions": [
                _action("once", "Allow Once", "primary"),
                _action("session", "Allow Session"),
                _action("always", "Always Allow"),
                _action("deny", "Deny", "danger"),
            ],
        }
        payload: Dict[str, Any] = {
            "channel_id": chat_id,
            "props": {"attachments": [attachment]},
        }
        resolved_root = await self._thread_root_for_send(None, metadata)
        if resolved_root:
            payload["root_id"] = resolved_root

        data = await self._post_preserving_thread(chat_id, payload, metadata)
        if not data or "id" not in data:
            return SendResult(success=False, error="Failed to post approval prompt")

        self._store_approval(approval_id, session_key, secret)
        return SendResult(success=True, message_id=data["id"])

    async def _handle_interactive(self, request: Any) -> Any:
        """Handle an interactive message button click from Mattermost.

        Mattermost POSTs JSON with ``user_id``, ``user_name``, ``channel_id``
        and the ``context`` dict we attached to the button.  We verify the
        per-approval secret (proving the click came from our message),
        authorize the clicking user, resolve the approval, and return an
        ``update`` payload that rewrites the post and drops the buttons so the
        decision can't be re-clicked.
        """
        from aiohttp import web
        try:
            body = await request.json()
        except Exception:  # noqa: BLE001
            return web.json_response({"ephemeral_text": "Invalid request"}, status=400)

        ctx = body.get("context") or {}
        approval_id = str(ctx.get("approval_id", ""))
        choice = str(ctx.get("choice", ""))
        secret = str(ctx.get("secret", ""))
        user_id = str(body.get("user_id", ""))
        user_name = str(body.get("user_name", "")).lstrip("@") or "user"

        if choice not in {"once", "session", "always", "deny"}:
            return web.json_response({"ephemeral_text": "Invalid action."}, status=400)

        if not self._is_action_user_authorized(user_id):
            return web.json_response(
                {"ephemeral_text": "⛔ You are not authorized to approve commands."}
            )

        state = self._exec_approval_state.get(approval_id)
        if not state:
            # Already resolved (or expired / cross-restart) — clear the
            # buttons so a stale prompt can't be re-clicked.
            return web.json_response({
                "update": {
                    "message": "⏱ This approval prompt has already been resolved.",
                    "props": {"attachments": []},
                }
            })

        if not secrets.compare_digest(secret, str(state.get("secret", ""))):
            logger.warning(
                "Mattermost: approval action with bad secret (approval_id=%s)", approval_id
            )
            return web.json_response(
                {"ephemeral_text": "⛔ Invalid approval token."}, status=403
            )

        # Single-use: pop so a double-click lands on the "already resolved" path.
        session_key = self._exec_approval_state.pop(approval_id, {}).get("session_key", "")

        label_map = {
            "once": "✅ Approved once",
            "session": "✅ Approved for this session",
            "always": "✅ Approved permanently",
            "deny": "❌ Denied",
        }
        label = label_map.get(choice, "Resolved")

        try:
            from tools.approval import resolve_gateway_approval
            count = resolve_gateway_approval(session_key, choice)
            logger.info(
                "Mattermost button resolved %d approval(s) for session %s "
                "(choice=%s, user=%s)",
                count, session_key, choice, user_name,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Mattermost: failed to resolve gateway approval: %s", exc)

        return web.json_response({
            "update": {
                "message": f"{label} by @{user_name}",
                "props": {"attachments": []},
            }
        })


    async def _resolve_root_id(self, post_id: str) -> str:
        """Resolve a post_id to the thread root_id for Mattermost.

        Mattermost requires root_id to be the *root* post of a thread.
        If the post is a reply (has its own root_id), we must use that
        root_id instead.  Using a reply's own ID as root_id causes
        "Invalid RootId parameter" errors.
        """
        if not post_id:
            return post_id
        # Check if this post has a root_id (meaning it's a reply)
        data = await self._api_get(f"posts/{post_id}")
        if data and data.get("root_id"):
            return data["root_id"]
        return post_id

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a message (or multiple chunks) to a channel."""
        if not content:
            return SendResult(success=True)

        formatted = self.format_message(content)
        chunks = self.truncate_message(formatted, MAX_POST_LENGTH)

        last_id = None
        for chunk in chunks:
            payload: Dict[str, Any] = {
                "channel_id": chat_id,
                "message": chunk,
            }
            # Thread support: reply_to or metadata["thread_id"] is the root post ID.
            resolved_root = await self._thread_root_for_send(reply_to, metadata)
            if resolved_root:
                payload["root_id"] = resolved_root

            data = await self._post_preserving_thread(chat_id, payload, metadata)
            if not data or "id" not in data:
                return SendResult(success=False, error="Failed to create post")
            last_id = data["id"]

        return SendResult(success=True, message_id=last_id)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return channel name and type."""
        data = await self._api_get(f"channels/{chat_id}")
        if not data:
            return {"name": chat_id, "type": "channel"}

        ch_type = _CHANNEL_TYPE_MAP.get(data.get("type", "O"), "channel")
        display_name = data.get("display_name") or data.get("name") or chat_id
        return {"name": display_name, "type": ch_type}

    # ------------------------------------------------------------------
    # Optional overrides
    # ------------------------------------------------------------------

    async def send_typing(
        self, chat_id: str, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Send a typing indicator."""
        await self._api_post(
            f"users/{self._bot_user_id}/typing",
            {"channel_id": chat_id},
        )

    async def edit_message(
        self, chat_id: str, message_id: str, content: str, *, finalize: bool = False
    ) -> SendResult:
        """Edit an existing post."""
        formatted = self.format_message(content)
        data = await self._api_put(
            f"posts/{message_id}/patch",
            {"message": formatted},
        )
        if not data or "id" not in data:
            return SendResult(success=False, error="Failed to edit post")
        return SendResult(success=True, message_id=data["id"])

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Download an image and upload it as a file attachment."""
        return await self._send_url_as_file(
            chat_id, image_url, caption, reply_to, "image", metadata
        )

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Upload a local image file."""
        return await self._send_local_file(
            chat_id, image_path, caption, reply_to, metadata=metadata
        )

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Upload a local file as a document."""
        return await self._send_local_file(
            chat_id, file_path, caption, reply_to, file_name, metadata
        )

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Upload an audio file."""
        return await self._send_local_file(
            chat_id, audio_path, caption, reply_to, metadata=metadata
        )

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Upload a video file."""
        return await self._send_local_file(
            chat_id, video_path, caption, reply_to, metadata=metadata
        )

    def format_message(self, content: str) -> str:
        """Mattermost uses standard Markdown — mostly pass through.

        Strip image markdown into plain links (files are uploaded separately).
        """
        # Convert ![alt](url) to just the URL — Mattermost renders
        # image URLs as inline previews automatically.
        content = re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", r"\2", content)
        return content

    # ------------------------------------------------------------------
    # File helpers
    # ------------------------------------------------------------------

    async def _send_url_as_file(
        self,
        chat_id: str,
        url: str,
        caption: Optional[str],
        reply_to: Optional[str],
        kind: str = "file",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Download a URL and upload it as a file attachment."""
        from tools.url_safety import is_safe_url
        if not is_safe_url(url):
            logger.warning("Mattermost: blocked unsafe URL (SSRF protection)")
            return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), reply_to, metadata=metadata)

        import aiohttp

        file_data = None
        ct = "application/octet-stream"
        fname = url.rsplit("/", 1)[-1].split("?")[0] or f"{kind}.png"

        for attempt in range(3):
            try:
                async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status >= 500 or resp.status == 429:
                        if attempt < 2:
                            logger.debug("Mattermost download retry %d/2 for %s (status %d)",
                                         attempt + 1, url[:80], resp.status)
                            await asyncio.sleep(1.5 * (attempt + 1))
                            continue
                    if resp.status >= 400:
                        return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), reply_to, metadata=metadata)
                    file_data = await resp.read()
                    ct = resp.content_type or "application/octet-stream"
                    break
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt < 2:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
                logger.warning("Mattermost: failed to download %s after %d attempts: %s", url, attempt + 1, exc)
                return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), reply_to, metadata=metadata)

        if file_data is None:
            logger.warning("Mattermost: download returned no data for %s", url)
            return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), reply_to, metadata=metadata)

        file_id = await self._upload_file(chat_id, file_data, fname, ct)
        if not file_id:
            return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), reply_to, metadata=metadata)

        payload: Dict[str, Any] = {
            "channel_id": chat_id,
            "message": caption or "",
            "file_ids": [file_id],
        }
        resolved_root = await self._thread_root_for_send(reply_to, metadata)
        if resolved_root:
            payload["root_id"] = resolved_root

        data = await self._post_preserving_thread(chat_id, payload, metadata)
        if not data or "id" not in data:
            return SendResult(success=False, error="Failed to post with file")
        return SendResult(success=True, message_id=data["id"])

    async def _send_local_file(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str],
        reply_to: Optional[str],
        file_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Upload a local file and attach it to a post."""
        import mimetypes

        p = Path(file_path)
        if not p.exists():
            logger.warning(
                "Mattermost: local file not found, skipping: %s", file_path
            )
            return SendResult(success=True, message_id=None)

        fname = file_name or p.name
        ct = mimetypes.guess_type(fname)[0] or "application/octet-stream"
        file_data = p.read_bytes()

        file_id = await self._upload_file(chat_id, file_data, fname, ct)
        if not file_id:
            return SendResult(success=False, error="File upload failed")

        payload: Dict[str, Any] = {
            "channel_id": chat_id,
            "message": caption or "",
            "file_ids": [file_id],
        }
        resolved_root = await self._thread_root_for_send(reply_to, metadata)
        if resolved_root:
            payload["root_id"] = resolved_root

        data = await self._post_preserving_thread(chat_id, payload, metadata)
        if not data or "id" not in data:
            return SendResult(success=False, error="Failed to post with file")
        return SendResult(success=True, message_id=data["id"])

    async def send_multiple_images(
        self,
        chat_id: str,
        images: List[Tuple[str, str]],
        metadata: Optional[Dict[str, Any]] = None,
        human_delay: float = 0.0,
    ) -> None:
        """Send a batch of images as a single Mattermost post with multiple attachments.

        Mattermost supports up to 5 ``file_ids`` per post. Each image is
        uploaded individually (Mattermost's file API is one-at-a-time),
        then a single post is created referencing all uploaded file_ids
        at once. Batches larger than 5 are chunked. Falls back to the
        base per-image loop on total failure.
        """
        if not images:
            return

        import mimetypes
        import aiohttp
        from urllib.parse import unquote as _unquote

        CHUNK = 5  # Mattermost post file_ids cap
        chunks = [images[i:i + CHUNK] for i in range(0, len(images), CHUNK)]

        for chunk_idx, chunk in enumerate(chunks):
            if human_delay > 0 and chunk_idx > 0:
                await asyncio.sleep(human_delay)

            file_ids: List[str] = []
            caption_parts: List[str] = []
            try:
                for image_url, alt_text in chunk:
                    if alt_text:
                        caption_parts.append(alt_text)

                    if image_url.startswith("file://"):
                        local_path = _unquote(image_url[7:])
                        p = Path(local_path)
                        if not p.exists():
                            logger.warning("Mattermost: skipping missing image %s", local_path)
                            continue
                        fname = p.name
                        ct = mimetypes.guess_type(fname)[0] or "image/png"
                        file_data = p.read_bytes()
                    else:
                        from tools.url_safety import is_safe_url
                        if not is_safe_url(image_url):
                            logger.warning("Mattermost: blocked unsafe image URL in batch")
                            continue
                        try:
                            async with self._session.get(
                                image_url, timeout=aiohttp.ClientTimeout(total=30)
                            ) as resp:
                                if resp.status >= 400:
                                    logger.warning(
                                        "Mattermost: failed to download image (HTTP %d): %s",
                                        resp.status, image_url[:80],
                                    )
                                    continue
                                file_data = await resp.read()
                                ct = resp.content_type or "image/png"
                        except Exception as dl_err:
                            logger.warning("Mattermost: download failed for %s: %s", image_url[:80], dl_err)
                            continue
                        fname = image_url.rsplit("/", 1)[-1].split("?")[0] or f"image_{len(file_ids)}.png"

                    fid = await self._upload_file(chat_id, file_data, fname, ct)
                    if fid:
                        file_ids.append(fid)

                if not file_ids:
                    continue

                payload: Dict[str, Any] = {
                    "channel_id": chat_id,
                    "message": "\n".join(caption_parts),
                    "file_ids": file_ids,
                }
                resolved_root = await self._thread_root_for_send(None, metadata)
                if resolved_root:
                    payload["root_id"] = resolved_root
                logger.info(
                    "Mattermost: sending %d image(s) as single post (chunk %d/%d)",
                    len(file_ids), chunk_idx + 1, len(chunks),
                )
                data = await self._post_preserving_thread(chat_id, payload, metadata)
                if not data or "id" not in data:
                    logger.warning("Mattermost: multi-image post failed, falling back")
                    await super().send_multiple_images(chat_id, chunk, metadata, human_delay=human_delay)
            except Exception as e:
                logger.warning(
                    "Mattermost: multi-image send failed (chunk %d/%d), falling back: %s",
                    chunk_idx + 1, len(chunks), e, exc_info=True,
                )
                await super().send_multiple_images(chat_id, chunk, metadata, human_delay=human_delay)

    # ------------------------------------------------------------------
    # WebSocket
    # ------------------------------------------------------------------

    async def _ws_loop(self) -> None:
        """Connect to the WebSocket and listen for events, reconnecting on failure."""
        delay = _RECONNECT_BASE_DELAY
        while not self._closing:
            try:
                await self._ws_connect_and_listen()
                # Clean disconnect — reset delay.
                delay = _RECONNECT_BASE_DELAY
            except asyncio.CancelledError:
                return
            except Exception as exc:
                if self._closing:
                    return
                # Detect permanent auth/permission failures that will never
                # succeed on retry — stop reconnecting instead of looping forever.
                import aiohttp
                err_str = str(exc).lower()
                if isinstance(exc, aiohttp.WSServerHandshakeError) and exc.status in {401, 403}:
                    logger.error("Mattermost WS auth failed (HTTP %d) — stopping reconnect", exc.status)
                    return
                if "401" in err_str or "403" in err_str or "unauthorized" in err_str:
                    logger.error("Mattermost WS permanent error: %s — stopping reconnect", exc)
                    return
                logger.warning("Mattermost WS error: %s — reconnecting in %.0fs", exc, delay)

            if self._closing:
                return

            # Exponential backoff with jitter.
            import random
            jitter = delay * _RECONNECT_JITTER * random.random()
            await asyncio.sleep(delay + jitter)
            delay = min(delay * 2, _RECONNECT_MAX_DELAY)

    async def _ws_connect_and_listen(self) -> None:
        """Single WebSocket session: connect, authenticate, process events."""
        # Build WS URL: https:// → wss://, http:// → ws://
        ws_url = re.sub(r"^http", "ws", self._base_url) + "/api/v4/websocket"
        logger.info("Mattermost: connecting to %s", ws_url)

        self._ws = await self._session.ws_connect(ws_url, heartbeat=30.0)

        # Authenticate via the WebSocket.
        auth_msg = {
            "seq": 1,
            "action": "authentication_challenge",
            "data": {"token": self._token},
        }
        await self._ws.send_json(auth_msg)
        logger.info("Mattermost: WebSocket connected and authenticated")

        async for raw_msg in self._ws:
            if self._closing:
                return

            if raw_msg.type in {
                raw_msg.type.TEXT,
                raw_msg.type.BINARY,
            }:
                try:
                    event = json.loads(raw_msg.data)
                except (json.JSONDecodeError, TypeError):
                    continue
                await self._handle_ws_event(event)
            elif raw_msg.type in {
                raw_msg.type.ERROR,
                raw_msg.type.CLOSE,
                raw_msg.type.CLOSING,
                raw_msg.type.CLOSED,
            }:
                logger.info("Mattermost: WebSocket closed (%s)", raw_msg.type)
                break

    async def _handle_ws_event(self, event: Dict[str, Any]) -> None:
        """Process a single WebSocket event."""
        event_type = event.get("event")
        if event_type != "posted":
            return

        data = event.get("data", {})
        raw_post_str = data.get("post")
        if not raw_post_str:
            return

        try:
            post = json.loads(raw_post_str)
        except (json.JSONDecodeError, TypeError):
            return

        # Ignore own messages.
        if post.get("user_id") == self._bot_user_id:
            return

        # Ignore system posts.
        if post.get("type"):
            return

        post_id = post.get("id", "")

        # Dedup.
        if self._dedup.is_duplicate(post_id):
            return

        # Build message event.
        channel_id = post.get("channel_id", "")
        channel_type_raw = data.get("channel_type", "O")
        chat_type = _CHANNEL_TYPE_MAP.get(channel_type_raw, "channel")

        # For DMs, user_id is sufficient.  For channels, check for @mention.
        message_text = post.get("message", "")

        # Mention-gating for non-DM channels.
        # Config (config.yaml `mattermost.*` with env-var fallback):
        #   require_mention / MATTERMOST_REQUIRE_MENTION: Require @mention in channels (default: true)
        #   free_response_channels / MATTERMOST_FREE_RESPONSE_CHANNELS: Channel IDs where bot responds without mention
        #   allowed_channels / MATTERMOST_ALLOWED_CHANNELS: If set, bot ONLY responds in these channels (whitelist)
        if channel_type_raw != "D":
            # allowed_channels check (whitelist — must pass before other gating).
            # When set, messages from channels NOT in this list are silently
            # ignored, even if @mentioned.  DMs are already excluded above.
            allowed_raw = self.config.extra.get("allowed_channels") if self.config.extra else None
            if allowed_raw is None:
                allowed_raw = os.getenv("MATTERMOST_ALLOWED_CHANNELS", "")
            if isinstance(allowed_raw, list):
                allowed_channels = {str(c).strip() for c in allowed_raw if str(c).strip()}
            else:
                allowed_channels = {
                    c.strip() for c in str(allowed_raw).split(",") if c.strip()
                }
            if allowed_channels and channel_id not in allowed_channels:
                logger.debug(
                    "Mattermost: ignoring message in non-allowed channel: %s",
                    channel_id,
                )
                return

            require_mention = os.getenv(
                "MATTERMOST_REQUIRE_MENTION", "true"
            ).lower() not in {"false", "0", "no"}

            free_channels_raw = os.getenv("MATTERMOST_FREE_RESPONSE_CHANNELS", "")
            free_channels = {ch.strip() for ch in free_channels_raw.split(",") if ch.strip()}
            is_free_channel = channel_id in free_channels

            mention_patterns = [
                f"@{self._bot_username}",
                f"@{self._bot_user_id}",
            ]
            has_mention = any(
                pattern.lower() in message_text.lower()
                for pattern in mention_patterns
            )

            if require_mention and not is_free_channel and not has_mention:
                logger.debug(
                    "Mattermost: skipping non-DM message without @mention (channel=%s)",
                    channel_id,
                )
                return

            # Strip @mention from the message text so the agent sees clean input.
            if has_mention:
                for pattern in mention_patterns:
                    message_text = re.sub(
                        re.escape(pattern), "", message_text, flags=re.IGNORECASE
                    ).strip()

        # Resolve sender info.
        sender_id = post.get("user_id", "")
        sender_name = data.get("sender_name", "").lstrip("@") or sender_id

        # Thread support: if the post is in a thread, use root_id. In
        # thread mode, top-level channel posts are valid roots for progress.
        thread_id = post.get("root_id") or None
        if (
            not thread_id
            and self._reply_mode == "thread"
            and channel_type_raw != "D"
            and post_id
        ):
            thread_id = post_id

        # Determine message type.
        file_ids = post.get("file_ids") or []
        msg_type = MessageType.TEXT
        if message_text[:1].isspace() and message_text.lstrip().startswith("/"):
            message_text = message_text.lstrip()
        if message_text.startswith("/"):
            msg_type = MessageType.COMMAND

        # Download file attachments immediately (URLs require auth headers
        # that downstream tools won't have).
        media_urls: List[str] = []
        media_types: List[str] = []
        for fid in file_ids:
            try:
                file_info = await self._api_get(f"files/{fid}/info")
                fname = file_info.get("name", f"file_{fid}")
                ext = Path(fname).suffix or ""
                mime = file_info.get("mime_type", "application/octet-stream")

                import aiohttp
                dl_url = f"{self._base_url}/api/v4/files/{fid}"
                async with self._session.get(
                    dl_url,
                    headers={"Authorization": f"Bearer {self._token}"},
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status < 400:
                        file_data = await resp.read()
                        from gateway.platforms.base import cache_image_from_bytes, cache_document_from_bytes
                        if mime.startswith("image/"):
                            local_path = cache_image_from_bytes(file_data, ext or ".png")
                            media_urls.append(local_path)
                            media_types.append(mime)
                        elif mime.startswith("audio/"):
                            from gateway.platforms.base import cache_audio_from_bytes
                            local_path = cache_audio_from_bytes(file_data, ext or ".ogg")
                            media_urls.append(local_path)
                            media_types.append(mime)
                        else:
                            local_path = cache_document_from_bytes(file_data, fname)
                            media_urls.append(local_path)
                            media_types.append(mime)
                    else:
                        logger.warning("Mattermost: failed to download file %s: HTTP %s", fid, resp.status)
            except Exception as exc:
                logger.warning("Mattermost: error downloading file %s: %s", fid, exc)

        # Set message type based on downloaded media types.
        if media_types and msg_type == MessageType.TEXT:
            if any(m.startswith("image/") for m in media_types):
                msg_type = MessageType.PHOTO
            elif any(m.startswith("audio/") for m in media_types):
                msg_type = MessageType.VOICE
            elif media_types:
                msg_type = MessageType.DOCUMENT

        source = self.build_source(
            chat_id=channel_id,
            chat_type=chat_type,
            user_id=sender_id,
            user_name=sender_name,
            thread_id=thread_id,
            message_id=post_id,
        )

        # Per-channel ephemeral prompt
        from gateway.platforms.base import resolve_channel_prompt
        _channel_prompt = resolve_channel_prompt(
            self.config.extra, channel_id, None,
        )

        msg_event = MessageEvent(
            text=message_text,
            message_type=msg_type,
            source=source,
            raw_message=post,
            message_id=post_id,
            media_urls=media_urls if media_urls else None,
            media_types=media_types if media_types else None,
            channel_prompt=_channel_prompt,
        )

        await self.handle_message(msg_event)




# ---------------------------------------------------------------------------
# Plugin standalone-send (out-of-process cron delivery via Mattermost REST)
# ---------------------------------------------------------------------------


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[list] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Send via the Mattermost v4 REST API without a live gateway adapter.

    Used by ``tools/send_message_tool._send_via_adapter`` when the gateway
    runner is not in this process (typical for cron jobs running out-of-process).
    Reads ``MATTERMOST_TOKEN`` from ``pconfig.token`` (set by the gateway
    config loader from env) and falls back to the ``MATTERMOST_TOKEN`` env
    var.  Server URL comes from ``pconfig.extra["url"]`` (set by the YAML
    bridge / env loader) or the ``MATTERMOST_URL`` env var.

    Thread replies (Mattermost CRT) are supported via the ``root_id`` field
    on the ``POST /posts`` payload — pass ``thread_id`` when threading is
    desired.  ``media_files`` are uploaded via ``POST /files``
    (multipart/form-data), then their returned ``file_id`` values are
    attached to the post.

    ``force_document`` is accepted for signature parity with other
    standalone senders but unused — Mattermost stores every uploaded file
    as a generic attachment regardless.
    """
    try:
        import aiohttp
    except ImportError:
        return {"error": "aiohttp not installed. Run: pip install aiohttp"}

    base_url = (
        (getattr(pconfig, "extra", {}) or {}).get("url")
        or os.getenv("MATTERMOST_URL", "")
    ).rstrip("/")
    token = (getattr(pconfig, "token", None) or os.getenv("MATTERMOST_TOKEN", "")).strip()
    if not base_url or not token:
        return {
            "error": (
                "Mattermost standalone send: MATTERMOST_URL and "
                "MATTERMOST_TOKEN must both be set"
            )
        }

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    upload_headers = {"Authorization": f"Bearer {token}"}

    media_files = media_files or []

    try:
        # Resolve proxy + session kwargs once so a single ClientSession can
        # cover the optional file uploads + final post.
        from gateway.platforms.base import resolve_proxy_url, proxy_kwargs_for_aiohttp
        _proxy = resolve_proxy_url(platform_env_var="MATTERMOST_PROXY")
        _sess_kw, _req_kw = proxy_kwargs_for_aiohttp(_proxy)

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
            **_sess_kw,
        ) as session:
            # 1. Upload media (if any) and collect file_ids.
            file_ids: List[str] = []
            for media in media_files:
                file_path = media.get("path") if isinstance(media, dict) else media
                if not file_path or not os.path.exists(file_path):
                    continue
                form = aiohttp.FormData()
                # Mattermost requires channel_id on file uploads so the
                # server can attribute them.
                form.add_field("channel_id", chat_id)
                with open(file_path, "rb") as fh:
                    form.add_field(
                        "files",
                        fh.read(),
                        filename=os.path.basename(file_path),
                    )
                async with session.post(
                    f"{base_url}/api/v4/files",
                    data=form,
                    headers=upload_headers,
                    **_req_kw,
                ) as upload_resp:
                    if upload_resp.status not in {200, 201}:
                        body = await upload_resp.text()
                        return {
                            "error": (
                                f"Mattermost file upload failed "
                                f"({upload_resp.status}): {body[:400]}"
                            )
                        }
                    upload_data = await upload_resp.json()
                    for info in upload_data.get("file_infos", []):
                        if info.get("id"):
                            file_ids.append(info["id"])

            # 2. Post the message (with thread root + attached file_ids).
            payload: Dict[str, Any] = {
                "channel_id": chat_id,
                "message": message,
            }
            if thread_id:
                payload["root_id"] = thread_id
            if file_ids:
                payload["file_ids"] = file_ids
            async with session.post(
                f"{base_url}/api/v4/posts",
                headers=headers,
                json=payload,
                **_req_kw,
            ) as resp:
                if resp.status not in {200, 201}:
                    body = await resp.text()
                    return {
                        "error": (
                            f"Mattermost API error ({resp.status}): "
                            f"{body[:400]}"
                        )
                    }
                data = await resp.json()
            return {
                "success": True,
                "platform": "mattermost",
                "chat_id": chat_id,
                "message_id": data.get("id"),
            }
    except aiohttp.ClientError as exc:
        return {"error": f"Mattermost send failed (network): {exc}"}
    except Exception as exc:  # noqa: BLE001
        return {"error": f"Mattermost send failed: {exc}"}


# ---------------------------------------------------------------------------
# Interactive setup wizard
# ---------------------------------------------------------------------------


def interactive_setup() -> None:
    """Guide the user through Mattermost bot setup.

    Mirrors Discord/Teams' ``interactive_setup`` shape: lazy-imports CLI
    helpers so the plugin's import surface stays small, prompts for the
    server URL + bot token, captures an allowlist, and offers to set a
    home channel.  Replaces the central
    ``hermes_cli/setup.py::_setup_mattermost`` function this migration
    removes.
    """
    from hermes_cli.config import get_env_value, save_env_value
    from hermes_cli.cli_output import (
        prompt,
        prompt_yes_no,
        print_header,
        print_info,
        print_success,
    )

    print_header("Mattermost")
    existing = get_env_value("MATTERMOST_TOKEN")
    if existing:
        print_info("Mattermost: already configured")
        if not prompt_yes_no("Reconfigure Mattermost?", False):
            return

    print_info("Works with any self-hosted Mattermost instance.")
    print_info("   1. In Mattermost: Integrations → Bot Accounts → Add Bot Account")
    print_info("   2. Copy the bot token")
    print()
    mm_url = prompt("Mattermost server URL (e.g. https://mm.example.com)")
    if mm_url:
        save_env_value("MATTERMOST_URL", mm_url.rstrip("/"))
    token = prompt("Bot token", password=True)
    if not token:
        return
    save_env_value("MATTERMOST_TOKEN", token)
    print_success("Mattermost token saved")

    print()
    print_info("🔒 Security: Restrict who can use your bot")
    print_info("   To find your user ID: click your avatar → Profile")
    print_info("   or use the API: GET /api/v4/users/me")
    print()
    allowed_users = prompt("Allowed user IDs (comma-separated, leave empty for open access)")
    if allowed_users:
        save_env_value("MATTERMOST_ALLOWED_USERS", allowed_users.replace(" ", ""))
        print_success("Mattermost allowlist configured")
    else:
        print_info("⚠️  No allowlist set - anyone who can message the bot can use it!")

    print()
    print_info("📬 Home Channel: where Hermes delivers cron job results and notifications.")
    print_info("   To get a channel ID: click channel name → View Info → copy the ID")
    print_info("   You can also set this later by typing /set-home in a Mattermost channel.")
    home_channel = prompt("Home channel ID (leave empty to set later with /set-home)")
    if home_channel:
        save_env_value("MATTERMOST_HOME_CHANNEL", home_channel)
    print_info("   Open config in your editor:  hermes config edit")


# ---------------------------------------------------------------------------
# YAML → env config bridge (apply_yaml_config_fn, #25443)
# ---------------------------------------------------------------------------


def _apply_yaml_config(yaml_cfg: dict, mattermost_cfg: dict) -> dict | None:
    """Translate ``config.yaml`` ``mattermost:`` keys into env vars.

    Implements the ``apply_yaml_config_fn`` contract (#24836 / #25443).
    Mirrors the legacy ``mattermost_cfg`` block that used to live in
    ``gateway/config.py::load_gateway_config()`` before this migration.

    The MattermostAdapter reads its runtime configuration via
    ``os.getenv()`` for ``MATTERMOST_REQUIRE_MENTION``,
    ``MATTERMOST_FREE_RESPONSE_CHANNELS``, and
    ``MATTERMOST_ALLOWED_CHANNELS``.  Rather than rewrite those call sites
    to read from ``PlatformConfig.extra``, this hook keeps the env-driven
    model and merely owns the YAML→env translation here, next to the
    adapter that consumes it.

    Env vars take precedence over YAML — every assignment is guarded
    by ``not os.getenv(...)`` so an explicit env var survives a config.yaml
    update.  Returns ``None`` because no extras are seeded into
    ``PlatformConfig.extra`` directly (everything flows through env).
    """
    if "require_mention" in mattermost_cfg and not os.getenv("MATTERMOST_REQUIRE_MENTION"):
        os.environ["MATTERMOST_REQUIRE_MENTION"] = str(mattermost_cfg["require_mention"]).lower()
    frc = mattermost_cfg.get("free_response_channels")
    if frc is not None and not os.getenv("MATTERMOST_FREE_RESPONSE_CHANNELS"):
        if isinstance(frc, list):
            frc = ",".join(str(v) for v in frc)
        os.environ["MATTERMOST_FREE_RESPONSE_CHANNELS"] = str(frc)
    # allowed_channels: if set, bot ONLY responds in these channels (whitelist)
    ac = mattermost_cfg.get("allowed_channels")
    if ac is not None and not os.getenv("MATTERMOST_ALLOWED_CHANNELS"):
        if isinstance(ac, list):
            ac = ",".join(str(v) for v in ac)
        os.environ["MATTERMOST_ALLOWED_CHANNELS"] = str(ac)
    return None  # all settings flow through env; nothing to merge into extras


# ---------------------------------------------------------------------------
# is_connected probe
# ---------------------------------------------------------------------------


def _is_connected(config) -> bool:
    """Mattermost is considered connected when BOTH MATTERMOST_TOKEN and
    MATTERMOST_URL are set.

    Looks up via ``hermes_cli.gateway.get_env_value`` at call time (not via
    the plugin's own bound import) so tests that patch
    ``gateway_mod.get_env_value`` can suppress ambient env vars.  Matches
    what the legacy connected-platforms check did before this migration.
    """
    import hermes_cli.gateway as gateway_mod
    return bool(
        (gateway_mod.get_env_value("MATTERMOST_TOKEN") or "").strip()
        and (gateway_mod.get_env_value("MATTERMOST_URL") or "").strip()
    )


# ---------------------------------------------------------------------------
# Plugin registration entry point
# ---------------------------------------------------------------------------


def _build_adapter(config):
    """Factory wrapper that constructs MattermostAdapter from a PlatformConfig."""
    return MattermostAdapter(config)


def register(ctx) -> None:
    """Plugin entry point — called by the Hermes plugin system."""
    ctx.register_platform(
        name="mattermost",
        label="Mattermost",
        adapter_factory=_build_adapter,
        check_fn=check_mattermost_requirements,
        is_connected=_is_connected,
        required_env=["MATTERMOST_URL", "MATTERMOST_TOKEN"],
        install_hint="pip install aiohttp",
        # Interactive setup wizard — replaces the central
        # hermes_cli/setup.py::_setup_mattermost function.
        setup_fn=interactive_setup,
        # YAML→env config bridge — owns the translation of
        # ``config.yaml`` ``mattermost:`` keys (require_mention,
        # free_response_channels, allowed_channels) into ``MATTERMOST_*``
        # env vars that the adapter reads via ``os.getenv()``.  Replaces
        # the hardcoded block that used to live in ``gateway/config.py``.
        # Hook contract: #24836 / #25443.
        apply_yaml_config_fn=_apply_yaml_config,
        # Auth env vars for _is_user_authorized() integration.
        allowed_users_env="MATTERMOST_ALLOWED_USERS",
        allow_all_env="MATTERMOST_ALLOW_ALL_USERS",
        # Cron home-channel delivery.
        cron_deliver_env_var="MATTERMOST_HOME_CHANNEL",
        # Out-of-process cron delivery via Mattermost REST API.  Without
        # this hook, ``deliver=mattermost`` cron jobs fail with "No live
        # adapter" when cron runs separately from the gateway.  Mirrors
        # the Discord / Teams pattern.
        standalone_sender_fn=_standalone_send,
        # Mattermost practical post-length limit (server default is 16383
        # but 4000 is the readable threshold the adapter has used since
        # day one).
        max_message_length=MAX_POST_LENGTH,
        # Display
        emoji="💬",
        allow_update_command=True,
    )
