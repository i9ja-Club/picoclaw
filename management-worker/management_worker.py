"""Picoclaw Management Layer observation daemon.

Every cycle it builds app telemetry, detects consented churn/stagnation signals,
creates an idempotent audit job, and asynchronously delegates copy + delivery to
Odysseus. It never generates or sends marketing content itself.
"""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import logging
import os
import re
import signal
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s │ %(levelname)-7s │ picoclaw-management │ %(message)s",
)
logger = logging.getLogger("picoclaw-management")

INACTIVE_STATUSES = {"inactive", "canceled", "cancelled", "expired", "past_due", "unpaid", "paused"}
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def env(*names: str, default: str = "") -> str:
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return default


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def metadata_allows_email(metadata: Any, require_explicit: bool) -> bool:
    data = metadata if isinstance(metadata, dict) else {}
    if data.get("email_opt_out") is True or data.get("marketing_opt_out") is True:
        return False
    consent = data.get("email_marketing_consent", data.get("marketing_consent"))
    return consent is True if require_explicit else consent is not False


@dataclass(frozen=True)
class Config:
    supabase_url: str = env("SUPABASE_PROJECT_HOST_URL", "NEXT_PUBLIC_SUPABASE_URL").rstrip("/")
    supabase_key: str = env("SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_SECRET_KEY")
    odysseus_url: str = env("ODYSSEUS_MANAGEMENT_URL", default="http://odysseus-telegram-bridge:8088").rstrip("/")
    odysseus_token: str = env("ODYSSEUS_MANAGEMENT_TOKEN", "ODYSSEUS_API_TOKEN", "TELEGRAM_ODYSSEUS_TOKEN")
    interval_seconds: int = int(env("MANAGEMENT_LOOP_INTERVAL_SECONDS", default="300"))
    page_limit: int = int(env("MANAGEMENT_SCAN_LIMIT", default="1000"))
    default_stagnation_days: int = int(env("MANAGEMENT_STAGNATION_DAYS", default="14"))
    default_cooldown_days: int = int(env("MANAGEMENT_COOLDOWN_DAYS", default="30"))
    default_max_per_cycle: int = int(env("MANAGEMENT_MAX_PER_CYCLE", default="25"))
    lock_file: str = env("MANAGEMENT_LOCK_FILE", default="/tmp/minaris-management-worker.lock")

    def validate(self) -> None:
        missing = []
        if not self.supabase_url:
            missing.append("SUPABASE_PROJECT_HOST_URL")
        if not self.supabase_key:
            missing.append("SUPABASE_SERVICE_ROLE_KEY")
        if not self.odysseus_token:
            missing.append("ODYSSEUS_MANAGEMENT_TOKEN or ODYSSEUS_API_TOKEN")
        if missing:
            raise RuntimeError(f"Missing required configuration: {', '.join(missing)}")


class SupabaseRest:
    def __init__(self, config: Config):
        self.base = f"{config.supabase_url}/rest/v1"
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(25, connect=10),
            headers={
                "apikey": config.supabase_key,
                "Authorization": f"Bearer {config.supabase_key}",
                "Content-Type": "application/json",
            },
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def select(self, table: str, params: dict[str, str]) -> list[dict[str, Any]]:
        response = await self.client.get(f"{self.base}/{table}", params=params)
        response.raise_for_status()
        return response.json()

    async def select_all(
        self,
        table: str,
        params: dict[str, str],
        *,
        page_size: int,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            page_params = {**params, "limit": str(page_size), "offset": str(offset)}
            page = await self.select(table, page_params)
            rows.extend(page)
            if len(page) < page_size:
                return rows
            offset += page_size

    async def insert_unique(self, table: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        response = await self.client.post(
            f"{self.base}/{table}",
            params={"on_conflict": "fingerprint"},
            headers={"Prefer": "resolution=ignore-duplicates,return=representation"},
            json=payload,
        )
        response.raise_for_status()
        rows = response.json()
        return rows[0] if rows else None

    async def insert(self, table: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = await self.client.post(
            f"{self.base}/{table}",
            headers={"Prefer": "return=representation"},
            json=payload,
        )
        response.raise_for_status()
        rows = response.json()
        if not rows:
            raise RuntimeError(f"Supabase returned no row after inserting into {table}")
        return rows[0]

    async def patch(self, table: str, filters: dict[str, str], payload: dict[str, Any]) -> None:
        response = await self.client.patch(
            f"{self.base}/{table}",
            params=filters,
            headers={"Prefer": "return=minimal"},
            json=payload,
        )
        response.raise_for_status()


class ManagementWorker:
    def __init__(self, config: Config):
        self.config = config
        self.db = SupabaseRest(config)
        headers = {"Authorization": f"Bearer {config.odysseus_token}"}
        self.odysseus = httpx.AsyncClient(
            base_url=config.odysseus_url,
            timeout=httpx.Timeout(15, connect=5),
            headers=headers,
        )
        self._cycle_lock = asyncio.Lock()

    async def close(self) -> None:
        await self.db.close()
        await self.odysseus.aclose()

    async def fetch_sources(
        self,
    ) -> tuple[
        list[dict[str, Any]],
        list[dict[str, Any]],
        dict[str, dict[str, Any]],
        dict[str, str],
    ]:
        progress_task = self.db.select_all(
            "user_progress",
            {
                "select": "id,user_id,email,app_id,app_name,module_id,lesson_id,pillar,progress_percent,engagement_score,last_activity_at,metadata,updated_at",
                "order": "updated_at.desc,id.desc",
            },
            page_size=self.config.page_limit,
        )
        subscriptions_task = self.db.select_all(
            "user_subscriptions",
            {
                "select": "id,user_id,email,app_id,app_name,product_id,status,access_locked,expires_at,metadata,updated_at",
                "order": "updated_at.desc,id.desc",
            },
            page_size=self.config.page_limit,
        )
        states_task = self.db.select(
            "minaris_world_state",
            {"select": "app_id,agent_rules,active_theme"},
        )
        profiles_task = self.db.select_all(
            "profiles",
            {"select": "id,email", "order": "id.asc"},
            page_size=self.config.page_limit,
        )
        progress, subscriptions, states, profiles = await asyncio.gather(
            progress_task, subscriptions_task, states_task, profiles_task
        )
        profile_ids: dict[str, str] = {}
        for profile in profiles:
            profile_id = str(profile.get("id") or "")
            email = str(profile.get("email") or "").strip().lower()
            if profile_id:
                profile_ids[profile_id] = profile_id
            if email and profile_id:
                profile_ids[email] = profile_id
        return progress, subscriptions, {row["app_id"]: row for row in states}, profile_ids

    async def recent_contact_keys(self, earliest: datetime) -> set[tuple[str, str]]:
        rows = await self.db.select_all(
            "minaris_reactivation_jobs",
            {
                "select": "app_id,user_id,recipient_email",
                "status": "eq.SENT",
                "sent_at": f"gte.{iso(earliest)}",
                "order": "sent_at.desc,id.desc",
            },
            page_size=self.config.page_limit,
        )
        return {
            (str(row.get("app_id") or ""), str(row.get("user_id") or row.get("recipient_email") or "").lower())
            for row in rows
        }

    async def update_world_metrics(
        self,
        progress: list[dict[str, Any]],
        subscriptions: list[dict[str, Any]],
        states: dict[str, dict[str, Any]],
    ) -> None:
        observed_at = utcnow()
        apps = set(states)
        apps.update(str(row.get("app_id") or "") for row in progress + subscriptions)
        for app_id in sorted(app for app in apps if app):
            active_users = {
                str(row.get("user_id") or row.get("email") or "")
                for row in progress
                if row.get("app_id") == app_id
                and (parse_timestamp(row.get("last_activity_at")) or datetime.min.replace(tzinfo=timezone.utc))
                >= observed_at - timedelta(days=30)
            }
            app_subscriptions = [row for row in subscriptions if row.get("app_id") == app_id]
            conversions = sum(
                1 for row in app_subscriptions if str(row.get("status") or "").lower() in {"active", "trialing"}
            )
            churn = sum(
                1 for row in app_subscriptions if str(row.get("status") or "").lower() in INACTIVE_STATUSES
            )
            metrics = {
                "active_users": len(active_users),
                "conversions": conversions,
                "churn": churn,
                "progress_rows_scanned": sum(1 for row in progress if row.get("app_id") == app_id),
                "subscription_rows_scanned": len(app_subscriptions),
                "observed_at": iso(observed_at),
            }
            await self.db.patch(
                "minaris_world_state",
                {"app_id": f"eq.{app_id}"},
                {"metrics_snapshot": metrics},
            )

    def candidates(
        self,
        progress: list[dict[str, Any]],
        subscriptions: list[dict[str, Any]],
        states: dict[str, dict[str, Any]],
        recent: set[tuple[str, str]],
        profile_ids: dict[str, str],
    ) -> list[dict[str, Any]]:
        now = utcnow()
        found: dict[tuple[str, str], dict[str, Any]] = {}

        def rules_for(app_id: str) -> dict[str, Any]:
            rules = states.get(app_id, {}).get("agent_rules") or {}
            return rules.get("reactivation") if isinstance(rules.get("reactivation"), dict) else {}

        for row in subscriptions:
            app_id = str(row.get("app_id") or "")
            rules = rules_for(app_id)
            if not app_id or rules.get("enabled") is not True or rules.get("human_approved") is not True:
                continue
            status = str(row.get("status") or "").lower()
            metadata = row.get("metadata")
            if status not in INACTIVE_STATUSES or not metadata_allows_email(
                metadata, bool(rules.get("require_explicit_consent", True))
            ):
                continue
            email = str(row.get("email") or "").strip().lower()
            actor = str(row.get("user_id") or email)
            key = (app_id, actor.lower())
            if not EMAIL_PATTERN.match(email) or key in recent:
                continue
            found[key] = {
                "actor_id": profile_ids.get(str(row.get("user_id") or "")) or profile_ids.get(email),
                "app_id": app_id,
                "user_id": row.get("user_id"),
                "recipient_email": email,
                "recipient_name": (metadata or {}).get("full_name", "") if isinstance(metadata, dict) else "",
                "trigger_type": "SUBSCRIPTION_INACTIVE",
                "trigger_reason": f"subscription status changed to {status}",
                "event_version": row.get("updated_at") or status,
                "context_snapshot": {
                    "app_name": row.get("app_name"),
                    "product_id": row.get("product_id"),
                    "subscription_status": status,
                    "access_locked": row.get("access_locked"),
                    "expires_at": row.get("expires_at"),
                },
                "max_per_cycle": int(rules.get("max_per_cycle", self.config.default_max_per_cycle)),
            }

        for row in progress:
            app_id = str(row.get("app_id") or "")
            rules = rules_for(app_id)
            if not app_id or rules.get("enabled") is not True or rules.get("human_approved") is not True:
                continue
            metadata = row.get("metadata")
            if not metadata_allows_email(metadata, bool(rules.get("require_explicit_consent", True))):
                continue
            last_activity = parse_timestamp(row.get("last_activity_at"))
            progress_percent = float(row.get("progress_percent") or 0)
            stagnation_days = int(rules.get("stagnation_days", self.config.default_stagnation_days))
            if not last_activity or progress_percent >= 100 or last_activity > now - timedelta(days=stagnation_days):
                continue
            email = str(row.get("email") or "").strip().lower()
            actor = str(row.get("user_id") or email)
            key = (app_id, actor.lower())
            if key in found or key in recent or not EMAIL_PATTERN.match(email):
                continue
            found[key] = {
                "actor_id": profile_ids.get(str(row.get("user_id") or "")) or profile_ids.get(email),
                "app_id": app_id,
                "user_id": row.get("user_id"),
                "recipient_email": email,
                "recipient_name": (metadata or {}).get("full_name", "") if isinstance(metadata, dict) else "",
                "trigger_type": "PROGRESS_STAGNATED",
                "trigger_reason": f"progress inactive for at least {stagnation_days} days",
                "event_version": row.get("last_activity_at") or row.get("updated_at"),
                "context_snapshot": {
                    "app_name": row.get("app_name"),
                    "module_id": row.get("module_id"),
                    "lesson_id": row.get("lesson_id"),
                    "pillar": row.get("pillar"),
                    "progress_percent": progress_percent,
                    "engagement_score": row.get("engagement_score"),
                    "last_activity_at": row.get("last_activity_at"),
                },
                "max_per_cycle": int(rules.get("max_per_cycle", self.config.default_max_per_cycle)),
            }
        return list(found.values())

    async def create_hermes_proposal(self, candidate: dict[str, Any]) -> bool:
        actor_id = str(candidate.get("actor_id") or "")
        if not actor_id:
            logger.warning(
                "Skipping proposal without profiles actor app=%s email_hash=%s",
                candidate.get("app_id"),
                hashlib.sha256(str(candidate.get("recipient_email") or "").encode()).hexdigest()[:12],
            )
            return False
        trigger = str(candidate["trigger_type"])
        cutoff = iso(utcnow() - timedelta(hours=72))
        existing = await self.db.select(
            "hermes_audits",
            {
                "select": "id,status,created_at",
                "actor_id": f"eq.{actor_id}",
                "trigger_reason": f"eq.{trigger}",
                "status": "in.(PENDING_APPROVAL,EXECUTED)",
                "created_at": f"gte.{cutoff}",
                "order": "created_at.desc",
                "limit": "1",
            },
        )
        if existing:
            logger.info(
                "Hermes duplicate blocked actor=%s trigger=%s audit=%s status=%s",
                actor_id,
                trigger,
                existing[0]["id"],
                existing[0]["status"],
            )
            return False
        preview_response = await self.odysseus.post(
            "/management/reactivations/preview",
            json={
                "app_id": candidate["app_id"],
                "context_snapshot": {
                    **candidate["context_snapshot"],
                    "trigger_type": trigger,
                    "trigger_reason": candidate["trigger_reason"],
                    "recipient_name": candidate.get("recipient_name") or "",
                },
            },
        )
        preview_response.raise_for_status()
        preview_payload = preview_response.json()
        if not preview_payload.get("success") or not isinstance(preview_payload.get("preview"), dict):
            raise RuntimeError("Odysseus returned an invalid Hermes proposal preview")
        proposal = await self.db.insert(
            "hermes_audits",
            {
                "actor_id": actor_id,
                "trigger_reason": trigger,
                "proposal_payload": {
                    "app_id": candidate["app_id"],
                    "trigger_detail": candidate["trigger_reason"],
                    "context_snapshot": candidate["context_snapshot"],
                    "recommended_action": "GENERATE_LUMEN_REACTIVATION",
                    "channel": "email",
                    "requires_boss_approval": True,
                    "copy_preview": preview_payload["preview"],
                    "recipient_email_hash": hashlib.sha256(
                        str(candidate["recipient_email"]).encode()
                    ).hexdigest(),
                },
                "status": "PENDING_APPROVAL",
            },
        )
        logger.info(
            "Hermes proposal persisted audit=%s actor=%s trigger=%s status=PENDING_APPROVAL",
            proposal["id"],
            actor_id,
            trigger,
        )
        return True

    async def enqueue_and_dispatch(self, candidate: dict[str, Any]) -> bool:
        fingerprint_source = "|".join(
            str(candidate.get(key) or "")
            for key in ("app_id", "user_id", "recipient_email", "trigger_type", "event_version")
        )
        fingerprint = hashlib.sha256(fingerprint_source.encode()).hexdigest()
        payload = {
            key: candidate.get(key)
            for key in (
                "app_id",
                "user_id",
                "recipient_email",
                "recipient_name",
                "trigger_type",
                "trigger_reason",
                "context_snapshot",
            )
        }
        payload["fingerprint"] = fingerprint
        row = await self.db.insert_unique("minaris_reactivation_jobs", payload)
        if not row:
            return False
        try:
            response = await self.odysseus.post(
                "/management/reactivations",
                json={"job_id": row["id"]},
            )
            response.raise_for_status()
            logger.info(
                "Delegated job=%s app=%s trigger=%s",
                row["id"],
                candidate["app_id"],
                candidate["trigger_type"],
            )
            return True
        except httpx.HTTPError as exc:
            await self.db.patch(
                "minaris_reactivation_jobs",
                {"id": f"eq.{row['id']}"},
                {
                    "status": "RETRY",
                    "last_error": f"Odysseus delegation failed: {str(exc)[:500]}",
                    "next_retry_at": iso(utcnow() + timedelta(minutes=5)),
                },
            )
            logger.warning("Delegation failed job=%s error=%s", row["id"], exc)
            return False

    async def dispatch_due_jobs(self) -> int:
        stale_before = iso(utcnow() - timedelta(minutes=15))
        await self.db.patch(
            "minaris_reactivation_jobs",
            {
                "status": "eq.PROCESSING",
                "processing_started_at": f"lt.{stale_before}",
            },
            {
                "status": "RETRY",
                "last_error": "Recovered stale processing lease",
                "next_retry_at": iso(utcnow()),
            },
        )
        pending_task = self.db.select(
            "minaris_reactivation_jobs",
            {
                "select": "id",
                "status": "eq.PENDING",
                "order": "requested_at.asc",
                "limit": str(self.config.default_max_per_cycle),
            },
        )
        retry_task = self.db.select(
            "minaris_reactivation_jobs",
            {
                "select": "id",
                "status": "eq.RETRY",
                "next_retry_at": f"lte.{iso(utcnow())}",
                "order": "next_retry_at.asc",
                "limit": str(self.config.default_max_per_cycle),
            },
        )
        pending, retries = await asyncio.gather(pending_task, retry_task)
        rows = (pending + retries)[: self.config.default_max_per_cycle]
        accepted = 0
        for row in rows:
            try:
                response = await self.odysseus.post(
                    "/management/reactivations",
                    json={"job_id": row["id"]},
                )
                response.raise_for_status()
                accepted += 1
            except httpx.HTTPError as exc:
                logger.warning("Retry delegation failed job=%s error=%s", row["id"], exc)
        return accepted

    async def run_cycle(self) -> None:
        if self._cycle_lock.locked():
            logger.warning("Skipping overlapping observation cycle")
            return
        async with self._cycle_lock:
            recovered = await self.dispatch_due_jobs()
            progress, subscriptions, states, profile_ids = await self.fetch_sources()
            await self.update_world_metrics(progress, subscriptions, states)
            max_cooldown = max(
                [
                    int(((state.get("agent_rules") or {}).get("reactivation") or {}).get(
                        "cooldown_days", self.config.default_cooldown_days
                    ))
                    for state in states.values()
                ]
                or [self.config.default_cooldown_days]
            )
            recent = await self.recent_contact_keys(utcnow() - timedelta(days=max_cooldown))
            candidates = self.candidates(progress, subscriptions, states, recent, profile_ids)
            app_counts: dict[str, int] = {}
            proposed = 0
            for candidate in candidates:
                app_id = candidate["app_id"]
                count = app_counts.get(app_id, 0)
                if count >= candidate["max_per_cycle"]:
                    continue
                app_counts[app_id] = count + 1
                if await self.create_hermes_proposal(candidate):
                    proposed += 1
            logger.info(
                "Cycle complete progress=%d subscriptions=%d candidates=%d proposed=%d recovered=%d",
                len(progress),
                len(subscriptions),
                len(candidates),
                proposed,
                recovered,
            )


async def main() -> None:
    config = Config()
    config.validate()
    Path(config.lock_file).parent.mkdir(parents=True, exist_ok=True)
    lock_handle = open(config.lock_file, "w", encoding="utf-8")
    try:
        fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        raise RuntimeError("Another Picoclaw management worker already owns the process lock") from exc

    worker = ManagementWorker(config)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    logger.info("Management observation loop started interval=%ss", config.interval_seconds)
    try:
        while not stop.is_set():
            try:
                await worker.run_cycle()
            except Exception:
                logger.exception("Observation cycle failed")
            try:
                await asyncio.wait_for(stop.wait(), timeout=config.interval_seconds)
            except asyncio.TimeoutError:
                pass
    finally:
        await worker.close()
        fcntl.flock(lock_handle, fcntl.LOCK_UN)
        lock_handle.close()


if __name__ == "__main__":
    asyncio.run(main())
