from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional

import psutil

from shared.logging_config import logger


def _read_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return float(default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        logger.warning("[ResourceGuard] invalid float env %s=%s; using default %s", name, raw, default)
        return float(default)


def _read_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return int(default)
    try:
        return int(raw)
    except (TypeError, ValueError):
        logger.warning("[ResourceGuard] invalid int env %s=%s; using default %s", name, raw, default)
        return int(default)


@dataclass(frozen=True)
class GuardThresholds:
    cpu_percent: float
    memory_percent: float
    load_percent: Optional[float]
    resume_margin: float

    def resume_cpu(self) -> float:
        return max(0.0, self.cpu_percent - self.resume_margin)

    def resume_mem(self) -> float:
        return max(0.0, self.memory_percent - self.resume_margin)

    def resume_load(self) -> Optional[float]:
        if self.load_percent is None:
            return None
        return max(0.0, self.load_percent - self.resume_margin)


class ResourceGuard:
    """Background resource monitor and throttle coordinator.

    A singleton per-process watches CPU, memory and load average. When metrics exceed
    configurable thresholds, workloads can call `wait_for_capacity` to block until the
    node is healthy again. This is intended to be used by long running training loops
    on cloud/fog/edge nodes to avoid overloading constrained devices.
    """

    def __init__(
        self,
        role: Optional[str] = None,
        *,
        cpu_threshold: float = 85.0,
        memory_threshold: float = 90.0,
        load_threshold: Optional[float] = None,
        resume_margin: float = 5.0,
        check_interval: float = 5.0,
        blocked_sleep: float = 5.0,
        log_interval: float = 30.0,
    ) -> None:
        self._role = role
        self._thresholds = GuardThresholds(
            cpu_percent=cpu_threshold,
            memory_percent=memory_threshold,
            load_percent=load_threshold,
            resume_margin=resume_margin,
        )
        self._check_interval = check_interval
        self._blocked_sleep = blocked_sleep
        self._log_interval = log_interval
        self._cpu_count = max(1, psutil.cpu_count() or 1)

        self._lock = threading.Lock()
        self._metrics: Dict[str, float] = {}
        self._overloaded = False
        self._overloaded_since: Optional[float] = None
        self._last_log_ts: float = 0.0
        self._last_resume_log_ts: float = 0.0

        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._monitor_loop, name="resource-guard", daemon=True)
        # Warm up psutil cpu_percent measurement to avoid initial spikes
        try:
            psutil.cpu_percent(interval=None)
        except Exception:
            pass
        self._thread.start()
        logger.info(
            "[ResourceGuard] started (role=%s thresholds=%s check_interval=%ss)",
            self._role,
            self._thresholds,
            self._check_interval,
        )

    def set_role(self, role: str) -> None:
        if not role:
            return
        with self._lock:
            if not self._role:
                self._role = role

    # --- public API ---

    def wait_for_capacity(self, reason: str, *, log_prefix: Optional[str] = None) -> None:
        """Block the caller until resource usage drops below thresholds."""
        prefix = log_prefix or "[ResourceGuard]"
        waited = False
        while self.is_overloaded():
            waited = True
            snapshot = self.snapshot()
            now = time.time()
            if now - self._last_log_ts >= self._log_interval:
                self._last_log_ts = now
                logger.warning(
                    "%s %s pausing workload due to high resource usage: cpu=%.1f%% mem=%.1f%% load=%.1f%% thresholds=%s",
                    prefix,
                    self._role or "node",
                    snapshot.get("cpu", float("nan")),
                    snapshot.get("memory", float("nan")),
                    snapshot.get("load", float("nan")),
                    self._thresholds,
                )
            time.sleep(self._blocked_sleep)

        if waited:
            now = time.time()
            if now - self._last_resume_log_ts >= self._log_interval:
                self._last_resume_log_ts = now
                snapshot = self.snapshot()
                logger.info(
                    "%s %s resuming workload '%s'. Current usage: cpu=%.1f%% mem=%.1f%% load=%.1f%%",
                    prefix,
                    self._role or "node",
                    reason,
                    snapshot.get("cpu", float("nan")),
                    snapshot.get("memory", float("nan")),
                    snapshot.get("load", float("nan")),
                )

    def is_overloaded(self) -> bool:
        with self._lock:
            return self._overloaded

    def snapshot(self) -> Dict[str, float]:
        with self._lock:
            return dict(self._metrics)

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=2 * self._check_interval)

    # --- monitoring loop ---

    def _monitor_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._sample_once()
            except Exception as exc:
                logger.exception("[ResourceGuard] sampling failed: %s", exc)
            finally:
                self._stop_event.wait(self._check_interval)

    def _sample_once(self) -> None:
        cpu_pct = float(psutil.cpu_percent(interval=None))
        mem = psutil.virtual_memory()
        mem_pct = float(mem.percent)

        load_pct: Optional[float]
        load_pct = None
        if self._thresholds.load_percent is not None:
            try:
                load1 = os.getloadavg()[0]
                load_pct = float(load1 / self._cpu_count * 100.0)
            except OSError:
                load_pct = None

        with self._lock:
            self._metrics = {
                "cpu": cpu_pct,
                "memory": mem_pct,
            }
            if load_pct is not None:
                self._metrics["load"] = load_pct

            overloaded = self._compute_overloaded(cpu_pct, mem_pct, load_pct)
            if overloaded:
                if not self._overloaded:
                    self._overloaded_since = time.time()
                self._overloaded = True
            else:
                self._overloaded = False
                self._overloaded_since = None

    def _compute_overloaded(self, cpu_pct: float, mem_pct: float, load_pct: Optional[float]) -> bool:
        thr = self._thresholds
        over_cpu = cpu_pct >= thr.cpu_percent
        over_mem = mem_pct >= thr.memory_percent
        over_load = load_pct is not None and thr.load_percent is not None and load_pct >= thr.load_percent

        if over_cpu or over_mem or over_load:
            return True

        if not self._overloaded:
            return False

        # We are currently overloaded; ensure we drop below resume thresholds before clearing.
        resume_cpu = thr.resume_cpu()
        resume_mem = thr.resume_mem()
        resume_load = thr.resume_load()

        below_cpu = cpu_pct <= resume_cpu
        below_mem = mem_pct <= resume_mem
        below_load = True
        if resume_load is not None and load_pct is not None:
            below_load = load_pct <= resume_load

        return not (below_cpu and below_mem and below_load)


_singleton_lock = threading.Lock()
_singleton_guard: Optional[ResourceGuard] = None


def get_resource_guard(role: Optional[str] = None) -> ResourceGuard:
    """Return the process-wide ResourceGuard singleton."""
    global _singleton_guard
    with _singleton_lock:
        if _singleton_guard is None:
            cpu_thr = _read_float_env("NODE_GUARD_CPU_PCT", 85.0)
            mem_thr = _read_float_env("NODE_GUARD_MEM_PCT", 90.0)
            load_thr_env = _read_float_env("NODE_GUARD_LOAD_PCT", -1.0)
            load_thr = load_thr_env if load_thr_env >= 0 else None
            resume_margin = _read_float_env("NODE_GUARD_RESUME_MARGIN", 5.0)
            check_interval = _read_float_env("NODE_GUARD_CHECK_INTERVAL", 5.0)
            blocked_sleep = _read_float_env("NODE_GUARD_BLOCK_SLEEP", 5.0)
            log_interval = _read_float_env("NODE_GUARD_LOG_INTERVAL", 30.0)

            _singleton_guard = ResourceGuard(
                role=role,
                cpu_threshold=cpu_thr,
                memory_threshold=mem_thr,
                load_threshold=load_thr,
                resume_margin=resume_margin,
                check_interval=check_interval,
                blocked_sleep=blocked_sleep,
                log_interval=log_interval,
            )
        elif role:
            _singleton_guard.set_role(role)
        return _singleton_guard

