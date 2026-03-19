"""Coordinator class for Ariston module."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable
from datetime import timedelta
import logging

from ariston.base_device import AristonBaseDevice
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


class DeviceDataUpdateCoordinator(DataUpdateCoordinator):
    """Manages polling for state changes from the device."""

    def __init__(
        self,
        hass: HomeAssistant,
        device: AristonBaseDevice,
        scan_interval_seconds: int,
        coordinator_name: str,
        async_update_state: Callable,
    ) -> None:
        """Initialize the data update coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}-{device.name}-{coordinator_name}",
            update_interval=timedelta(seconds=scan_interval_seconds),
            update_method=async_update_state,
        )
        self.device = device
        self._blocked_until = None
        self._consecutive_failures: int = 0

    async def _async_update_data(self):
        """Wrap update_method with timeout, 429 handling and backoff."""

        # 1. Cooldown attivo (es. dopo 429): skip il poll
        if self._blocked_until and dt_util.now() < self._blocked_until:
            remaining = (self._blocked_until - dt_util.now()).total_seconds()
            _LOGGER.debug(
                "Ariston [%s]: API in cooldown, skip poll (%.0fs rimasti)",
                self.name, remaining,
            )
            return self.data  # restituisce dati precedenti senza chiamare le API

        # 2. Backoff esponenziale dopo 3+ errori consecutivi
        if self._consecutive_failures >= 3:
            backoff = min(60 * (2 ** (self._consecutive_failures - 3)), 600)
            self._blocked_until = dt_util.now() + timedelta(seconds=backoff)
            _LOGGER.warning(
                "Ariston [%s]: %d errori consecutivi, backoff %ds",
                self.name, self._consecutive_failures, backoff,
            )
            return self.data

        try:
            # 3. Timeout globale: la chiamata non può durare più di 20s
            result = await asyncio.wait_for(
                self.update_method(),
                timeout=20.0,
            )
            # Successo: reset contatori
            self._consecutive_failures = 0
            self._blocked_until = None
            return result

        except asyncio.TimeoutError:
            self._consecutive_failures += 1
            _LOGGER.warning(
                "Ariston [%s]: timeout dopo 20s (errori consecutivi: %d)",
                self.name, self._consecutive_failures,
            )
            # Restituisce dati precedenti invece di UpdateFailed
            # → evita che l'entità vada "unavailable" per un semplice timeout
            return self.data

        except Exception as err:
            err_str = str(err)

            # 4. Rate limit 429: legge Retry-After e imposta cooldown
            if "429" in err_str or "blocked" in err_str.lower():
                wait_s = self._parse_retry_after(err_str)
                self._blocked_until = dt_util.now() + timedelta(seconds=wait_s)
                self._consecutive_failures += 1
                _LOGGER.warning(
                    "Ariston [%s]: rate limit 429, cooldown per %ds (errori: %d)",
                    self.name, wait_s, self._consecutive_failures,
                )
                return self.data  # dati precedenti, non UpdateFailed

            # Errore generico: propaga normalmente
            self._consecutive_failures += 1
            _LOGGER.error(
                "Ariston [%s]: errore aggiornamento (errori consecutivi: %d): %s",
                self.name, self._consecutive_failures, err_str,
            )
            raise UpdateFailed(err_str) from err

    @staticmethod
    def _parse_retry_after(err_str: str) -> int:
        """Estrae secondi di attesa dall'errore 429, default 120s."""
        match = re.search(r"blocked for (\d+)", err_str)
        if match:
            return int(match.group(1)) + 15  # +15s di margine
        return 120
