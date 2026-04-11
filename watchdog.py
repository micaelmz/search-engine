import logging
import threading
import time
import os
import signal
from datetime import datetime, timezone

log = logging.getLogger("crawler.watchdog")

class Heartbeat:
    def __init__(self, timeout_minutes=5):
        self.timeout_seconds = timeout_minutes * 60
        self.last_beat = datetime.now(timezone.utc)
        self.sentinel = threading.Event()
        self.thread = None

    def update(self):
        self.last_beat = datetime.now(timezone.utc)

    def _loop(self):
        check_interval = 30
        while not self.sentinel.is_set():
            time.sleep(check_interval)
            now = datetime.now(timezone.utc)
            inactivity = (now - self.last_beat).total_seconds()
            
            if inactivity > self.timeout_seconds:
                log.error(f"🚨 WATCHDOG TRIGGERED: Inatividade de {inactivity:.0f}s (Limite: {self.timeout_seconds}s)")
                log.error("Forçando graceful shutdown via SIGINT para o Dokploy reiniciar o processo...")
                # Envia SIGINT para o próprio processo para iniciar graceful shutdown no main thread
                os.kill(os.getpid(), signal.SIGINT)
                break

    def start(self):
        if self.timeout_seconds <= 0:
            log.info("Watchdog desabilitado (timeout <= 0).")
            return
            
        if self.thread is None or not self.thread.is_alive():
            self.thread = threading.Thread(target=self._loop, daemon=True, name="Watchdog")
            self.thread.start()
            log.info(f"🛡️ Watchdog iniciado com timeout de {self.timeout_seconds}s.")

    def stop(self):
        self.sentinel.set()

_global_heartbeat = None

def get_heartbeat(timeout_minutes=5):
    global _global_heartbeat
    if _global_heartbeat is None:
        _global_heartbeat = Heartbeat(timeout_minutes)
    return _global_heartbeat
