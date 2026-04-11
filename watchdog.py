import logging
import threading
import time
import os
from time import monotonic

log = logging.getLogger("crawler.watchdog")

class Heartbeat:
    """Watchdog leve com mínimo overhead de memória. Usa monotonic() ao invés de datetime."""
    
    def __init__(self, timeout_minutes=5):
        self.timeout_seconds = timeout_minutes * 60
        self.last_beat = monotonic()
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = None
        self.started = False

    def update(self):
        """Update heartbeat (chamado pelo crawler a cada iteração)."""
        with self.lock:
            self.last_beat = monotonic()

    def _watchdog_loop(self):
        """Loop de monitoramento com mínimo impacto de memória."""
        check_interval = 30
        timeout_val = self.timeout_seconds
        
        while not self.stop_event.is_set():
            time.sleep(check_interval)
            
            # Lê última atividade com lock mínimo
            with self.lock:
                last = self.last_beat
            
            # Calcula inatividade fora do lock
            inactivity = monotonic() - last
            
            # Se travou, sai imediatamente (sem logging que acumula)
            if inactivity > timeout_val:
                # Apenas 1 log antes de matar
                log.error(f"⚠️  WATCHDOG: Inatividade {inactivity:.0f}s > {timeout_val}s. Encerrando.")
                os._exit(1)

    def start(self):
        """Inicia watchdog (idempotente)."""
        if self.timeout_seconds <= 0:
            log.info("Watchdog: desabilitado (timeout <= 0).")
            return
        
        # Double-checked locking para evitar múltiplas threads
        if self.started:
            return
        
        with self.lock:
            if self.started:  # Recheck após lock
                return
            
            self.thread = threading.Thread(target=self._watchdog_loop, daemon=True, name="Watchdog")
            self.thread.start()
            self.started = True
            
        log.info(f"✓ Watchdog: timeout={self.timeout_seconds}s (detecta travamento)")

    def stop(self):
        """Para o watchdog de forma limpa."""
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=1)

_global_heartbeat = None
_global_lock = threading.Lock()

def get_heartbeat(timeout_minutes=5):
    """Singleton thread-safe do heartbeat watchdog."""
    global _global_heartbeat
    
    if _global_heartbeat is None:
        with _global_lock:
            if _global_heartbeat is None:
                _global_heartbeat = Heartbeat(timeout_minutes)
    
    return _global_heartbeat
