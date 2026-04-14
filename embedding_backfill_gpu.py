"""
Backfill de embeddings em GPU (script independente).

Objetivo:
- Buscar paginas indexadas com embedding NULL.
- Gerar embeddings com nomic-ai/nomic-embed-text.
- Persistir em batch no Postgres com minimo overhead no banco.

Uso:
    python embedding_backfill_gpu.py

Variaveis de ambiente principais:
- EMBEDDING_HF_MODEL=nomic-ai/nomic-embed-text
- EMBED_FETCH_BATCH_SIZE=4096
- EMBED_GPU_BATCH_SIZE=1024
- EMBED_MAX_EMPTY_PASSES=2
- EMBED_NORMALIZE=true
- EMBED_DB_COUNT_REFRESH_EVERY=25
- EMBED_DEVICE=auto  # auto|cuda|cpu|dml
- EMBED_REQUIRE_GPU=true
"""

from __future__ import annotations

import os
import sys
import time
import logging
from dataclasses import dataclass

from sqlalchemy import create_engine, select, update, func, bindparam
from sqlalchemy.orm import Session

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import DATABASE_URL
from models import Page


def env_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    return int(raw)


EMBEDDING_HF_MODEL = os.getenv("EMBEDDING_HF_MODEL", "nomic-ai/nomic-embed-text")
EMBED_FETCH_BATCH_SIZE = env_int("EMBED_FETCH_BATCH_SIZE", 4096)
EMBED_GPU_BATCH_SIZE = env_int("EMBED_GPU_BATCH_SIZE", 1024)
EMBED_MAX_EMPTY_PASSES = env_int("EMBED_MAX_EMPTY_PASSES", 2)
EMBED_DB_COUNT_REFRESH_EVERY = env_int("EMBED_DB_COUNT_REFRESH_EVERY", 25)
EMBED_NORMALIZE = env_truthy(os.getenv("EMBED_NORMALIZE", "true"))
EMBED_VERBOSE = env_truthy(os.getenv("EMBED_VERBOSE", "false"))
EMBED_DEVICE = os.getenv("EMBED_DEVICE", "auto").strip().lower()
EMBED_REQUIRE_GPU = env_truthy(os.getenv("EMBED_REQUIRE_GPU", "true"))

logging.basicConfig(
    level=logging.DEBUG if EMBED_VERBOSE else logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("embedding.backfill")

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
GRAY = "\033[90m"
RESET = "\033[0m"


def log_info(msg: str) -> None:
    log.info(f"{CYAN}{msg}{RESET}")


def log_ok(msg: str) -> None:
    log.info(f"{GREEN}{msg}{RESET}")


def log_warn(msg: str) -> None:
    log.warning(f"{YELLOW}{msg}{RESET}")


def log_err(msg: str) -> None:
    log.error(f"{RED}{msg}{RESET}")


def log_dim(msg: str) -> None:
    log.debug(f"{GRAY}{msg}{RESET}")


def fmt_seconds(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    rem = int(seconds % 60)
    return f"{minutes}m{rem:02d}s"


def progress_bar(done: int, total: int, started_at: float) -> str:
    now = time.perf_counter()
    elapsed = max(now - started_at, 1e-9)
    pct = (done / total * 100.0) if total > 0 else 100.0
    width = 28
    filled = int((pct / 100.0) * width)
    bar = "#" * filled + "-" * (width - filled)
    rate = done / elapsed
    remaining = max(total - done, 0)
    eta = (remaining / rate) if rate > 0 else 0.0
    return (
        f"[{bar}] {pct:6.2f}% | processadas {done:,}/{total:,} | "
        f"restantes {remaining:,} | {rate:,.1f} pg/s | ETA {fmt_seconds(eta)}"
    )


@dataclass
class BatchStats:
    fetched: int
    embedded: int
    updated: int
    fetch_seconds: float
    embed_seconds: float
    write_seconds: float


def model_candidates(model_name: str) -> list[str]:
    candidates: list[str] = [model_name]

    # Alias amigavel para "latest" do Nomic; fallback real no HF costuma ser v1.5.
    if model_name == "nomic-ai/nomic-embed-text":
        candidates.append("nomic-ai/nomic-embed-text-v1.5")

    # Remove duplicados preservando ordem.
    seen = set()
    ordered = []
    for item in candidates:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def detect_device(torch_mod):
    if EMBED_DEVICE in {"cpu", "cuda"}:
        return EMBED_DEVICE, EMBED_DEVICE

    if EMBED_DEVICE == "dml":
        try:
            import torch_directml
            dml_device = torch_directml.device()
            return dml_device, "directml"
        except Exception as exc:
            raise RuntimeError(
                "EMBED_DEVICE=dml definido, mas torch-directml nao esta disponivel. "
                "Instale: pip install torch-directml"
            ) from exc

    # auto
    if torch_mod.cuda.is_available():
        return "cuda", "cuda"

    if os.name == "nt":
        # Fallback opcional para AMD no Windows quando ROCm nao estiver disponivel no torch.
        try:
            import torch_directml
            dml_device = torch_directml.device()
            return dml_device, "directml"
        except Exception:
            pass

    return "cpu", "cpu"


def load_embedder():
    try:
        import torch
        from sentence_transformers import SentenceTransformer
    except ImportError as exc:
        raise RuntimeError(
            "Dependencias ausentes. Instale no ambiente local de GPU: "
            "pip install sentence-transformers"
        ) from exc

    device, backend_name = detect_device(torch)

    hip_version = getattr(torch.version, "hip", None)
    cuda_version = getattr(torch.version, "cuda", None)
    log_info(
        "Torch runtime: "
        f"cuda_available={torch.cuda.is_available()} | "
        f"torch.version.cuda={cuda_version} | torch.version.hip={hip_version}"
    )

    if backend_name == "cpu":
        if os.name == "nt":
            log_warn(
                "GPU nao detectada no backend atual. Em Windows, ROCm no PyTorch pode nao estar ativo; "
                "tentando CPU. Para AMD local, prefira ambiente ROCm funcional (normalmente Linux/WSL) "
                "ou use DirectML (torch-directml)."
            )
        else:
            log_warn("GPU nao detectada; rodando em CPU (muito mais lento).")

        if EMBED_REQUIRE_GPU:
            raise RuntimeError(
                "EMBED_REQUIRE_GPU=true e nenhum backend GPU foi detectado. "
                "No seu ambiente atual o torch esta CPU-only. "
                "Instale backend GPU (ROCm/DirectML) ou rode com EMBED_REQUIRE_GPU=false para permitir CPU."
            )
    else:
        log_ok(f"Backend de inferencia: {backend_name}")

    model = None
    load_errors: list[str] = []
    for candidate in model_candidates(EMBEDDING_HF_MODEL):
        log_info(f"Carregando modelo: {candidate} em {backend_name} ...")
        try:
            model = SentenceTransformer(candidate, device=device, trust_remote_code=True)
            break
        except Exception as exc:
            load_errors.append(f"{candidate}: {type(exc).__name__}: {exc}")
            continue

    if model is None:
        msg = "\n".join(load_errors[-3:])
        raise RuntimeError(
            "Falha ao carregar modelo de embedding. "
            "Verifique nome do modelo e autenticacao HF (hf auth login). "
            f"Tentativas:\n{msg}"
        )

    # Warmup curto para compilar kernels e reduzir latencia dos primeiros lotes.
    _ = model.encode(
        ["search_document: warmup"],
        batch_size=1,
        show_progress_bar=False,
        normalize_embeddings=EMBED_NORMALIZE,
        convert_to_numpy=True,
    )
    log_ok("Modelo carregado e aquecido.")
    return model, torch


def count_pending(session: Session) -> int:
    return session.execute(
        select(func.count())
        .select_from(Page)
        .where(
            Page.status == "indexed",
            Page.embedding.is_(None),
            Page.summary.is_not(None),
            Page.summary != "",
        )
    ).scalar_one()


def fetch_pending_batch(session: Session, last_id: int) -> list[tuple[int, str]]:
    rows = session.execute(
        select(Page.id, Page.summary)
        .where(
            Page.id > last_id,
            Page.status == "indexed",
            Page.embedding.is_(None),
            Page.summary.is_not(None),
            Page.summary != "",
        )
        .order_by(Page.id.asc())
        .limit(EMBED_FETCH_BATCH_SIZE)
    ).all()
    return [(row[0], row[1]) for row in rows]


def encode_batch(model, torch_mod, texts: list[str], gpu_batch_size: int) -> tuple[list[list[float]], int]:
    current_batch = max(8, gpu_batch_size)

    while True:
        try:
            vectors = model.encode(
                texts,
                batch_size=current_batch,
                show_progress_bar=False,
                normalize_embeddings=EMBED_NORMALIZE,
                convert_to_numpy=True,
            )
            return vectors.tolist(), current_batch
        except RuntimeError as exc:
            message = str(exc).lower()
            if "out of memory" in message and current_batch > 16:
                current_batch = max(16, current_batch // 2)
                log_warn(f"OOM na GPU; reduzindo EMBED_GPU_BATCH_SIZE para {current_batch} e tentando novamente.")
                if torch_mod.cuda.is_available():
                    torch_mod.cuda.empty_cache()
                continue
            raise


def write_embeddings_batch(session: Session, payload: list[dict]) -> int:
    if not payload:
        return 0

    stmt = (
        update(Page)
        .where(Page.id == bindparam("b_id"))
        .where(Page.embedding.is_(None))
        .values(
            embedding=bindparam("b_embedding"),
            language=None,
            updated_at=func.now(),
        )
    )
    result = session.execute(stmt, payload)
    session.commit()
    return int(result.rowcount or 0)


def run() -> None:
    model, torch_mod = load_embedder()
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    with Session(engine) as session:
        total_pending = count_pending(session)

    if total_pending == 0:
        log_ok("Nenhuma pagina pendente com embedding NULL.")
        return

    log_info("Iniciando backfill de embeddings...")
    log_info(
        "Config: "
        f"FETCH={EMBED_FETCH_BATCH_SIZE} | GPU_BATCH={EMBED_GPU_BATCH_SIZE} | "
        f"NORMALIZE={'ON' if EMBED_NORMALIZE else 'OFF'}"
    )
    log_info(f"Pendente inicial: {total_pending:,} paginas")

    started = time.perf_counter()
    last_id = 0
    empty_passes = 0
    batches = 0
    done_updates = 0
    used_gpu_batch = EMBED_GPU_BATCH_SIZE

    while True:
        with Session(engine) as session:
            fetch_t0 = time.perf_counter()
            rows = fetch_pending_batch(session, last_id)
            fetch_seconds = time.perf_counter() - fetch_t0

        if not rows:
            empty_passes += 1
            if empty_passes >= EMBED_MAX_EMPTY_PASSES:
                break
            # Reinicia cursor para capturar eventuais paginas antigas que possam ter voltado para NULL.
            last_id = 0
            continue

        empty_passes = 0
        batches += 1
        last_id = rows[-1][0]

        ids = [row[0] for row in rows]
        docs = [f"search_document: {row[1]}" for row in rows]

        embed_t0 = time.perf_counter()
        vectors, used_gpu_batch = encode_batch(model, torch_mod, docs, used_gpu_batch)
        embed_seconds = time.perf_counter() - embed_t0

        payload = [
            {"b_id": page_id, "b_embedding": vector}
            for page_id, vector in zip(ids, vectors)
        ]

        with Session(engine) as session:
            write_t0 = time.perf_counter()
            updated = write_embeddings_batch(session, payload)
            write_seconds = time.perf_counter() - write_t0

        done_updates += updated

        current_total = max(total_pending, done_updates)
        status_line = progress_bar(done_updates, current_total, started)
        log_info(status_line)

        if EMBED_VERBOSE:
            stats = BatchStats(
                fetched=len(rows),
                embedded=len(vectors),
                updated=updated,
                fetch_seconds=fetch_seconds,
                embed_seconds=embed_seconds,
                write_seconds=write_seconds,
            )
            log_dim(
                "lote "
                f"fetched={stats.fetched} embedded={stats.embedded} updated={stats.updated} | "
                f"fetch={stats.fetch_seconds:.3f}s embed={stats.embed_seconds:.3f}s write={stats.write_seconds:.3f}s"
            )

        if batches % EMBED_DB_COUNT_REFRESH_EVERY == 0:
            with Session(engine) as session:
                db_pending_now = count_pending(session)
            total_pending = done_updates + db_pending_now

    elapsed = time.perf_counter() - started
    pages_per_second = done_updates / elapsed if elapsed > 0 else 0.0

    with Session(engine) as session:
        final_pending = count_pending(session)

    log_ok("Backfill finalizado.")
    log_ok(
        f"Atualizadas: {done_updates:,} paginas | Restantes: {final_pending:,} | "
        f"Tempo: {fmt_seconds(elapsed)} | Throughput medio: {pages_per_second:,.1f} pg/s"
    )


if __name__ == "__main__":
    run()
