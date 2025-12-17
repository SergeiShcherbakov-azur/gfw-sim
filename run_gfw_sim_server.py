# run_gfw_sim_server.py
import argparse
import logging
import time
import uvicorn
from pathlib import Path

# Импортируем функции для создания и сохранения снапшота
from gfw_sim.snapshot.collector import collect_k8s_snapshot
from gfw_sim.snapshot.io import save_snapshot_to_file

# Настраиваем логирование для лаунчера
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("launcher")

def capture_new_snapshot():
    """
    Создает новый снапшот и сохраняет его в папку snapshots/.
    """
    log.info("Capturing new snapshot on startup...")
    try:
        # 1. Сбор данных (подключается к K8s и VictoriaMetrics)
        # Убедитесь, что у вас есть доступ к K8s контексту и VPN к VictoriaMetrics
        snap = collect_k8s_snapshot()
        
        # 2. Формирование пути
        filename = f"k8s-{int(time.time())}.json"
        
        root_dir = Path(__file__).resolve().parent
        snapshots_dir = root_dir / "snapshots"
        snapshots_dir.mkdir(parents=True, exist_ok=True)
        
        file_path = snapshots_dir / filename
        
        # 3. Сохранение
        save_snapshot_to_file(snap, file_path)
        log.info(f"Snapshot successfully saved to: {file_path}")
        
    except Exception as e:
        log.error(f"Failed to capture snapshot: {e}")
        # Не прерываем выполнение, чтобы сервер мог запуститься даже если сбор упал

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GFW Sim Server Launcher")
    
    # Флаг для создания снапшота при старте
    parser.add_argument(
        "--capture", 
        action="store_true", 
        help="Capture a new K8s snapshot immediately upon startup"
    )
    
    # Стандартные настройки uvicorn
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=8000, help="Bind port")
    parser.add_argument("--no-reload", action="store_true", help="Disable auto-reload")

    args = parser.parse_args()

    # Если передан флаг --capture, сначала снимаем снапшот
    if args.capture:
        capture_new_snapshot()

    # Запускаем веб-сервер
    # reload=True по умолчанию, если не передан флаг --no-reload
    uvicorn.run(
        "gfw_sim.api.server:app",
        host=args.host,
        port=args.port,
        reload=not args.no_reload,
    )