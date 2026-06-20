import os
import sys
os.environ["PYTHONUTF8"] = "1"
sys.stdout.reconfigure(encoding="utf-8")
import socket
import subprocess
import threading
import time
import signal
import webbrowser
import urllib.request
import urllib.error
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

SERVER_FILE = PROJECT_ROOT / 'lab13' / 'dashboard' / 'server.py'
FRONTEND_FILE = PROJECT_ROOT / 'lab13' / 'dashboard' / 'frontend' / 'index.html'
FEATURES_FILE = PROJECT_ROOT / 'lab10' / 'batch_1000_features.csv'
RAW_FILE = PROJECT_ROOT / 'lab09' / 'online_shopping_10_cats.csv'
SERVER_DIR = PROJECT_ROOT / 'lab13' / 'dashboard'

HEALTH_URL = 'http://localhost:8000/api/health'
DASHBOARD_URL = 'http://localhost:8000/'
PORT = 8000
MAX_WAIT_SECONDS = 30
WAIT_INTERVAL = 1


def check_port_available(port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1)
    try:
        result = sock.connect_ex(('127.0.0.1', port))
        sock.close()
        return result != 0
    except OSError:
        return True


def read_output(pipe, prefix):
    try:
        for line in iter(pipe.readline, ''):
            sys.stdout.write(f'{prefix}{line}')
            sys.stdout.flush()
    finally:
        pipe.close()


def main():
    print('=' * 60)
    print('  全链路系统联调启动脚本')
    print('=' * 60)
    print()

    print('[自检] 开始环境自检...')

    if not SERVER_FILE.exists():
        print(f'[错误] 后端服务文件不存在: {SERVER_FILE}')
        sys.exit(1)
    print(f'[通过] 后端服务文件: {SERVER_FILE}')

    if not FRONTEND_FILE.exists():
        print(f'[错误] 前端页面文件不存在: {FRONTEND_FILE}')
        sys.exit(1)
    print(f'[通过] 前端页面文件: {FRONTEND_FILE}')

    has_features = FEATURES_FILE.exists()
    has_raw = RAW_FILE.exists()

    if has_features:
        print(f'[通过] LLM增强数据源: {FEATURES_FILE}')
    if has_raw:
        print(f'[通过] 原始数据源: {RAW_FILE}')

    if not has_features and not has_raw:
        print(f'[警告] 两个数据源均不存在，后端将自行降级处理')
        print(f'        缺失: {FEATURES_FILE}')
        print(f'        缺失: {RAW_FILE}')

    if not check_port_available(PORT):
        print(f'[错误] 端口 {PORT} 已被占用，请先释放端口后重试')
        sys.exit(1)
    print(f'[通过] 端口 {PORT} 可用')

    print('[自检] 环境自检全部通过！')
    print()

    print('[启动] 正在启动Uvicorn服务...')

    try:
        proc = subprocess.Popen(
            [sys.executable, '-m', 'uvicorn', 'server:app',
             '--host', '0.0.0.0', '--port', str(PORT)],
            cwd=str(SERVER_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            text=True,
            bufsize=1,
        )
    except Exception as e:
        print(f'[错误] 启动Uvicorn失败: {e}')
        sys.exit(1)

    output_thread = threading.Thread(
        target=read_output,
        args=(proc.stdout, '[uvicorn] '),
        daemon=True,
    )
    output_thread.start()

    print('[等待] 等待服务就绪...')
    ready = False
    for _ in range(MAX_WAIT_SECONDS):
        if proc.poll() is not None:
            print(f'[错误] Uvicorn进程意外退出，退出码: {proc.returncode}')
            sys.exit(1)

        try:
            req = urllib.request.Request(HEALTH_URL)
            with urllib.request.urlopen(req, timeout=2) as resp:
                if resp.status == 200:
                    ready = True
                    break
        except (urllib.error.URLError, OSError):
            pass

        time.sleep(WAIT_INTERVAL)

    if not ready:
        print(f'[错误] 服务启动超时（{MAX_WAIT_SECONDS}秒），正在终止子进程...')
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        print('[错误] 服务启动超时，已退出')
        sys.exit(1)

    print(f'[就绪] 服务已就绪！监听地址: http://localhost:{PORT}')
    print(f'[就绪] 健康检查通过')
    print()

    print('[浏览器] 正在打开数据看板...')
    webbrowser.open(DASHBOARD_URL)
    print(f'[浏览器] 已打开: {DASHBOARD_URL}')
    print()
    print('-' * 60)
    print('  系统运行中，按 Ctrl+C 停止服务')
    print('-' * 60)

    if hasattr(signal, 'SIGTERM'):
        def sigterm_handler(signum, frame):
            raise KeyboardInterrupt()
        signal.signal(signal.SIGTERM, sigterm_handler)

    try:
        while proc.poll() is None:
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass

    if proc.poll() is None:
        print()
        print('[终止] 正在停止服务...')
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            print('[终止] 子进程未在5秒内退出，强制终止...')
            proc.kill()
            proc.wait()
        time.sleep(0.5)
        print('服务已停止')
    else:
        print()
        print('[提示] Uvicorn进程已自动退出')


if __name__ == '__main__':
    main()
