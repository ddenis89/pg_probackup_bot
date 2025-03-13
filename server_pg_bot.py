import os
import json
import socket
import threading
import subprocess
import logging
from logging.handlers import RotatingFileHandler
from queue import Queue
from datetime import datetime
import uuid
import time
import sqlite3

# Загружаем настройки из файла конфигурации.
CONFIG_FILE = os.environ.get('PROBACKUP_CONFIG', 'config.json')
with open(CONFIG_FILE, 'r') as f:
    config = json.load(f)

# Основные настройки
HOST = config['host']
PORT = config['port']
ARCHIVE_HOST = config['archive_host']
MESSAGE_HOST = config['message_host']
LOG_DIR = config['log_dir']
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, config['log_file'])
DB_FILE = config['db_file']
LOG_MAX_BYTES = config['log_max_bytes']
LOG_BACKUP_COUNT = config['log_backup_count']
SSH_KEY = config['ssh_key']
SSH_PORT = config['ssh_port']
SSH_USER = config['ssh_user']
DB_HOST_MAPPING = config['db_host_mapping']

queue = Queue()

# Настройка логгирования
logger = logging.getLogger('probackup_restore_service')
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT)
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def init_db():
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # Добавлено поле recovery_time
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY,
            db_instance TEXT,
            db_host TEXT,
            backup_id TEXT,
            recovery_time TEXT,
            status TEXT,
            created_at TEXT,
            started_at TEXT,
            completed_at TEXT,
            canceled INTEGER DEFAULT 0
        )
    ''')
    conn.commit()
    conn.close()

def cleanup_tasks():
    """
    Оставляем в БД только 30 самых свежих задач (по created_at).
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        DELETE FROM tasks
        WHERE id NOT IN (
            SELECT id FROM tasks ORDER BY created_at DESC LIMIT 30
        )
    ''')
    conn.commit()
    conn.close()

def save_task(task):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # Поле recovery_time может отсутствовать, поэтому берем task.get('recovery_time')
    cursor.execute('''
        INSERT INTO tasks (id, db_instance, db_host, backup_id, recovery_time, status, created_at, canceled)
        VALUES (?, ?, ?, ?, ?, ?, ?, 0)
    ''', (task['id'], task['db_instance'], task['db_host'],
          task.get('backup_id'), task.get('recovery_time'), task['status'], task['created_at']))
    conn.commit()
    conn.close()
    cleanup_tasks()

def update_task_status(task_id, status, started_at=None, completed_at=None, backup_id=None):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    if backup_id is not None:
        cursor.execute('''
            UPDATE tasks
            SET status = ?,
                backup_id = ?,
                started_at = COALESCE(?, started_at),
                completed_at = COALESCE(?, completed_at)
            WHERE id = ?
        ''', (status, backup_id, started_at, completed_at, task_id))
    else:
        cursor.execute('''
            UPDATE tasks
            SET status = ?,
                started_at = COALESCE(?, started_at),
                completed_at = COALESCE(?, completed_at)
            WHERE id = ?
        ''', (status, started_at, completed_at, task_id))
    conn.commit()
    conn.close()

def update_recovery_time(task_id, recovery_time):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('UPDATE tasks SET recovery_time = ? WHERE id = ?', (recovery_time, task_id))
    conn.commit()
    conn.close()

def get_all_tasks():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM tasks')
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def fix_timezone(dt_str):
    if dt_str.endswith(('+00', '+01', '+02', '+03', '+04', '+05', '+06', '+07', '+08', '+09')):
        return dt_str + '00'
    if dt_str.endswith(('-00', '-01', '-02', '-03', '-04', '-05', '-06', '-07', '-08', '-09')):
        return dt_str + '00'
    return dt_str

def start_cluster_via_ssh(db_host, db_instance):
    # Извлекаем pg_ctl_instance из маппинга без запасного варианта
    pg_ctl_instance = DB_HOST_MAPPING.get(db_instance, {}).get('pg_ctl_instance')
    
    ssh_cmd = (
        f"ssh -p {SSH_PORT} -i {SSH_KEY} "
        f"{SSH_USER}@{db_host} "
        f"'pg_ctlcluster 11 {pg_ctl_instance} start'"
    )
    logger.info(f"Starting cluster via SSH: {ssh_cmd}")
    result = subprocess.run(
        ssh_cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )
    
    if result.returncode == 0:
        logger.info(f"Cluster started successfully on {db_host}")
        notification_message = f"{pg_ctl_instance} восстановлен в тестовом контуре"
    else:
        logger.error(f"Failed to start cluster on {db_host}: {result.stderr}")
        notification_message = f"{pg_ctl_instance} сервер не запустился за отведённое время или не восстановился корректно бэкап"
    
    curl_cmd = f"curl -X POST -d \"message={notification_message}\" {MESSAGE_HOST}:8080"
    logger.info(f"Sending notification via curl: {curl_cmd}")
    curl_result = subprocess.run(
        curl_cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )
    if curl_result.returncode == 0:
        logger.info("Notification sent successfully")
    else:
        logger.error(f"Failed to send notification: {curl_result.stderr}")

def is_task_canceled(task_id):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT canceled FROM tasks WHERE id = ?', (task_id,))
    result = cursor.fetchone()
    conn.close()
    return result is not None and result[0] == 1

def cancel_task(task_id):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('UPDATE tasks SET canceled = 1, status = "canceled" WHERE id = ?', (task_id,))
    conn.commit()
    conn.close()
    logger.info(f"Task {task_id} canceled.")

def log_and_run(cmd, db_host, db_instance, task_id):
    logger.info(f"Running command: {cmd}")
    process = subprocess.Popen(
        cmd, shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True
    )
    output = []
    for line in process.stdout:
        line = line.strip()
        logger.info(line)
        output.append(line)
        if is_task_canceled(task_id):
            logger.info("Task canceled. Killing the process.")
            process.kill()
            break
        if "INFO: Restore of backup" in line and "completed." in line:
            logger.info("Restore completed.")
            start_cluster_via_ssh(db_host, db_instance)
    process.wait()
    return process.returncode, "\n".join(output)

def process_task(task):
    task_id = task['id']
    if is_task_canceled(task_id):
        logger.info(f"Task {task_id} was canceled before processing.")
        return
    update_task_status(task_id, 'in_progress', started_at=time.strftime("%Y-%m-%d %H:%M:%S"))
    db_instance = task['db_instance']
    db_host = task['db_host']
    backup_id = task.get('backup_id')
    incremental = task.get('incremental', '').lower() in ['true', '1', 'yes']
    logger.info(f"Processing task: {task}")
    
    # Получаем список бекапов через pg_probackup show
    show_cmd = f"pg_probackup show --instance={db_instance} --format=json"
    result = subprocess.run(
        show_cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )
    if result.returncode != 0:
        logger.error(f"Error running pg_probackup show: {result.stderr}")
        update_task_status(task_id, 'error', completed_at=time.strftime("%Y-%m-%d %H:%M:%S"))
        return
    try:
        backups = json.loads(result.stdout)[0]["backups"]
    except (IndexError, KeyError, json.JSONDecodeError) as e:
        logger.error(f"Error parsing backups: {e}")
        update_task_status(task_id, 'error', completed_at=time.strftime("%Y-%m-%d %H:%M:%S"))
        return

    if backup_id:
        # Если backup_id указан, проверяем его наличие и извлекаем recovery_time
        matching_backup = None
        for backup in backups:
            if backup.get("id") == backup_id:
                matching_backup = backup
                break
        if not matching_backup:
            logger.error(f"Provided backup_id {backup_id} not found among backups.")
            update_task_status(task_id, 'error', completed_at=time.strftime("%Y-%m-%d %H:%M:%S"))
            return
        recovery_time = matching_backup.get("recovery-time")
        logger.info(f"Using provided backup_id: {backup_id} with recovery_time: {recovery_time}")
    else:
        # Если backup_id не указан, выбираем последний доступный бекап
        if not backups:
            logger.error("No backups found.")
            update_task_status(task_id, 'error', completed_at=time.strftime("%Y-%m-%d %H:%M:%S"))
            return
        latest_backup = max(
            backups,
            key=lambda x: datetime.strptime(
                fix_timezone(x["recovery-time"]),
                "%Y-%m-%d %H:%M:%S%z"
            )
        )
        backup_id = latest_backup["id"]
        recovery_time = latest_backup.get("recovery-time")
        logger.info(f"No backup_id provided, using latest backup with ID: {backup_id} and recovery_time: {recovery_time}")

    # Обновляем задачу в БД: записываем backup_id и recovery_time
    update_task_status(task_id, 'in_progress', backup_id=backup_id)
    update_recovery_time(task_id, recovery_time)
    
    if is_task_canceled(task_id):
        logger.info(f"Task {task_id} was canceled before preparing restore environment.")
        return
    prepare_restore_environment(db_host, db_instance, incremental)
    
    mapping = DB_HOST_MAPPING.get(db_instance, {})
    pgdata = mapping.get('pgdata', f"/data/{db_instance}")
    
    restore_cmd = (
        f"pg_probackup-11 restore --skip-external-dirs --progress -j2 "
        f"--instance={db_instance} --no-validate -B /data/probackup "
        f"--remote-host={db_host} --remote-port=422 --remote-user=postgres "
        f"--pgdata={pgdata} "
        f"--recovery-target-action=promote --archive-user=probackup "
        f"--archive-host={ARCHIVE_HOST} --archive-port=422 "
        f"--recovery-target='immediate' "
        f"--backup-id={backup_id}"
    )
    if incremental:
        restore_cmd += " -I CHECKSUM"
    ret, output = log_and_run(restore_cmd, db_host, db_instance, task_id)
    if is_task_canceled(task_id):
        update_task_status(task_id, 'canceled', completed_at=time.strftime("%Y-%m-%d %H:%M:%S"))
    elif ret == 0:
        update_task_status(task_id, 'completed', completed_at=time.strftime("%Y-%m-%d %H:%M:%S"))
    else:
        update_task_status(task_id, 'error', completed_at=time.strftime("%Y-%m-%d %H:%M:%S"))

def prepare_restore_environment(db_host, db_instance, incremental=False):
    mapping = DB_HOST_MAPPING.get(db_instance, {})
    pg_ctl_instance = mapping.get('pg_ctl_instance')
    pgdata = mapping.get('pgdata', f"/data/{db_instance}")
    
    if incremental:
        ssh_cmd = (
            f"ssh -p {SSH_PORT} -i {SSH_KEY} "
            f"{SSH_USER}@{db_host} "
            f"'pg_ctlcluster 11 {pg_ctl_instance} stop'"
        )
    else:
        ssh_cmd = (
            f"ssh -p {SSH_PORT} -i {SSH_KEY} "
            f"{SSH_USER}@{db_host} "
            f"'pg_ctlcluster 11 {pg_ctl_instance} stop; rm -rf {pgdata}/*'"
        )
    logger.info(f"Executing pre-restore commands via SSH: {ssh_cmd}")
    result = subprocess.run(
        ssh_cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )
    if result.returncode == 0:
        logger.info("Pre-restore commands executed successfully.")
    else:
        logger.error(f"Pre-restore commands failed: {result.stderr}")

def worker():
    logger.info("Worker thread started, waiting for tasks...")
    while True:
        task = queue.get()
        if task is None:
            break
        process_task(task)
        queue.task_done()

def parse_task_data(data_str):
    """
    Разбирает входящую строку, где элементы разделяются запятыми.
    Если элемент не содержит "=", то считается, что это флаг со значением "true".
    Нормализует ключи, заменяя дефисы на подчеркивания.
    """
    result = {}
    for item in data_str.split(', '):
        if '=' in item:
            key, value = item.split('=', 1)
            key = key.replace('-', '_')
            result[key] = value
        else:
            result[item] = "true"
    return result

def start_server():
    init_db()
    print(f"Starting Probackup Restore Service on {HOST}:{PORT}")
    logger.info(f"Starting Probackup Restore Service on {HOST}:{PORT}")
    threading.Thread(target=worker, daemon=True).start()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind((HOST, PORT))
        server.listen()
        print(f"Server is listening on {HOST}:{PORT}")
        logger.info(f"Server listening on {HOST}:{PORT}")
        while True:
            conn, addr = server.accept()
            with conn:
                data = conn.recv(4096).decode().strip()
                if data == "show":
                    show_cmd = "pg_probackup show --format=json"
                    result = subprocess.run(
                        show_cmd, shell=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        universal_newlines=True
                    )
                    if result.returncode == 0:
                        conn.sendall(result.stdout.encode() + b"\n")
                    else:
                        conn.sendall(f"Error: {result.stderr}".encode() + b"\n")
                    continue
                if data == "status":
                    tasks = get_all_tasks()
                    conn.sendall(json.dumps(tasks, indent=4).encode() + b"\n")
                    continue
                if data.startswith("cancel"):
                    try:
                        # Ожидаемый формат: "cancel, id=<task_id>"
                        parts = data.split(', ')
                        params = dict(item.split('=') for item in parts[1:])
                        task_id = params.get('id')
                        if not task_id:
                            raise ValueError("Не указан id задачи")
                        cancel_task(task_id)
                        conn.sendall(f"Task {task_id} canceled.\n".encode())
                    except Exception as e:
                        conn.sendall("Invalid cancel format\n".encode())
                        logger.error(f"Invalid cancel command: {data}, error: {e}")
                    continue
                try:
                    task = parse_task_data(data)
                    if 'db_host' not in task or not task['db_host']:
                        db_instance = task.get('db_instance')
                        if db_instance in DB_HOST_MAPPING:
                            task['db_host'] = DB_HOST_MAPPING[db_instance]['db_host']
                        else:
                            error_msg = f"Не найдено сопоставление db_host для db_instance: {db_instance}"
                            conn.sendall(error_msg.encode() + b"\n")
                            logger.error(error_msg)
                            continue
                    task['id'] = str(uuid.uuid4())
                    task['status'] = 'queued'
                    task['created_at'] = time.strftime("%Y-%m-%d %H:%M:%S")
                    save_task(task)
                    queue.put(task)
                    conn.sendall(f"Task accepted with ID: {task['id']}\n".encode())
                except Exception as e:
                    conn.sendall(b"Invalid data format\n")
                    logger.error(f"Invalid data received: {data}, error: {e}")

if __name__ == "__main__":
    try:
        print("Initializing service...")
        logger.info("Initializing Probackup Restore Service...")
        start_server()
    except Exception as e:
        print(f"Fatal error: {e}")
        logger.exception(f"Fatal error: {e}")