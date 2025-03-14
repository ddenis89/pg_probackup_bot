# pg_probackup_bot
Серверное приложение для автоматизированного восстановления баз данных PostgreSQL с использованием утилиты pg_probackup. Принимает задачи восстановления через TCP, сохраняет их состояние в SQLite. Скрипт выполняет подготовку среды восстановления (остановку кластера, очистку каталога данных при неинкрементальном восстановлении), запускает процесс восстановления по заданному backup_id (либо выбирает последний бэкап) и автоматически запускает кластер после завершения восстановления. Все параметры конфигурации задаются через JSON-файл.

# Функциональность
- Прием заданий восстановления через TCP.
- Сохранение истории и статусов задач в базе данных SQLite.
- Очередь заданий: одновременно выполняется не более одного задания.
- Подготовка среды восстановления: остановка кластера, очистка каталога данных (если восстановление не инкрементальное).
- Восстановление базы данных с поддержкой инкрементального режима (при передаче флага incremental добавляется ключ -I CHECKSUM).
- Автоматический запуск кластера после завершения восстановления.
- Гибкая конфигурация через файл config.json.

# Запуск
Запустить скрипт можно через команду python ./server_pg_bot.py
Важно: Файл config.json должен находиться в той же папке, что и скрипт. В конфигурационном файле задаются все необходимые переменные: адреса, порты, пути, параметры SSH, маппинг db_instance и т.д.
Скрипт также можно запустить как службу systemd для автоматического запуска и мониторинга.
При запуске приложение проверяет наличие каталога для логов (по умолчанию ./Logs) и базы данных SQLite. Если их нет, они будут созданы автоматически.
При запуске проверяетя наличие папкии ./Logs и базы SQLite, если их нет они будут созданы.

По умолчанию скрипт ждет команды на 5000 порту, команды бывают такого типа например:
db_instance=main1 – обязательный параметр, означает какой кластер postgres мы хотим восстановить, в примере например указан имя кластера МС, имена кластера берется с pg_probackup show
db_host=S-DB1 - куда восстанавливаем, по сути это параметр --remote-host= в команде pg_probackup restore, db_host это не обязательный параметр, если его не укажем то он возьмется с файла config.json  
backup_id=SSCOAD – id бэкапа для восстановления, берется с pg_probackup show, backup_id не обязательный параметр, если его не укажем то возьмется самый свежий бэкап 
incremental – не обязательный параметр, в команду pg_probackup restore добавляет -I CHEKSUM, восстановит инкрементально 

# Примеры команды восстанвоелния:

echo "db_instance=main1" | nc IP_где_запущен_скрипт 5000
восстановить последнюю копию main1

echo "db_instance=main1, db_host=S-DB1" | nc IP_где_запущен_скрипт 5000
восстановить последнюю копию main1 на хост t36s-b-db1

echo "db_instance=main1, db_host=S-DB1, backup_id=SSCOAD" | nc IP_где_запущен_скрипт 5000
восстановить бэкап main1 с id SSCOAD на хост S-DB1

echo "db_instance=main1, db_host=S-DB1, backup_id=SSCOAD, incremental" | nc IP_где_запущен_скрипт 5000
восстановить инкрементально бэкап main1 с id SSCOAD на хост S-DB1

если отправим все команды сразу, то они встанут в очередь, одновременно выполянется не больше одного задания

# Логирование
Все действия приложения логируются с подробными сообщениями. Логи сохраняются в каталоге, указанном в конфигурационном файле (по умолчанию ./Logs).
