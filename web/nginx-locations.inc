# Общие маршруты веб-версии (include в server-блоки web/nginx.conf и web/nginx-ssl.conf): графики сделок и само приложение Flet с WebSocket.
# Адрес web разрешается на каждый запрос (resolver в server-блоке): nginx стартует и тогда, когда web ещё пересобирается.

# графики сделок: aiohttp-сервер внутри контейнера web (порт 8551, наружу не опубликован); ссылка /chart/<случайный id> живёт 1 час
location /chart/ {
    set $charts http://web:8551;
    proxy_pass $charts;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_read_timeout 60s;
}

# Flet: обычные запросы и WebSocket (по HTTPS браузер сам открывает wss://). Upgrade/Connection пробрасываются через map $connection_upgrade.
location / {
    set $flet http://web:8550;
    proxy_pass $flet;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection $connection_upgrade;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_read_timeout 3600s;
    proxy_send_timeout 3600s;
    proxy_buffering off;
}
