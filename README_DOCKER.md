# flask_pdf_viewer (DEV en Docker - Windows Server 2019)

## Requisitos
- Windows Server 2019
- Docker funcionando en modo **Windows Containers**

## Levantar en DEV
En PowerShell (Admin), dentro de esta carpeta:

```powershell
docker compose build
docker compose up -d
docker ps
```

Abre:
- http://localhost:5005

Logs:
```powershell
docker logs -f flask_pdf_viewer_dev
```

## Variables de entorno
En `docker-compose.yml` completa:
- `FLASK_SECRET_KEY`
- `DB_CONN_STRING` (ODBC Driver 18)

Ejemplo:
`Driver={ODBC Driver 18 for SQL Server};Server=10.4.1.240,1433;Database=TU_BD;UID=TU_USER;PWD=TU_PASS;TrustServerCertificate=yes;`
