# PRD (Windows Server 2025 + Docker Engine)

Este paquete deja tu app lista para correr en PRD como **Windows container**.

## Variables de entorno
Copia `.env.prd.example` a `.env.prd` y completa credenciales.

## Build & Run
Desde la carpeta del proyecto:

```powershell
docker compose -f docker-compose.prd.yml up -d --build
```

## Verificar
```powershell
docker ps
docker logs -f flask_pdf_viewer_prd
```

La app escuchará en `http://127.0.0.1:5000`.

## Publicación recomendada
- Mantener el contenedor en `127.0.0.1:5000`
- Publicar hacia usuarios con IIS (ARR/Reverse Proxy) con HTTPS.

## Nota sobre autenticación
Por ahora se mantiene el **DemoUser** (admin/admin) tal como está en el código.
Para PRD real, lo correcto es mover auth a BD (`AUTH_PRD`) y hash bcrypt.
