# NexoWorld Online

## Publicación recomendada

GitHub guarda el código. Para que el chat global, presencia, likes, mundos y WebSocket funcionen, despliega el mismo repositorio como **Web Service** en Render.

### Archivos
- `NexoWorld_Arcade_MEJORADO.html` — cliente web.
- `server.js` — servidor HTTP + WebSocket + API.
- `package.json` — comando `npm start`.
- `render.yaml` — configuración de Render.

### Render
- Build Command: `npm install`
- Start Command: `npm start`
- Health Check: `/api/health`

### Nota
GitHub Pages sirve contenido estático; no ejecuta este `server.js`. Para la versión online usa el Web Service.
