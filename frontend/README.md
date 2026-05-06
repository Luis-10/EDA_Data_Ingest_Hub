# Frontend — EDA Data Ingest Hub

Dashboard web construido con **Vue 3 + TypeScript + Vite** que consume la API REST del backend para visualizar estadisticas de inversion publicitaria en TV, Radio e Impresos. Incluye autenticacion JWT, registro de usuarios y control de acceso por tipo de medio.

---

## Stack tecnologico

| Tecnologia | Version | Proposito |
|---|---|---|
| Vue 3 | ^3.5 | Framework reactivo (`<script setup>` + Composition API) |
| TypeScript | ~5.9 | Tipado estatico |
| Vite | ^7.3 | Build tool y dev server |
| Vue Router | ^4.5 | Navegacion entre vistas con guards de autenticacion |
| Pinia | ^2.3 | Gestion de estado global |
| PrimeVue | ^4.5 | Biblioteca de componentes UI |
| Tailwind CSS | ^4.2 | Utilidades de estilos |
| Axios | ^1.13 | Cliente HTTP |
| Chart.js + vue-chartjs | ^4.x / ^5.x | Graficos de inversion y tendencias |
| xlsx | ^0.18 | Exportacion a Excel |
| pnpm | — | Gestor de paquetes |

---

## Estructura de directorios

```
frontend/
├── Dockerfile
├── index.html
├── package.json
├── vite.config.ts
├── tsconfig.json / tsconfig.app.json / tsconfig.node.json
├── tailwind.config.ts
├── public/
│   ├── vite.svg
│   └── publicisgroupelogo.svg
└── src/
    ├── main.ts               ← Entrypoint: Vue app, Pinia, PrimeVue (tema Aura), Vue Router
    ├── App.vue               ← Componente raiz
    ├── style.css             ← Estilos globales
    ├── router/
    │   └── index.ts          ← Vue Router: rutas y navigation guard JWT
    ├── layouts/
    │   └── DashboardLayout.vue  ← Layout con sidebar + navegacion (wrappea vistas autenticadas)
    ├── services/
    │   └── api.ts            ← Cliente HTTP (Axios): interfaces TypeScript + funciones de API
    ├── stores/
    │   ├── auth.ts           ← Store de autenticacion (JWT, login, logout, token persistido)
    │   ├── stats.ts          ← Store de estadisticas globales (stats + top-marcas con retry)
    │   ├── auditsa.ts        ← Store del dashboard: filtros, busqueda cross-tabla, resultados
    │   └── impresos.ts       ← Store especifico de Impresos
    └── views/
        ├── LoginView.vue         ← Formulario de autenticacion JWT
        ├── SignupView.vue         ← Registro de nuevos usuarios
        ├── AuditsaDashboard.vue  ← Vista principal del dashboard (activa)
        ├── ImpresosData.vue      ← Vista de registros de Impresos
        └── RadioData.vue         ← Vista de Radio
```

---

## Arquitectura de la aplicacion

### Enrutamiento (`router/index.ts`)

Vue Router con navigation guard que verifica la presencia de un token JWT antes de permitir acceso a rutas protegidas.

```
/ (raiz)
  ├── /login          ← LoginView (publica)
  ├── /signup         ← SignupView (publica)
  └── /dashboard      ← DashboardLayout (protegida, requiere token)
        ├── /          ← AuditsaDashboard
        ├── /impresos  ← ImpresosData
        └── /radio     ← RadioData
```

El guard redirige a `/login` si no hay token. Al hacer login exitoso, redirige al dashboard.

### Layout (`DashboardLayout.vue`)

Estructura de dos columnas: sidebar colapsable (220 px) + area de contenido principal. Contiene la navegacion entre vistas y el boton de logout.

```
DashboardLayout.vue
├── <aside> Sidebar (220px, fondo oscuro)
│     ├── Logo / nombre de la aplicacion
│     ├── Nav items (botones con estado activo)
│     └── Nombre de usuario + rol + boton logout
├── <button> Float toggle (colapsa/expande sidebar)
└── <main>
      └── <router-view> ← Vistas: AuditsaDashboard / ImpresosData / RadioData
```

### Servicio API (`src/services/api.ts`)

Cliente centralizado con Axios. Todas las llamadas usan rutas relativas (`/api/...`) — el proxy de Vite las redirige al backend. El token JWT se incluye automaticamente via interceptor Axios.

**Interfaces TypeScript exportadas:**

| Interfaz | Descripcion |
|---|---|
| `GeneralStatsResponse` | Respuesta de `/api/{tipo}/stats` |
| `StatusRangeResponse` | Respuesta de `/api/{tipo}/status/range` |
| `TopMarcaRangeItem` | Item de top marcas por inversion |
| `ImpresosRecord` | Registro individual de Impresos |
| `ImpresosRecordsResponse` | Respuesta paginada de `/api/impresos/records` |
| `AutocompleteFilters` | Filtros opcionales en cascada para autocompletado |
| `StatsSummaryResponse` | Respuesta de `/api/stats/summary` (stats + top-marcas + top-sectores) |

**Funciones exportadas:**

| Funcion | Endpoint | Descripcion |
|---|---|---|
| `login(username, password)` | `POST /auth/token` | Obtener token JWT |
| `register(payload)` | `POST /auth/register` | Registrar nuevo usuario |
| `getAccounts()` | `GET /auth/accounts` | Lista de cuentas disponibles |
| `getStatsSummary()` | `GET /api/stats/summary` | Stats + top-marcas + top-sectores de los 3 medios |
| `getAutocomplete(field, q, limit, filters)` | `GET /api/autocomplete` | Sugerencias con filtros en cascada |
| `getGeneralStats(tipo)` | `GET /api/{tipo}/stats` | Estadisticas globales |
| `getStatusRange(tipo, fi, ff, filters)` | `GET /api/{tipo}/status/range` | Stats filtradas en rango de fechas |
| `getTopMarcasAllTime(tipo)` | `GET /api/{tipo}/top-marcas` | Top marcas historicas |
| `getTopMarcasRange(tipo, fi, ff, filters)` | `GET /api/{tipo}/top-marcas/range` | Top marcas en rango con filtros |
| `getImpresosRecords(params)` | `GET /api/impresos/records` | Registros paginados de Impresos |
| `exportRecords(tipo, params)` | `GET /api/export/{tipo}` | Datos para exportacion Excel |

### Stores Pinia

#### `auth.ts` — `useAuthStore`

Gestiona la sesion del usuario. El token JWT se persiste en `localStorage`.

| Estado | Tipo | Descripcion |
|---|---|---|
| `token` | `string \| null` | Token JWT activo |
| `username` | `string \| null` | Nombre del usuario autenticado |
| `role` | `string \| null` | Rol (admin, reader, standard) |
| `allowedMedia` | `string[]` | Tipos de medio accesibles segun la cuenta |

Metodos: `login(username, password)`, `logout()`, `isAuthenticated` (getter).

#### `stats.ts` — `useStatsStore`

Carga y cachea las estadisticas globales de las 3 tablas. Incluye **retry con backoff exponencial** (hasta 4 intentos, delays: 2s, 4s, 8s) para tolerar el tiempo de prewarm del cache del backend.

| Estado | Tipo | Descripcion |
|---|---|---|
| `tvStats` / `radioStats` / `impresosStats` | `GeneralStatsResponse` | Estadisticas globales por tipo |
| `tvTopMarcas` / `radioTopMarcas` / `impresosTopMarcas` | `TopMarcaRangeItem[]` | Top marcas historicas |
| `loading` / `loaded` | `boolean` | Control de estado de carga |

Metodos: `loadAll()` (guarded — no recarga si ya hay datos), `refresh()` (fuerza recarga).

#### `auditsa.ts` — `useAuditsaStore`

Gestiona el estado completo del dashboard de busqueda cross-tabla:
- **Filtros** con soporte de multiples valores (tags): `anunciante`, `marca`, `categoria`, `localidad`, `tipoMedio`, `medio`, `fechaInicio`, `fechaFin`
- **`buscar(filters?)`**: llama en paralelo (`Promise.all`) a los 6 endpoints necesarios (status/range + top-marcas/range) para los 3 tipos de medio
- **`clearAll()`**: resetea todos los filtros y resultados

#### `impresos.ts` — `useImpresosStore`

Store especifico para la vista de registros paginados de Impresos.

### Vistas

| Vista | Ruta | Descripcion |
|---|---|---|
| `LoginView.vue` | `/login` | Formulario de autenticacion JWT. Llama a `useAuthStore.login()`. |
| `SignupView.vue` | `/signup` | Formulario de registro. Carga cuentas desde `/auth/accounts` para el selector. |
| `AuditsaDashboard.vue` | `/dashboard` | Dashboard principal. Filtros con autocompletado en cascada, resultados comparativos TV/Radio/Impresos, tablas de top marcas. |
| `ImpresosData.vue` | `/dashboard/impresos` | Vista de registros paginados de Impresos. |
| `RadioData.vue` | `/dashboard/radio` | Vista de registros de Radio. |

### Vista principal (`AuditsaDashboard.vue`)

Dashboard de busqueda y comparacion cross-tabla con:
- Selector de rango de fechas
- Filtros con **autocompletado en cascada** (debounced, llama a `/api/autocomplete`)
- Resultados comparativos TV / Radio / Impresos: total de registros, fechas, canales/fuentes, marcas, anunciantes e inversion total
- Tablas de Top Marcas por tipo
- Botones de exportacion a Excel via `/api/export/{tipo}`

---

## Proxy de desarrollo (`vite.config.ts`)

```typescript
server: {
  host: '0.0.0.0',
  port: 3000,
  proxy: {
    '/api': {
      target: process.env.API_TARGET ?? 'http://localhost:8000',
      changeOrigin: true,
    },
    '/auth': {
      target: process.env.API_TARGET ?? 'http://localhost:8000',
      changeOrigin: true,
    },
  },
}
```

Todas las peticiones a `/api/*` y `/auth/*` se redirigen al backend. La variable `API_TARGET` permite configurar el host destino (en Docker se inyecta `http://backend:8000`).

---

## Dockerfile

```dockerfile
FROM node:25-alpine
RUN npm install -g pnpm
WORKDIR /app
EXPOSE 3000
CMD ["sh", "-c", "pnpm install --node-linker=hoisted && pnpm dev --host 0.0.0.0 --port 3000"]
```

El codigo fuente se monta como volumen en docker-compose (`./frontend:/app`), habilitando hot-reload en desarrollo.

---

## Ejecucion local

```bash
# Instalar dependencias
pnpm install

# Servidor de desarrollo (http://localhost:3000)
pnpm dev

# Build de produccion
pnpm build

# Preview del build
pnpm preview
```

> El backend debe estar corriendo en `http://localhost:8000` (o configurar `API_TARGET`) para que el proxy funcione correctamente.

## Credenciales por defecto

| Usuario | Password | Rol | Acceso |
|---|---|---|---|
| `admin` | `admin123` | admin | Todo |
| `reader` | `reader123` | reader | Solo lectura |
| Usuarios registrados | (su contrasena) | standard | Segun `allowed_media` de su cuenta |
