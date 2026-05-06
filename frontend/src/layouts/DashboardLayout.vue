<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useRouter } from 'vue-router'
import { useAuthStore } from '../stores/auth'
import AuditsaDashboard from '../views/AuditsaDashboard.vue'
import ImpresosData from '../views/ImpresosData.vue'
import RadioData from '../views/RadioData.vue'

type View = 'auditsa' | 'impresos' | 'radio'

const router = useRouter()
const auth = useAuthStore()

const currentView = ref<View>('auditsa')
const sidebarOpen = ref(true)
const showLogoutConfirm = ref(false)

const ALL_NAV_ITEMS: { id: View; icon: string; label: string; media: string }[] = [
  { id: 'auditsa',  icon: '📊', label: 'TV Dashboard', media: 'tv' },
  // { id: 'radio',    icon: '📻', label: 'Radio',        media: 'radio' },
  // { id: 'impresos', icon: '📰', label: 'Impresos',     media: 'impresos' },
]

const navItems = computed(() => {
  const allowed = auth.user?.allowed_media ?? ['tv', 'radio', 'impresos']
  return ALL_NAV_ITEMS.filter(item => allowed.includes(item.media))
})

// If the active view is no longer accessible, switch to the first allowed one
watch(navItems, (items) => {
  if (items.length > 0 && !items.find(i => i.id === currentView.value)) {
    currentView.value = items[0].id
  }
}, { immediate: true })

const userInitial = computed(() =>
  auth.user?.username?.charAt(0).toUpperCase() ?? '?'
)

const userRole = computed(() => {
  const r = auth.user?.role
  if (r === 'admin') return 'Administrador'
  if (r === 'reader') return 'Solo lectura'
  return 'Estándar'
})

function logout(): void {
  showLogoutConfirm.value = false
  auth.logout()
  router.push('/login')
}
</script>

<template>
  <div class="app-layout">
    <!-- Sidebar -->
    <aside v-if="sidebarOpen" class="sidebar">
      <div class="sidebar-logo">
        <span class="logo-icon">🚀</span>
        <span class="logo-text">Data Hub</span>
      </div>

      <nav class="sidebar-nav">
        <button
          v-for="item in navItems"
          :key="item.id"
          class="nav-item"
          :class="{ 'nav-item--active': currentView === item.id }"
          @click="currentView = item.id"
        >
          <span class="nav-icon">{{ item.icon }}</span>
          <span>{{ item.label }}</span>
        </button>
      </nav>

      <!-- User info + logout -->
      <div class="sidebar-user">
        <div class="user-avatar">{{ userInitial }}</div>
        <div class="user-info">
          <p class="user-name">{{ auth.user?.username }}</p>
          <p class="user-role">{{ userRole }}</p>
        </div>
        <button
          class="logout-btn"
          title="Cerrar sesión"
          @click="showLogoutConfirm = true"
        >
          <svg width="15" height="15" viewBox="0 0 24 24" fill="none">
            <path
              d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
            />
          </svg>
        </button>
      </div>
    </aside>

    <!-- Botón flotante toggle -->
    <button
      class="float-toggle"
      :class="{ 'float-toggle--closed': !sidebarOpen }"
      @click="sidebarOpen = !sidebarOpen"
      :title="sidebarOpen ? 'Ocultar sidebar' : 'Mostrar sidebar'"
    >
      <svg v-if="sidebarOpen" width="16" height="16" viewBox="0 0 16 16" fill="none">
        <path d="M12 4L4 12M4 4l8 8" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
      </svg>
      <svg v-else width="16" height="16" viewBox="0 0 16 16" fill="none">
        <path d="M2 4h12M2 8h12M2 12h12" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
      </svg>
    </button>

    <!-- Main -->
    <main class="main-content">
      <AuditsaDashboard v-if="currentView === 'auditsa'" />
      <ImpresosData v-else-if="currentView === 'impresos'" />
      <RadioData v-else-if="currentView === 'radio'" />
    </main>

    <!-- Logout confirm modal -->
    <div v-if="showLogoutConfirm" class="modal-overlay" @click.self="showLogoutConfirm = false">
      <div class="modal">
        <p class="modal-title">¿Cerrar sesión?</p>
        <p class="modal-body">Se cerrará tu sesión actual y serás redirigido al login.</p>
        <div class="modal-actions">
          <button class="modal-cancel" @click="showLogoutConfirm = false">Cancelar</button>
          <button class="modal-confirm" @click="logout">Cerrar sesión</button>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.app-layout {
  display: flex;
  height: 100vh;
  overflow: hidden;
  background: #f1f5f9;
  position: relative;
}

/* ── Sidebar ── */
.sidebar {
  width: 220px;
  min-width: 220px;
  background: #1a2332;
  display: flex;
  flex-direction: column;
  height: 100vh;
}

.sidebar-logo {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 20px 16px;
  border-bottom: 1px solid rgba(255,255,255,0.08);
}

.logo-icon { font-size: 22px; flex-shrink: 0; }

.logo-text {
  color: #fff;
  font-weight: 700;
  font-size: 17px;
  letter-spacing: -0.3px;
}

.sidebar-nav {
  flex: 1;
  padding: 12px 10px;
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.nav-item {
  display: flex;
  align-items: center;
  gap: 10px;
  width: 100%;
  padding: 10px 12px;
  border: none;
  border-radius: 8px;
  background: transparent;
  color: rgba(255,255,255,0.6);
  font-size: 13.5px;
  font-weight: 500;
  cursor: pointer;
  text-align: left;
  transition: background 0.15s, color 0.15s;
}
.nav-item:hover { background: rgba(255,255,255,0.07); color: #fff; }
.nav-item--active { background: #f97316; color: #fff; }
.nav-item--active:hover { background: #ea6c0a; }

.nav-icon { font-size: 15px; }

.sidebar-user {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 14px 16px;
  border-top: 1px solid rgba(255,255,255,0.08);
}

.user-avatar {
  width: 34px;
  height: 34px;
  border-radius: 50%;
  background: #f97316;
  color: #fff;
  font-weight: 700;
  font-size: 14px;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
}

.user-info { min-width: 0; flex: 1; }

.user-name {
  color: #fff;
  font-size: 13px;
  font-weight: 600;
  margin: 0;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.user-role {
  color: rgba(255,255,255,0.45);
  font-size: 11.5px;
  margin: 0;
}

.logout-btn {
  background: transparent;
  border: none;
  color: rgba(255,255,255,0.35);
  cursor: pointer;
  padding: 4px;
  border-radius: 6px;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: color 0.15s, background 0.15s;
  flex-shrink: 0;
}
.logout-btn:hover {
  color: #fca5a5;
  background: rgba(239, 68, 68, 0.15);
}

/* ── Botón flotante ── */
.float-toggle {
  position: absolute;
  top: 22px;
  left: calc(220px - 16px);
  z-index: 100;
  width: 32px;
  height: 32px;
  border-radius: 50%;
  background: #f97316;
  color: #fff;
  border: 2px solid #fff;
  box-shadow: 0 2px 8px rgba(0,0,0,0.18);
  display: flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  transition: left 0.01s, background 0.15s, transform 0.15s;
}
.float-toggle:hover { background: #ea6c0a; transform: scale(1.08); }
.float-toggle--closed { left: 8px; }

/* ── Main ── */
.main-content {
  flex: 1;
  overflow-y: auto;
  background: #f1f5f9;
}

/* ── Modal ── */
.modal-overlay {
  position: fixed;
  inset: 0;
  background: rgba(0,0,0,0.55);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}

.modal {
  background: #1a2332;
  border-radius: 14px;
  padding: 28px 32px;
  max-width: 360px;
  width: 90%;
  border: 1px solid rgba(255,255,255,0.1);
  box-shadow: 0 20px 60px rgba(0,0,0,0.5);
}

.modal-title {
  color: #fff;
  font-size: 17px;
  font-weight: 700;
  margin: 0 0 8px 0;
}

.modal-body {
  color: rgba(255,255,255,0.5);
  font-size: 13.5px;
  margin: 0 0 24px 0;
  line-height: 1.5;
}

.modal-actions {
  display: flex;
  gap: 10px;
  justify-content: flex-end;
}

.modal-cancel {
  background: rgba(255,255,255,0.07);
  border: 1px solid rgba(255,255,255,0.12);
  border-radius: 8px;
  color: rgba(255,255,255,0.7);
  cursor: pointer;
  font-size: 13.5px;
  font-weight: 500;
  padding: 9px 18px;
  transition: background 0.15s;
}
.modal-cancel:hover { background: rgba(255,255,255,0.12); }

.modal-confirm {
  background: #ef4444;
  border: none;
  border-radius: 8px;
  color: #fff;
  cursor: pointer;
  font-size: 13.5px;
  font-weight: 600;
  padding: 9px 18px;
  transition: background 0.15s;
}
.modal-confirm:hover { background: #dc2626; }
</style>
