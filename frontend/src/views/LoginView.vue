<script setup lang="ts">
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { useAuthStore } from '../stores/auth'

const router = useRouter()
const auth = useAuthStore()

const username = ref('')
const password = ref('')
const loading = ref(false)
const errorMsg = ref('')

async function handleLogin(): Promise<void> {
  if (!username.value.trim() || !password.value) return

  loading.value = true
  errorMsg.value = ''

  try {
    await auth.login(username.value.trim(), password.value)
    router.push('/')
  } catch (err: unknown) {
    const axiosErr = err as { response?: { data?: { detail?: string } } }
    errorMsg.value =
      axiosErr.response?.data?.detail ?? 'Error de conexión. Intenta de nuevo.'
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div class="auth-page">
    <div class="auth-card">
      <!-- Logo -->
      <div class="auth-logo">
        <span class="logo-icon">🚀</span>
        <span class="logo-text">Data Hub</span>
      </div>

      <h2 class="auth-title">Iniciar sesión</h2>
      <p class="auth-subtitle">Accede al dashboard de inversión publicitaria</p>

      <form class="auth-form" @submit.prevent="handleLogin">
        <!-- Username -->
        <div class="field">
          <label class="field-label">Usuario</label>
          <input
            v-model="username"
            type="text"
            class="field-input"
            placeholder="Tu nombre de usuario"
            autocomplete="username"
            :disabled="loading"
          />
        </div>

        <!-- Password -->
        <div class="field">
          <label class="field-label">Contraseña</label>
          <input
            v-model="password"
            type="password"
            class="field-input"
            placeholder="••••••••"
            autocomplete="current-password"
            :disabled="loading"
          />
        </div>

        <!-- Error -->
        <div v-if="errorMsg" class="auth-error">
          <span class="error-icon">⚠</span>
          {{ errorMsg }}
        </div>

        <!-- Submit -->
        <button type="submit" class="auth-btn" :disabled="loading || !username || !password">
          <span v-if="loading" class="spinner"></span>
          <span v-else>Entrar</span>
        </button>
      </form>

      <!-- Footer link -->
      <p class="auth-footer">
        ¿No tienes cuenta?
        <RouterLink to="/register" class="auth-link">Regístrate aquí</RouterLink>
      </p>
    </div>
  </div>
</template>

<style scoped>
.auth-page {
  min-height: 100vh;
  background: #0f1520;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 24px;
}

.auth-card {
  background: #1a2332;
  border-radius: 16px;
  padding: 40px 36px;
  width: 100%;
  max-width: 400px;
  box-shadow: 0 20px 60px rgba(0, 0, 0, 0.5);
  border: 1px solid rgba(255, 255, 255, 0.07);
}

.auth-logo {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 28px;
}

.logo-icon {
  font-size: 26px;
}

.logo-text {
  color: #fff;
  font-weight: 700;
  font-size: 20px;
  letter-spacing: -0.3px;
}

.auth-title {
  color: #fff;
  font-size: 22px;
  font-weight: 700;
  margin: 0 0 6px 0;
}

.auth-subtitle {
  color: rgba(255, 255, 255, 0.45);
  font-size: 13.5px;
  margin: 0 0 28px 0;
}

.auth-form {
  display: flex;
  flex-direction: column;
  gap: 18px;
}

.field {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.field-label {
  color: rgba(255, 255, 255, 0.7);
  font-size: 13px;
  font-weight: 500;
}

.field-input {
  background: rgba(255, 255, 255, 0.06);
  border: 1px solid rgba(255, 255, 255, 0.12);
  border-radius: 8px;
  color: #fff;
  font-size: 14px;
  padding: 11px 14px;
  outline: none;
  transition: border-color 0.15s, background 0.15s;
  width: 100%;
  box-sizing: border-box;
}

.field-input::placeholder {
  color: rgba(255, 255, 255, 0.25);
}

.field-input:focus {
  border-color: #f97316;
  background: rgba(249, 115, 22, 0.06);
}

.field-input:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.auth-error {
  background: rgba(239, 68, 68, 0.12);
  border: 1px solid rgba(239, 68, 68, 0.35);
  border-radius: 8px;
  color: #fca5a5;
  font-size: 13px;
  padding: 10px 14px;
  display: flex;
  align-items: center;
  gap: 8px;
}

.error-icon {
  font-size: 14px;
  flex-shrink: 0;
}

.auth-btn {
  background: #f97316;
  border: none;
  border-radius: 8px;
  color: #fff;
  cursor: pointer;
  font-size: 15px;
  font-weight: 600;
  padding: 12px;
  transition: background 0.15s, opacity 0.15s;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  margin-top: 4px;
}

.auth-btn:hover:not(:disabled) {
  background: #ea6c0a;
}

.auth-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.spinner {
  width: 16px;
  height: 16px;
  border: 2px solid rgba(255, 255, 255, 0.3);
  border-top-color: #fff;
  border-radius: 50%;
  animation: spin 0.7s linear infinite;
  flex-shrink: 0;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.auth-footer {
  color: rgba(255, 255, 255, 0.4);
  font-size: 13.5px;
  text-align: center;
  margin: 22px 0 0 0;
}

.auth-link {
  color: #f97316;
  text-decoration: none;
  font-weight: 500;
}

.auth-link:hover {
  text-decoration: underline;
}
</style>
