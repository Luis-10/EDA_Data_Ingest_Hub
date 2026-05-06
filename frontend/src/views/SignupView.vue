<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { useAuthStore } from '../stores/auth'
import { getAccounts } from '../services/api'

const router = useRouter()
const auth = useAuthStore()

const username = ref('')
const fullName = ref('')
const email = ref('')
const password = ref('')
const confirmPassword = ref('')
const account = ref('')

const accounts = ref<string[]>([])
const accountsLoading = ref(false)

const loading = ref(false)
const errorMsg = ref('')
const successMsg = ref('')

onMounted(async () => {
  accountsLoading.value = true
  try {
    accounts.value = await getAccounts()
  } catch {
    accounts.value = []
  } finally {
    accountsLoading.value = false
  }
})

function validate(): string | null {
  if (!username.value.trim()) return 'El nombre de usuario es requerido.'
  if (username.value.trim().length < 3) return 'El usuario debe tener al menos 3 caracteres.'
  if (!fullName.value.trim()) return 'El nombre completo es requerido.'
  if (!email.value.trim()) return 'El correo electrónico es requerido.'
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email.value)) return 'Ingresa un correo electrónico válido.'
  if (!account.value) return 'Debes seleccionar una cuenta.'
  if (password.value.length < 6) return 'La contraseña debe tener al menos 6 caracteres.'
  if (password.value !== confirmPassword.value) return 'Las contraseñas no coinciden.'
  return null
}

async function handleRegister(): Promise<void> {
  errorMsg.value = ''
  successMsg.value = ''

  const validationError = validate()
  if (validationError) {
    errorMsg.value = validationError
    return
  }

  loading.value = true

  try {
    await auth.register({
      username: username.value.trim(),
      full_name: fullName.value.trim(),
      email: email.value.trim(),
      password: password.value,
      account: account.value,
    })
    successMsg.value = 'Cuenta creada exitosamente. Redirigiendo al login...'
    setTimeout(() => router.push('/login'), 2000)
  } catch (err: unknown) {
    const axiosErr = err as { response?: { data?: { detail?: string } } }
    errorMsg.value =
      axiosErr.response?.data?.detail ?? 'Error al crear la cuenta. Intenta de nuevo.'
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

      <h2 class="auth-title">Crear cuenta</h2>
      <p class="auth-subtitle">Solicita acceso al dashboard de inversión publicitaria</p>

      <form class="auth-form" @submit.prevent="handleRegister">
        <!-- Username -->
        <div class="field">
          <label class="field-label">Usuario</label>
          <input
            v-model="username"
            type="text"
            class="field-input"
            placeholder="Ej: jlopez"
            autocomplete="username"
            :disabled="loading"
          />
        </div>

        <!-- Full Name -->
        <div class="field">
          <label class="field-label">Nombre completo</label>
          <input
            v-model="fullName"
            type="text"
            class="field-input"
            placeholder="Ej: Juan López"
            autocomplete="name"
            :disabled="loading"
          />
        </div>

        <!-- Email -->
        <div class="field">
          <label class="field-label">Correo electrónico</label>
          <input
            v-model="email"
            type="email"
            class="field-input"
            placeholder="usuario@empresa.com"
            autocomplete="email"
            :disabled="loading"
          />
        </div>

        <!-- Account -->
        <div class="field">
          <label class="field-label">Cuenta</label>
          <select
            v-model="account"
            class="field-input field-select"
            :disabled="loading || accountsLoading"
          >
            <option value="" disabled>
              {{ accountsLoading ? 'Cargando cuentas...' : 'Selecciona una cuenta' }}
            </option>
            <option v-for="acc in accounts" :key="acc" :value="acc">{{ acc }}</option>
          </select>
        </div>

        <!-- Password -->
        <div class="field-row">
          <div class="field">
            <label class="field-label">Contraseña</label>
            <input
              v-model="password"
              type="password"
              class="field-input"
              placeholder="Min. 6 caracteres"
              autocomplete="new-password"
              :disabled="loading"
            />
          </div>
          <div class="field">
            <label class="field-label">Confirmar contraseña</label>
            <input
              v-model="confirmPassword"
              type="password"
              class="field-input"
              placeholder="Repite la contraseña"
              autocomplete="new-password"
              :disabled="loading"
            />
          </div>
        </div>

        <!-- Role notice -->
        <div class="role-notice">
          <span class="notice-icon">ℹ</span>
          Las cuentas nuevas tienen rol <strong>Standard</strong> (consulta e inserción). Contacta al administrador para acceso completo.
        </div>

        <!-- Error -->
        <div v-if="errorMsg" class="auth-error">
          <span class="error-icon">⚠</span>
          {{ errorMsg }}
        </div>

        <!-- Success -->
        <div v-if="successMsg" class="auth-success">
          <span class="success-icon">✓</span>
          {{ successMsg }}
        </div>

        <!-- Submit -->
        <button
          type="submit"
          class="auth-btn"
          :disabled="loading || !!successMsg"
        >
          <span v-if="loading" class="spinner"></span>
          <span v-else>Crear cuenta</span>
        </button>
      </form>

      <!-- Footer link -->
      <p class="auth-footer">
        ¿Ya tienes cuenta?
        <RouterLink to="/login" class="auth-link">Inicia sesión</RouterLink>
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
  max-width: 480px;
  box-shadow: 0 20px 60px rgba(0, 0, 0, 0.5);
  border: 1px solid rgba(255, 255, 255, 0.07);
}

.auth-logo {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 28px;
}

.logo-icon { font-size: 26px; }

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
  gap: 16px;
}

.field {
  display: flex;
  flex-direction: column;
  gap: 6px;
  flex: 1;
}

.field-row {
  display: flex;
  gap: 14px;
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

.field-select {
  appearance: none;
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='8' viewBox='0 0 12 8'%3E%3Cpath fill='rgba(255,255,255,0.4)' d='M1 1l5 5 5-5'/%3E%3C/svg%3E");
  background-repeat: no-repeat;
  background-position: right 14px center;
  padding-right: 36px;
  cursor: pointer;
}

.field-select option {
  background: #1a2332;
  color: #fff;
}

.field-input:focus {
  border-color: #f97316;
  background: rgba(249, 115, 22, 0.06);
}

.field-input:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.role-notice {
  background: rgba(59, 130, 246, 0.1);
  border: 1px solid rgba(59, 130, 246, 0.25);
  border-radius: 8px;
  color: rgba(147, 197, 253, 0.9);
  font-size: 12.5px;
  padding: 10px 14px;
  display: flex;
  align-items: flex-start;
  gap: 8px;
  line-height: 1.5;
}

.notice-icon { font-size: 14px; flex-shrink: 0; margin-top: 1px; }

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

.auth-success {
  background: rgba(34, 197, 94, 0.12);
  border: 1px solid rgba(34, 197, 94, 0.35);
  border-radius: 8px;
  color: #86efac;
  font-size: 13px;
  padding: 10px 14px;
  display: flex;
  align-items: center;
  gap: 8px;
}

.error-icon, .success-icon { font-size: 14px; flex-shrink: 0; }

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

.auth-btn:hover:not(:disabled) { background: #ea6c0a; }

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

.auth-link:hover { text-decoration: underline; }
</style>
