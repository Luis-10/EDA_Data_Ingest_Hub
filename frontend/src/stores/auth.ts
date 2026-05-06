import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import axios from 'axios'

export interface AuthUser {
  username: string
  role: 'admin' | 'reader' | 'standard'
  allowed_media: string[]
}

export const useAuthStore = defineStore('auth', () => {
  const token = ref<string | null>(localStorage.getItem('auth_token'))
  const user = ref<AuthUser | null>(null)

  const isAuthenticated = computed(() => !!token.value)

  function decodeToken(jwt: string): AuthUser | null {
    try {
      const payload = JSON.parse(atob(jwt.split('.')[1]))
      if (!payload.sub || !payload.role) return null
      if (payload.exp && payload.exp * 1000 < Date.now()) return null
      return {
        username: payload.sub,
        role: payload.role as AuthUser['role'],
        allowed_media: payload.allowed_media ?? ['tv', 'radio', 'impresos'],
      }
    } catch {
      return null
    }
  }

  function initFromStorage(): void {
    if (token.value) {
      const decoded = decodeToken(token.value)
      if (decoded) {
        user.value = decoded
      } else {
        logout()
      }
    }
  }

  async function login(username: string, password: string): Promise<void> {
    const form = new URLSearchParams()
    form.append('username', username)
    form.append('password', password)

    const res = await axios.post<{
      access_token: string
      token_type: string
      role: string
      username: string
      allowed_media: string[]
    }>('/auth/token', form, {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    })

    token.value = res.data.access_token
    user.value = {
      username: res.data.username,
      role: res.data.role as AuthUser['role'],
      allowed_media: res.data.allowed_media ?? ['tv', 'radio', 'impresos'],
    }
    localStorage.setItem('auth_token', res.data.access_token)
  }

  async function register(payload: {
    username: string
    full_name: string
    email: string
    password: string
    account: string
  }): Promise<void> {
    await axios.post('/auth/register', payload)
  }

  function logout(): void {
    token.value = null
    user.value = null
    localStorage.removeItem('auth_token')
  }

  initFromStorage()

  return { token, user, isAuthenticated, login, register, logout }
})
