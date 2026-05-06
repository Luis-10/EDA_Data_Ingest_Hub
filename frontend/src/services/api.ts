import axios from 'axios'
import { useAuthStore } from '../stores/auth'

function getAuthToken(): string | null {
  // Lee directamente de localStorage para evitar dependencia circular en el interceptor
  return localStorage.getItem('auth_token')
}

// Interceptor: adjuntar Bearer token del usuario autenticado a cada petición /api/*
axios.interceptors.request.use((config) => {
  if (config.url?.startsWith('/api/')) {
    const token = getAuthToken()
    if (token) {
      config.headers.Authorization = `Bearer ${token}`
    }
  }
  return config
})

// Si recibimos 401, hacer logout y redirigir al login
axios.interceptors.response.use(
  (res) => res,
  (error) => {
    if (error.response?.status === 401 && error.config?.url?.startsWith('/api/')) {
      try {
        const auth = useAuthStore()
        auth.logout()
        window.location.href = '/login'
      } catch {
        localStorage.removeItem('auth_token')
        window.location.href = '/login'
      }
    }
    return Promise.reject(error)
  },
)

export interface GeneralStatsResponse {
  tipo: string
  total_records: number
  unique_dates: number
  unique_channels?: number
  unique_sources?: number
  unique_brands: number
  unique_advertisers: number
  first_date: string | null
  last_date: string | null
  total_tarifa?: number
  total_costo?: number
}

export interface StatusRangeResponse {
  status: string
  fecha_inicio: string
  fecha_fin: string
  total_dates_in_range: number
  data_exists: boolean
  statistics: {
    total_records: number
    unique_dates: number
    unique_channels?: number
    unique_sources?: number
    unique_brands: number
    unique_advertisers: number
    total_tarifa?: number
    total_costo?: number
  }
}

export interface TopMarcaRangeItem {
  anunciante: string
  marca: string
  registros: number
  total_inversion: number
}

export interface ImpresosRecord {
  id: number
  Fuente: string
  Medio: string
  Anunciante: string
  Marca: string
  Sector: string
  Categoria: string
  Fecha: string
  Costo: number
  Tiraje: number
  Dimension: number
}

export interface ImpresosRecordsResponse {
  summary: {
    total_records: number
    unique_dates: number
    first_date: string | null
    last_date: string | null
    total_costo: number
    avg_costo: number
    medios: { medio: string; count: number }[]
  }
  pagination: {
    page: number
    per_page: number
    total: number
    total_pages: number
  }
  records: ImpresosRecord[]
}

export function dateToYYYYMMDD(date: string): string {
  return date.replace(/-/g, '')
}

export interface AutocompleteFilters {
  anunciante?: string
  marca?: string
  submarca?: string
  medio?: string
  tipo_medio?: string
  localidad?: string
  categoria?: string
  fuente?: string
  sector?: string
  sector_industria?: string
  industria?: string
  fecha_inicio?: string
  fecha_fin?: string
}

export async function getAutocomplete(
  field: 'Anunciante' | 'Marca' | 'Submarca' | 'Categoria' | 'Localidad' | 'TipoMedio' | 'Medio' | 'Fuente' | 'Sector' | 'SectorIndustria' | 'Industria',
  q: string,
  limit = 10,
  filters?: AutocompleteFilters,
  signal?: AbortSignal,
): Promise<string[]> {
  const response = await axios.get<{ field: string; suggestions: string[] }>('/api/autocomplete', {
    params: { field, q, limit, ...filters },
    signal,
  })
  return response.data.suggestions
}

export async function getAccounts(): Promise<string[]> {
  const response = await axios.get<{ accounts: string[] }>('/auth/accounts')
  return response.data.accounts
}

export async function getGeneralStats(
  tipo: 'tv' | 'radio' | 'impresos'
): Promise<GeneralStatsResponse> {
  const response = await axios.get<GeneralStatsResponse>(`/api/${tipo}/stats`)
  return response.data
}

export async function getStatusRange(
  tipo: 'tv' | 'radio' | 'impresos',
  fechaInicio: string,
  fechaFin: string,
  filters?: AutocompleteFilters,
): Promise<StatusRangeResponse> {
  const response = await axios.get<StatusRangeResponse>(`/api/${tipo}/status/range`, {
    params: {
      fecha_inicio: fechaInicio,
      fecha_fin: fechaFin,
      ...filters,
    },
  })
  return response.data
}

export interface ImpresosTopMarca {
  anunciante?: string
  marca: string
  registros: number
  total_costo: number
  total_inversion?: number
}

export async function getImpresosTopMarcas(): Promise<ImpresosTopMarca[]> {
  const response = await axios.get<ImpresosTopMarca[]>('/api/impresos/top-marcas')
  return response.data
}

export async function getTopMarcasAllTime(
  tipo: 'tv' | 'radio' | 'impresos',
): Promise<TopMarcaRangeItem[]> {
  const response = await axios.get<TopMarcaRangeItem[]>(`/api/${tipo}/top-marcas`)
  return response.data
}

export async function getImpresosRecords(params: {
  fecha_inicio?: string
  fecha_fin?: string
  medio?: string
  fuente?: string
  anunciante?: string
  marca?: string
  sector?: string
  page?: number
  per_page?: number
}): Promise<ImpresosRecordsResponse> {
  const response = await axios.get<ImpresosRecordsResponse>('/api/impresos/records', { params })
  return response.data
}

export async function getTopMarcasRange(
  tipo: 'tv' | 'radio' | 'impresos',
  fechaInicio: string,
  fechaFin: string,
  filters?: AutocompleteFilters,
): Promise<TopMarcaRangeItem[]> {
  const response = await axios.get<TopMarcaRangeItem[]>(`/api/${tipo}/top-marcas/range`, {
    params: { fecha_inicio: fechaInicio, fecha_fin: fechaFin, ...filters },
  })
  return response.data
}

// ── Export ──

export interface ExportResponse {
  tipo: string
  total_available: number
  total_exported: number
  truncated: boolean
  columns: string[]
  records: Record<string, unknown>[]
}

export async function getExportRecords(
  tipo: 'tv' | 'radio' | 'impresos',
  fechaInicio: string,
  fechaFin: string,
  columns: string[],
  filters?: AutocompleteFilters,
): Promise<ExportResponse> {
  const response = await axios.get<ExportResponse>(`/api/export/${tipo}`, {
    params: {
      fecha_inicio: fechaInicio,
      fecha_fin: fechaFin,
      columns: columns.join(','),
      ...filters,
    },
  })
  return response.data
}

// ── Top sectores por inversión ──

export interface TopSectorItem {
  sector: string
  registros: number
  total_inversion: number
}

export async function getTopSectores(
  tipo: 'tv' | 'radio' | 'impresos',
  limit = 10,
): Promise<TopSectorItem[]> {
  const response = await axios.get<TopSectorItem[]>(`/api/${tipo}/top-sectores`, {
    params: { limit },
  })
  return response.data
}

export interface StatsSummaryMedia {
  stats: GeneralStatsResponse
  top_marcas: TopMarcaRangeItem[]
  top_sectores: TopSectorItem[]
}

export interface StatsSummary {
  tv?: StatsSummaryMedia
  radio?: StatsSummaryMedia
  impresos?: StatsSummaryMedia
}

export async function getStatsSummary(): Promise<StatsSummary> {
  const response = await axios.get<StatsSummary>('/api/stats/summary')
  return response.data
}

export async function getTopSectoresRange(
  tipo: 'tv' | 'radio' | 'impresos',
  fechaInicio: string,
  fechaFin: string,
  filters?: AutocompleteFilters,
  limit = 10,
): Promise<TopSectorItem[]> {
  const response = await axios.get<TopSectorItem[]>(`/api/${tipo}/top-sectores/range`, {
    params: { fecha_inicio: fechaInicio, fecha_fin: fechaFin, limit, ...filters },
  })
  return response.data
}

// ── Registros paginados para tablas de detalle ──
export interface PaginatedRecordsResponse {
  tipo: string
  columns: string[]
  records: Record<string, unknown>[]
  total: number
  page: number
  page_size: number
  pages: number
}

export async function getRecords(
  tipo: 'tv' | 'radio' | 'impresos',
  fechaInicio: string,
  fechaFin: string,
  page: number,
  pageSize: number,
  filters?: AutocompleteFilters,
): Promise<PaginatedRecordsResponse> {
  const response = await axios.get<PaginatedRecordsResponse>(`/api/records/${tipo}`, {
    params: {
      fecha_inicio: fechaInicio,
      fecha_fin: fechaFin,
      page,
      page_size: pageSize,
      ...filters,
    },
  })
  return response.data
}
