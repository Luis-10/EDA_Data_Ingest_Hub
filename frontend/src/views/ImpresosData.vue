<script setup lang="ts">
import { reactive, computed, onMounted } from 'vue'
import { storeToRefs } from 'pinia'
import { getAutocomplete } from '../services/api'
import { useStatsStore } from '../stores/stats'
import { useImpresosStore } from '../stores/impresos'

type AcField = 'Medio' | 'Fuente' | 'Anunciante' | 'Marca' | 'Sector'
interface TagField { input: string; tags: string[] }

// ── Stores ──
const statsStore = useStatsStore()
const store      = useImpresosStore()

const { impresosStats, topMarcas, loading: statsLoading } = storeToRefs(statsStore)
const { fechaInicio, fechaFin, perPage,
        checkAnunciante, checkMarca, checkSector,
        loading, data, searched, currentPage } = storeToRefs(store)

const { medio, fuente, anunciante, marca, sector } = store

onMounted(() => statsStore.loadAll())

// ── Autocomplete (estado local efímero) ──
const suggestions = reactive<Record<AcField, string[]>>({
  Medio: [], Fuente: [], Anunciante: [], Marca: [], Sector: [],
})
const acVisible = reactive<Record<AcField, boolean>>({
  Medio: false, Fuente: false, Anunciante: false, Marca: false, Sector: false,
})
const debounceTimers: Record<string, ReturnType<typeof setTimeout>> = {}

async function onAcInput(apiField: AcField, field: TagField) {
  clearTimeout(debounceTimers[apiField])
  const q = field.input.trim()
  if (!q) { suggestions[apiField] = []; acVisible[apiField] = false; return }
  debounceTimers[apiField] = setTimeout(async () => {
    try {
      const results = await getAutocomplete(apiField, q)
      suggestions[apiField] = results.filter(s => !field.tags.includes(s))
      acVisible[apiField] = suggestions[apiField].length > 0
    } catch { suggestions[apiField] = [] }
  }, 220)
}

function selectSuggestion(apiField: AcField, field: TagField, value: string) {
  if (!field.tags.includes(value)) field.tags.push(value)
  field.input = ''
  suggestions[apiField] = []
  acVisible[apiField] = false
}

function hideAc(apiField: AcField) {
  setTimeout(() => { acVisible[apiField] = false }, 150)
}

function addTag(field: TagField) {
  const val = field.input.trim()
  if (val && !field.tags.includes(val)) field.tags.push(val)
  field.input = ''
}

function removeTag(field: TagField, i: number) { field.tags.splice(i, 1) }
function onEnter(e: KeyboardEvent, field: TagField) { e.preventDefault(); addTag(field) }

const buscar   = (page = 1) => store.buscar(page)
const clearAll = () => store.clearAll()

// ── Computed display ──
const summaryTotal  = computed(() => searched.value ? (data.value?.summary.total_records ?? 0) : (impresosStats.value?.total_records ?? 0))
const summaryDates  = computed(() => searched.value ? (data.value?.summary.unique_dates ?? 0)  : (impresosStats.value?.unique_dates ?? 0))
const summaryFirst  = computed(() => searched.value ? (data.value?.summary.first_date ?? null)  : (impresosStats.value?.first_date ?? null))
const summaryLast   = computed(() => searched.value ? (data.value?.summary.last_date ?? null)   : (impresosStats.value?.last_date ?? null))
const summaryCosto  = computed(() => searched.value ? (data.value?.summary.total_costo ?? 0)    : (impresosStats.value?.total_costo ?? 0))
const summaryAvg    = computed(() => data.value?.summary.avg_costo ?? 0)
const summaryMedios = computed(() => data.value?.summary.medios ?? [])

const fmtMoney = (v: number) =>
  '$' + v.toLocaleString('es-MX', { minimumFractionDigits: 2, maximumFractionDigits: 2 })

const fmtDate = (d: string | null) => {
  if (!d) return '—'
  const [y, m, day] = d.split('-')
  return `${day}/${m}/${y}`
}

const totalPages = computed(() => data.value?.pagination.total_pages ?? 0)

function goPage(p: number) {
  if (p < 1 || p > totalPages.value) return
  buscar(p)
}
</script>

<template>
  <div class="imp">

    <!-- Header -->
    <div class="imp-header">
      <span class="imp-header-icon">📰</span>
      <h1 class="imp-header-title">Print Data</h1>
    </div>

    <!-- Filter card -->
    <div class="card">
      <div class="filter-header">
        <span class="filter-title">🔍 Filtros de Búsqueda</span>
        <button class="btn-clear" @click="clearAll">✕ Limpiar</button>
      </div>

      <!-- Row 1: fechas + Medio + Fuente -->
      <div class="filter-grid">
        <div class="field">
          <label class="label">Fecha Inicio</label>
          <input type="date" v-model="fechaInicio" class="input" />
        </div>
        <div class="field">
          <label class="label">Fecha Fin</label>
          <input type="date" v-model="fechaFin" class="input" />
        </div>

        <!-- Medio -->
        <div class="field">
          <label class="label">Medio <span class="label-count">({{ medio.tags.length }} opciones)</span></label>
          <div class="ac-wrap">
            <div class="tag-input-wrap">
              <span v-for="(t,i) in medio.tags" :key="i" class="tag">
                {{ t }} <button @click="removeTag(medio,i)" class="tag-remove">×</button>
              </span>
              <input
                v-model="medio.input"
                @input="onAcInput('Medio', medio)"
                @keydown.enter="onEnter($event, medio)"
                @blur="hideAc('Medio')"
                placeholder="Buscar medio..."
                class="tag-input"
              />
            </div>
            <ul v-if="acVisible.Medio" class="ac-dropdown">
              <li v-for="s in suggestions.Medio" :key="s" @mousedown.prevent="selectSuggestion('Medio', medio, s)">{{ s }}</li>
            </ul>
          </div>
        </div>

        <!-- Fuente -->
        <div class="field">
          <label class="label">Fuente <span class="label-count">({{ fuente.tags.length }} opciones)</span></label>
          <div class="ac-wrap">
            <div class="tag-input-wrap">
              <span v-for="(t,i) in fuente.tags" :key="i" class="tag">
                {{ t }} <button @click="removeTag(fuente,i)" class="tag-remove">×</button>
              </span>
              <input
                v-model="fuente.input"
                @input="onAcInput('Fuente', fuente)"
                @keydown.enter="onEnter($event, fuente)"
                @blur="hideAc('Fuente')"
                placeholder="Buscar fuente..."
                class="tag-input"
              />
            </div>
            <ul v-if="acVisible.Fuente" class="ac-dropdown">
              <li v-for="s in suggestions.Fuente" :key="s" @mousedown.prevent="selectSuggestion('Fuente', fuente, s)">{{ s }}</li>
            </ul>
          </div>
        </div>
      </div>

      <!-- Row 2: Anunciante + Marca + Sector + Per page -->
      <div class="filter-grid" style="margin-top:14px">

        <!-- Anunciante -->
        <div class="field">
          <label class="label">Anunciante <span class="label-count">({{ anunciante.tags.length }} opciones)</span></label>
          <div class="ac-wrap">
            <div class="tag-input-wrap">
              <span v-for="(t,i) in anunciante.tags" :key="i" class="tag">
                {{ t }} <button @click="removeTag(anunciante,i)" class="tag-remove">×</button>
              </span>
              <input
                v-model="anunciante.input"
                @input="onAcInput('Anunciante', anunciante)"
                @keydown.enter="onEnter($event, anunciante)"
                @blur="hideAc('Anunciante')"
                placeholder="Buscar y presiona Enter..."
                class="tag-input"
              />
            </div>
            <ul v-if="acVisible.Anunciante" class="ac-dropdown">
              <li v-for="s in suggestions.Anunciante" :key="s" @mousedown.prevent="selectSuggestion('Anunciante', anunciante, s)">{{ s }}</li>
            </ul>
          </div>
        </div>

        <!-- Marca -->
        <div class="field">
          <label class="label">Marca <span class="label-count">({{ marca.tags.length }} opciones)</span></label>
          <div class="ac-wrap">
            <div class="tag-input-wrap">
              <span v-for="(t,i) in marca.tags" :key="i" class="tag">
                {{ t }} <button @click="removeTag(marca,i)" class="tag-remove">×</button>
              </span>
              <input
                v-model="marca.input"
                @input="onAcInput('Marca', marca)"
                @keydown.enter="onEnter($event, marca)"
                @blur="hideAc('Marca')"
                placeholder="Buscar y presiona Enter..."
                class="tag-input"
              />
            </div>
            <ul v-if="acVisible.Marca" class="ac-dropdown">
              <li v-for="s in suggestions.Marca" :key="s" @mousedown.prevent="selectSuggestion('Marca', marca, s)">{{ s }}</li>
            </ul>
          </div>
        </div>

        <!-- Sector -->
        <div class="field">
          <label class="label">Sector <span class="label-count">({{ sector.tags.length }} opciones)</span></label>
          <div class="ac-wrap">
            <div class="tag-input-wrap">
              <span v-for="(t,i) in sector.tags" :key="i" class="tag">
                {{ t }} <button @click="removeTag(sector,i)" class="tag-remove">×</button>
              </span>
              <input
                v-model="sector.input"
                @input="onAcInput('Sector', sector)"
                @keydown.enter="onEnter($event, sector)"
                @blur="hideAc('Sector')"
                placeholder="Buscar y presiona Enter..."
                class="tag-input"
              />
            </div>
            <ul v-if="acVisible.Sector" class="ac-dropdown">
              <li v-for="s in suggestions.Sector" :key="s" @mousedown.prevent="selectSuggestion('Sector', sector, s)">{{ s }}</li>
            </ul>
          </div>
        </div>

        <!-- Per page -->
        <div class="field">
          <label class="label">Registros por página</label>
          <select v-model="perPage" class="input">
            <option :value="25">25</option>
            <option :value="50">50</option>
            <option :value="100">100</option>
            <option :value="200">200</option>
            <option :value="500">500</option>
          </select>
        </div>
      </div>

      <!-- Actions -->
      <div class="filter-actions">
        <button class="btn-buscar" :disabled="loading" @click="buscar(1)">
          <span v-if="loading" class="spinner"></span>
          🔍 Buscar
        </button>

        <div class="checks">
          <label class="check-label"><input type="checkbox" v-model="checkAnunciante" /> Anunciante</label>
          <label class="check-label"><input type="checkbox" v-model="checkMarca" /> Marca</label>
          <label class="check-label"><input type="checkbox" v-model="checkSector" /> Sector</label>
        </div>

        <button class="btn-export">📥 Exportar</button>
      </div>
    </div>

    <!-- Summary cards -->
    <div class="cards-grid">

      <!-- Card 1: Total registros -->
      <div class="stat-card stat-card--red">
        <div class="card-inner">
          <span class="card-big-icon">📰</span>
          <div>
            <p class="card-label">IMPRESOS</p>
            <template v-if="statsLoading && !searched">
              <div class="skel skel--value"></div>
              <div class="skel skel--sub"></div>
              <div class="skel skel--dates"></div>
            </template>
            <template v-else>
              <p class="card-value">{{ summaryTotal.toLocaleString() }}</p>
              <p class="card-sub">Registros</p>
              <p class="card-accent">{{ summaryDates }} fechas</p>
            </template>
          </div>
        </div>
      </div>

      <!-- Card 2: Rango de fechas -->
      <div class="stat-card stat-card--blue">
        <div class="card-inner">
          <span class="card-big-icon">📅</span>
          <div>
            <p class="card-label">RANGO DE FECHAS</p>
            <template v-if="statsLoading && !searched">
              <div class="skel skel--date"></div>
              <div class="skel skel--sub" style="margin:4px 0"></div>
              <div class="skel skel--date"></div>
            </template>
            <template v-else>
              <p class="card-date-main">{{ fmtDate(summaryFirst) }}</p>
              <p class="card-date-until">hasta</p>
              <p class="card-date-main">{{ fmtDate(summaryLast) }}</p>
            </template>
          </div>
        </div>
      </div>

      <!-- Card 3: Costo total -->
      <div class="stat-card stat-card--green">
        <div class="card-inner">
          <span class="card-big-icon">💰</span>
          <div>
            <p class="card-label">COSTO TOTAL</p>
            <template v-if="statsLoading && !searched">
              <div class="skel skel--value-sm"></div>
              <div class="skel skel--sub"></div>
            </template>
            <template v-else>
              <p class="card-value card-value--sm">{{ fmtMoney(summaryCosto) }}</p>
              <p class="card-sub">Total invertido</p>
              <p v-if="searched && summaryAvg > 0" class="card-accent">Promedio: {{ fmtMoney(summaryAvg) }}</p>
            </template>
          </div>
        </div>
      </div>

      <!-- Card 4: Medios breakdown -->
      <div class="stat-card stat-card--orange">
        <div class="card-inner card-inner--medios">
          <span class="card-big-icon">📊</span>
          <div class="medios-list">
            <p class="card-label">MEDIOS</p>
            <template v-if="summaryMedios.length > 0">
              <div v-for="m in summaryMedios.slice(0,5)" :key="m.medio" class="medio-row">
                <span class="medio-bar" :style="{ width: Math.round((m.count / (data?.summary.total_records || 1)) * 100) + '%' }"></span>
                <span class="medio-name">{{ m.medio }}</span>
                <span class="medio-count">{{ m.count.toLocaleString() }}</span>
              </div>
            </template>
            <p v-else class="card-sub">—</p>
          </div>
        </div>
      </div>
    </div>

    <!-- Table -->
    <div class="card" style="padding:0; overflow:hidden;">

      <!-- Top 10 table (no search applied) -->
      <template v-if="!searched">
        <div class="table-banner">
          🏆 Top 10 Marcas — Mayor Inversión
        </div>
        <table class="data-table">
          <thead>
            <tr>
              <th class="text-center">#</th>
              <th>Marca</th>
              <th class="text-right">Costo Total</th>
              <th class="text-right">Registros</th>
              <th class="text-center">Estado</th>
            </tr>
          </thead>
          <tbody>
            <tr v-if="statsLoading">
              <td v-for="n in 5" :key="n" style="padding:13px 18px">
                <div class="skel" :style="{ height: '14px', width: n===1 ? '24px' : n===3 ? '80px' : n===4 ? '40px' : '60%' }"></div>
              </td>
            </tr>
            <tr v-else-if="topMarcas.length === 0">
              <td colspan="5" class="td-empty">Sin datos disponibles</td>
            </tr>
            <tr v-for="(row, i) in topMarcas" :key="row.marca" v-else>
              <td class="text-center td-rank">{{ (i as number) + 1 }}</td>
              <td class="td-strong">{{ row.marca }}</td>
              <td class="text-right td-strong">{{ fmtMoney(row.total_costo) }}</td>
              <td class="text-right td-muted">{{ row.registros.toLocaleString() }}</td>
              <td class="text-center">
                <span class="badge badge--green">CON DATOS</span>
              </td>
            </tr>
          </tbody>
        </table>
      </template>

      <!-- Filtered results table -->
      <template v-else>
        <table class="data-table">
          <thead>
            <tr>
              <th>Fuente</th>
              <th>Marca</th>
              <th v-if="checkAnunciante">Anunciante</th>
              <th v-if="checkSector">Sector</th>
              <th>Fecha</th>
              <th class="text-right">Costo</th>
              <th class="text-center">Estado</th>
            </tr>
          </thead>
          <tbody>
            <tr v-if="loading">
              <td colspan="7" class="td-empty">
                <span class="spinner spinner--dark"></span> Cargando...
              </td>
            </tr>
            <tr v-else-if="data?.records.length === 0">
              <td colspan="7" class="td-empty">Sin resultados para los filtros aplicados</td>
            </tr>
            <tr v-for="row in data?.records" :key="row.id" v-else>
              <td class="td-fuente"><span>📰</span> {{ row.Fuente || row.Medio || 'Impresos' }}</td>
              <td class="td-strong">{{ row.Marca || '—' }}</td>
              <td v-if="checkAnunciante" class="td-muted">{{ row.Anunciante || '—' }}</td>
              <td v-if="checkSector" class="td-muted">{{ row.Sector || '—' }}</td>
              <td class="td-muted">{{ fmtDate(row.Fecha) }}</td>
              <td class="text-right td-strong">{{ fmtMoney(row.Costo) }}</td>
              <td class="text-center">
                <span class="badge badge--green">CON DATOS</span>
              </td>
            </tr>
          </tbody>
        </table>

        <!-- Pagination -->
        <div v-if="totalPages > 1" class="pagination">
          <button class="page-btn" :disabled="currentPage === 1" @click="goPage(currentPage - 1)">‹ Anterior</button>
          <span class="page-info">Página {{ currentPage }} de {{ totalPages }} · {{ data?.pagination.total.toLocaleString() }} registros</span>
          <button class="page-btn" :disabled="currentPage === totalPages" @click="goPage(currentPage + 1)">Siguiente ›</button>
        </div>
      </template>

    </div>

  </div>
</template>

<style scoped>
.imp {
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 20px;
  min-height: 100%;
  background: #f1f5f9;
}

/* ── Header ── */
.imp-header {
  background: #f97316;
  padding: 28px 32px;
  display: flex;
  align-items: center;
  gap: 14px;
}
.imp-header-icon { font-size: 28px; }
.imp-header-title {
  color: #fff;
  font-size: 26px;
  font-weight: 700;
  margin: 0;
  letter-spacing: -0.4px;
}

/* ── Card base ── */
.card {
  background: #fff;
  border-radius: 12px;
  box-shadow: 0 1px 4px rgba(0,0,0,0.07);
  padding: 20px;
  margin: 0 24px;
}

/* ── Filter header ── */
.filter-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}
.filter-title { font-size: 15px; font-weight: 600; color: #374151; }
.btn-clear {
  background: none; border: 1px solid #e2e8f0; border-radius: 6px;
  padding: 5px 12px; font-size: 12.5px; color: #64748b; cursor: pointer;
  display: flex; align-items: center; gap: 5px;
}
.btn-clear:hover { border-color: #94a3b8; color: #374151; }

/* ── Grid ── */
.filter-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 14px;
}
.field { display: flex; flex-direction: column; gap: 5px; }

.label {
  font-size: 11px; font-weight: 600; color: #64748b;
  text-transform: uppercase; letter-spacing: 0.5px;
}
.label-count {
  font-size: 10.5px; color: #94a3b8; font-weight: 400;
  text-transform: none; letter-spacing: 0;
}

.input {
  border: 1px solid #e2e8f0; border-radius: 8px;
  padding: 8px 12px; font-size: 13px; color: #374151;
  outline: none; background: #fff; width: 100%;
  box-sizing: border-box; font-family: inherit;
}
.input:focus { border-color: #f97316; box-shadow: 0 0 0 3px rgba(249,115,22,0.12); }

/* ── Tag input ── */
.tag-input-wrap {
  border: 1px solid #e2e8f0; border-radius: 8px;
  padding: 5px 8px; min-height: 38px;
  display: flex; flex-wrap: wrap; gap: 5px;
  align-items: center; background: #fff; cursor: text;
}
.tag-input-wrap:focus-within { border-color: #f97316; box-shadow: 0 0 0 3px rgba(249,115,22,0.12); }
.tag {
  background: #fff7ed; color: #c2410c; font-size: 11.5px;
  padding: 2px 8px; border-radius: 20px;
  display: flex; align-items: center; gap: 4px;
}
.tag-remove { background: none; border: none; color: #c2410c; cursor: pointer; padding: 0; font-size: 14px; line-height: 1; }
.tag-input {
  flex: 1; min-width: 60px; border: none; outline: none;
  font-size: 12.5px; color: #374151; background: transparent;
  font-family: inherit; padding: 2px 0;
}
.tag-input::placeholder { color: #94a3b8; }

/* ── Autocomplete ── */
.ac-wrap { position: relative; }
.ac-dropdown {
  position: absolute; top: calc(100% + 2px); left: 0; right: 0;
  z-index: 200; background: #fff;
  border: 1px solid #e2e8f0; border-radius: 8px;
  box-shadow: 0 4px 16px rgba(0,0,0,0.10);
  margin: 0; padding: 4px 0; list-style: none;
  max-height: 200px; overflow-y: auto;
}
.ac-dropdown li {
  padding: 8px 12px; font-size: 13px; color: #374151;
  cursor: pointer; white-space: nowrap; overflow: hidden;
  text-overflow: ellipsis; transition: background 0.1s;
}
.ac-dropdown li:hover { background: #fff7ed; color: #c2410c; }

/* ── Actions ── */
.filter-actions {
  display: flex; align-items: center; gap: 20px;
  margin-top: 16px; flex-wrap: wrap;
}
.btn-buscar {
  background: #f97316; color: #fff; border: none;
  border-radius: 8px; padding: 10px 24px;
  font-size: 14px; font-weight: 600; cursor: pointer;
  display: flex; align-items: center; gap: 8px;
  font-family: inherit; transition: background 0.15s;
}
.btn-buscar:hover:not(:disabled) { background: #ea6c0a; }
.btn-buscar:disabled { opacity: 0.5; cursor: not-allowed; }
.spinner {
  width: 14px; height: 14px;
  border: 2px solid rgba(255,255,255,0.4);
  border-top-color: #fff; border-radius: 50%;
  animation: spin 0.7s linear infinite; display: inline-block;
}
.spinner--dark { border-color: rgba(0,0,0,0.15); border-top-color: #f97316; }
@keyframes spin { to { transform: rotate(360deg); } }

.checks { display: flex; align-items: center; gap: 16px; }
.check-label { display: flex; align-items: center; gap: 6px; font-size: 13px; color: #4b5563; cursor: pointer; }

.btn-export {
  margin-left: auto; background: #1e293b; color: #fff; border: none;
  border-radius: 8px; padding: 10px 20px; font-size: 13.5px;
  font-weight: 600; cursor: pointer; font-family: inherit; transition: background 0.15s;
}
.btn-export:hover { background: #0f172a; }

/* ── Summary cards ── */
.cards-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 16px;
  margin: 0 24px;
}

.stat-card {
  background: #fff;
  border-radius: 12px;
  box-shadow: 0 1px 4px rgba(0,0,0,0.07);
  padding: 20px;
  border-left: 4px solid transparent;
  transition: transform 0.18s ease, box-shadow 0.18s ease;
  cursor: default;
}
.stat-card:hover { transform: translateY(-4px); box-shadow: 0 8px 24px rgba(0,0,0,0.11); }
.stat-card--red    { border-left-color: #ef4444; }
.stat-card--blue   { border-left-color: #3b82f6; }
.stat-card--green  { border-left-color: #22c55e; }
.stat-card--orange { border-left-color: #f97316; }

.card-inner {
  display: flex;
  align-items: flex-start;
  gap: 14px;
}
.card-inner--medios { align-items: flex-start; }

.card-big-icon { font-size: 32px; opacity: 0.65; flex-shrink: 0; }

.card-label {
  font-size: 10.5px; font-weight: 700; color: #64748b;
  text-transform: uppercase; letter-spacing: 0.6px; margin: 0 0 6px;
}
.card-value {
  font-size: 28px; font-weight: 700; color: #1e293b; margin: 0 0 2px; line-height: 1.1;
}
.card-value--sm { font-size: 18px; }
.card-sub  { font-size: 12.5px; color: #64748b; margin: 0 0 4px; }
.card-accent { font-size: 12px; color: #f97316; margin: 0; }

.card-date-main  { font-size: 20px; font-weight: 700; color: #1e293b; margin: 2px 0; line-height: 1.2; }
.card-date-until { font-size: 11px; color: #94a3b8; margin: 0; }

/* Medios breakdown */
.medios-list { flex: 1; }
.medio-row {
  display: flex; align-items: center; gap: 6px;
  margin-bottom: 6px; font-size: 12px; color: #374151; position: relative;
}
.medio-bar {
  display: inline-block; height: 6px; background: #f97316;
  border-radius: 3px; min-width: 4px; max-width: 80px;
}
.medio-name { flex: 1; font-size: 11.5px; font-weight: 600; color: #374151; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.medio-count { font-size: 11px; color: #64748b; flex-shrink: 0; }

/* ── Table banner ── */
.table-banner {
  padding: 12px 20px;
  font-size: 13.5px;
  font-weight: 600;
  color: #374151;
  background: #fff7ed;
  border-bottom: 1px solid #fed7aa;
  letter-spacing: 0.2px;
}

/* ── Table ── */
.data-table { width: 100%; border-collapse: collapse; font-size: 13.5px; }
.data-table thead tr { background: #1e293b; }
.data-table thead th {
  padding: 13px 18px; font-size: 11.5px; font-weight: 600;
  color: #fff; text-transform: uppercase; letter-spacing: 0.5px; text-align: left;
}
.data-table thead th.text-right  { text-align: right; }
.data-table thead th.text-center { text-align: center; }
.data-table tbody tr { border-bottom: 1px solid #f1f5f9; transition: background 0.12s; }
.data-table tbody tr:last-child  { border-bottom: none; }
.data-table tbody tr:hover { background: #f8fafc; }
.data-table tbody td { padding: 13px 18px; }

.td-rank { font-size: 13px; font-weight: 700; color: #f97316; }
.td-fuente { display: flex; align-items: center; gap: 8px; font-weight: 600; color: #1e293b; }
.td-strong { font-weight: 600; color: #1e293b; }
.td-muted  { color: #64748b; }
.td-empty  { text-align: center; padding: 40px 18px; color: #94a3b8; font-size: 14px; }
.text-right  { text-align: right; }
.text-center { text-align: center; }

.badge { display: inline-flex; align-items: center; padding: 3px 10px; border-radius: 20px; font-size: 11.5px; font-weight: 600; }
.badge--green { background: #dcfce7; color: #15803d; }

/* ── Pagination ── */
.pagination {
  display: flex; align-items: center; justify-content: space-between;
  padding: 14px 18px; border-top: 1px solid #f1f5f9;
}
.page-btn {
  background: #f1f5f9; border: none; border-radius: 6px;
  padding: 7px 16px; font-size: 13px; cursor: pointer; color: #374151;
  font-family: inherit; transition: background 0.12s;
}
.page-btn:hover:not(:disabled) { background: #e2e8f0; }
.page-btn:disabled { opacity: 0.4; cursor: not-allowed; }
.page-info { font-size: 13px; color: #64748b; }

/* ── Skeleton loading ── */
@keyframes shimmer {
  0%   { background-position: -400px 0; }
  100% { background-position: 400px 0; }
}
.skel {
  border-radius: 6px;
  background: linear-gradient(90deg, #e2e8f0 25%, #f1f5f9 50%, #e2e8f0 75%);
  background-size: 800px 100%;
  animation: shimmer 1.4s infinite linear;
}
.skel--value    { height: 32px; width: 75%; margin: 4px 0 6px; }
.skel--value-sm { height: 22px; width: 80%; margin: 4px 0 6px; }
.skel--sub      { height: 13px; width: 50%; margin-bottom: 5px; }
.skel--dates    { height: 12px; width: 40%; }
.skel--date     { height: 24px; width: 65%; margin: 2px 0; }
</style>
