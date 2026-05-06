import { defineStore } from 'pinia'
import { ref, reactive } from 'vue'
import { getStatusRange, getTopMarcasRange, getTopSectoresRange, getRecords, dateToYYYYMMDD, type StatusRangeResponse, type TopMarcaRangeItem, type TopSectorItem, type AutocompleteFilters, type PaginatedRecordsResponse } from '../services/api'

interface TagField {
  input: string
  tags: string[]
}

export const useAuditsaStore = defineStore('auditsa', () => {
  // ── Filtros ──
  const fechaInicio = ref('')
  const fechaFin    = ref('')

  const anunciante      = reactive<TagField>({ input: '', tags: [] })
  const marca           = reactive<TagField>({ input: '', tags: [] })
  const submarca        = reactive<TagField>({ input: '', tags: [] })
  const categoria       = reactive<TagField>({ input: '', tags: [] })
  const localidad       = reactive<TagField>({ input: '', tags: [] })
  const tipoMedio       = reactive<TagField>({ input: '', tags: [] })
  const medio           = reactive<TagField>({ input: '', tags: [] })
  const sectorIndustria = reactive<TagField>({ input: '', tags: [] })
  const industria       = reactive<TagField>({ input: '', tags: [] })

  // Filtro de fuente: solo una seleccionable a la vez (TV, Radio o Impresos)
  const selectedSource  = ref<'tv' | 'radio' | 'impresos' | null>(null)

  // ── Columnas seleccionables por fuente (selección múltiple) ──
  const tvRadioColumns = reactive<Record<string, boolean>>({
    Fecha: false, Anunciante: false, Marca: false, Submarca: false, Producto: false,
    Version: false, Ins: false, Tarifa: false, Industria: false, Mercado: false,
    Segmento: false, Campania: false, Localidad: false, Medio: false, TipoMedio: false,
    Canal: false, HInicio: false, HFinal: false, FHoraria: false, TipoSpot: false,
    SubTipoSpot: false, TipoCorte: false, PEnCorte: false, NoCorte: false,
    Programa: false, Genero: false, GComercial: false, GEstacion: false, Testigo: false,
  })
  const impresosColumns = reactive<Record<string, boolean>>({
    Fecha: false, Anunciante: false, Marca: false, Submarca: false, Producto: false,
    Ins: false, Costo: false, Sector: false, Subsector: false, Categoria: false,
    Medio: false, Fuente: false, Seccion: false, Pagina: false, TextoNota: false,
  })

  // ── Resultados ──
  const loading         = ref(false)
  const tvData          = ref<StatusRangeResponse | null>(null)
  const radioData       = ref<StatusRangeResponse | null>(null)
  const impresosData    = ref<StatusRangeResponse | null>(null)
  const tvTopMarcas     = ref<TopMarcaRangeItem[]>([])
  const radioTopMarcas  = ref<TopMarcaRangeItem[]>([])
  const impresosTopMarcas = ref<TopMarcaRangeItem[]>([])
  const tvTopSectores      = ref<TopSectorItem[]>([])
  const radioTopSectores   = ref<TopSectorItem[]>([])
  const impresosTopSectores = ref<TopSectorItem[]>([])
  const searched        = ref(false)

  // Flags por-card para render progresivo (cada uno se apaga cuando su query resuelve)
  const tvLoading       = ref(false)
  const radioLoading    = ref(false)
  const impresosLoading = ref(false)

  // ── Tablas de detalle paginadas ──
  const tvRecords       = ref<PaginatedRecordsResponse | null>(null)
  const radioRecords    = ref<PaginatedRecordsResponse | null>(null)
  const impresosRecords = ref<PaginatedRecordsResponse | null>(null)
  const tvRecordsLoading       = ref(false)
  const radioRecordsLoading    = ref(false)
  const impresosRecordsLoading = ref(false)

  // Almacena los filtros usados en la última búsqueda para paginar con los mismos
  let lastFilters: AutocompleteFilters | undefined
  let lastFi = ''
  let lastFf = ''

  async function fetchRecords(
    tipo: 'tv' | 'radio' | 'impresos',
    page: number,
    pageSize = 25,
  ) {
    if (!lastFi || !lastFf) return
    const loadingRef = tipo === 'tv' ? tvRecordsLoading : tipo === 'radio' ? radioRecordsLoading : impresosRecordsLoading
    const dataRef = tipo === 'tv' ? tvRecords : tipo === 'radio' ? radioRecords : impresosRecords
    loadingRef.value = true
    try {
      dataRef.value = await getRecords(tipo, lastFi, lastFf, page, pageSize, lastFilters)
    } catch (e) {
      console.error(`[${tipo} records]`, e)
    } finally {
      loadingRef.value = false
    }
  }

  async function buscar(filters?: AutocompleteFilters) {
    if (!fechaInicio.value || !fechaFin.value) return
    loading.value = true
    searched.value = true
    tvLoading.value = radioLoading.value = impresosLoading.value = true

    const fi = dateToYYYYMMDD(fechaInicio.value)
    const ff = dateToYYYYMMDD(fechaFin.value)
    lastFi = fi
    lastFf = ff
    lastFilters = filters

    // Reset tablas de detalle
    tvRecords.value = radioRecords.value = impresosRecords.value = null
    tvRecordsLoading.value = radioRecordsLoading.value = impresosRecordsLoading.value = true

    // Render progresivo: cada fuente se pinta en cuanto sus dos queries (status + top-marcas)
    // resuelven, sin esperar al resto. Impresos (~ms) aparece casi inmediato;
    // TV/Radio (segundos) llegan después.
    const tvTask = Promise.all([
      getStatusRange('tv', fi, ff, filters),
      getTopMarcasRange('tv', fi, ff, filters),
      getTopSectoresRange('tv', fi, ff, filters),
    ]).then(([d, t, s]) => {
      tvData.value = d
      tvTopMarcas.value = t
      tvTopSectores.value = s
    }).catch(e => console.error('[tv]', e))
      .finally(() => { tvLoading.value = false })

    const radioTask = Promise.all([
      getStatusRange('radio', fi, ff, filters),
      getTopMarcasRange('radio', fi, ff, filters),
      getTopSectoresRange('radio', fi, ff, filters),
    ]).then(([d, t, s]) => {
      radioData.value = d
      radioTopMarcas.value = t
      radioTopSectores.value = s
    }).catch(e => console.error('[radio]', e))
      .finally(() => { radioLoading.value = false })

    const impTask = Promise.all([
      getStatusRange('impresos', fi, ff, filters),
      getTopMarcasRange('impresos', fi, ff, filters),
      getTopSectoresRange('impresos', fi, ff, filters),
    ]).then(([d, t, s]) => {
      impresosData.value = d
      impresosTopMarcas.value = t
      impresosTopSectores.value = s
    }).catch(e => console.error('[impresos]', e))
      .finally(() => { impresosLoading.value = false })

    // Tablas de detalle (página 1) — corren en paralelo con los stats
    const tvRec = fetchRecords('tv', 1)
    const radioRec = fetchRecords('radio', 1)
    const impRec = fetchRecords('impresos', 1)

    try {
      await Promise.allSettled([tvTask, radioTask, impTask, tvRec, radioRec, impRec])
    } finally {
      loading.value = false
    }
  }

  function clearAll() {
    fechaInicio.value = ''
    fechaFin.value    = ''
    ;[anunciante, marca, submarca, categoria, localidad, tipoMedio, medio, sectorIndustria, industria].forEach(f => {
      f.input = ''
      f.tags  = []
    })
    selectedSource.value = null
    Object.keys(tvRadioColumns).forEach(k => tvRadioColumns[k] = false)
    Object.keys(impresosColumns).forEach(k => impresosColumns[k] = false)
    tvData.value = radioData.value = impresosData.value = null
    tvTopMarcas.value = radioTopMarcas.value = impresosTopMarcas.value = []
    tvTopSectores.value = radioTopSectores.value = impresosTopSectores.value = []
    tvRecords.value = radioRecords.value = impresosRecords.value = null
    searched.value = false
    tvLoading.value = radioLoading.value = impresosLoading.value = false
    tvRecordsLoading.value = radioRecordsLoading.value = impresosRecordsLoading.value = false
  }

  return {
    fechaInicio, fechaFin,
    anunciante, marca, submarca, categoria, localidad, tipoMedio, medio, sectorIndustria, industria,
    selectedSource, tvRadioColumns, impresosColumns,
    loading, tvData, radioData, impresosData,
    tvTopMarcas, radioTopMarcas, impresosTopMarcas,
    searched,
    tvLoading, radioLoading, impresosLoading,
    tvRecords, radioRecords, impresosRecords,
    tvRecordsLoading, radioRecordsLoading, impresosRecordsLoading,
    tvTopSectores, radioTopSectores, impresosTopSectores,
    fetchRecords,
    buscar, clearAll,
  }
})
