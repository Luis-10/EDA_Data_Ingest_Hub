import { defineStore } from 'pinia'
import { ref, reactive } from 'vue'
import { getImpresosRecords, dateToYYYYMMDD, type ImpresosRecordsResponse } from '../services/api'

interface TagField {
  input: string
  tags: string[]
}

export const useImpresosStore = defineStore('impresos', () => {
  // ── Filtros ──
  const fechaInicio = ref('')
  const fechaFin    = ref('')
  const perPage     = ref(50)

  const medio      = reactive<TagField>({ input: '', tags: [] })
  const fuente     = reactive<TagField>({ input: '', tags: [] })
  const anunciante = reactive<TagField>({ input: '', tags: [] })
  const marca      = reactive<TagField>({ input: '', tags: [] })
  const sector     = reactive<TagField>({ input: '', tags: [] })

  const checkAnunciante = ref(false)
  const checkMarca      = ref(false)
  const checkSector     = ref(false)

  // ── Resultados ──
  const loading     = ref(false)
  const data        = ref<ImpresosRecordsResponse | null>(null)
  const searched    = ref(false)
  const currentPage = ref(1)

  async function buscar(page = 1) {
    loading.value     = true
    currentPage.value = page
    try {
      const params: Record<string, string | number> = { page, per_page: perPage.value }
      if (fechaInicio.value)      params.fecha_inicio = dateToYYYYMMDD(fechaInicio.value)
      if (fechaFin.value)         params.fecha_fin    = dateToYYYYMMDD(fechaFin.value)
      if (medio.tags.length)      params.medio        = medio.tags.join(',')
      if (fuente.tags.length)     params.fuente       = fuente.tags.join(',')
      if (anunciante.tags.length) params.anunciante   = anunciante.tags.join(',')
      if (marca.tags.length)      params.marca        = marca.tags.join(',')
      if (sector.tags.length)     params.sector       = sector.tags.join(',')

      data.value     = await getImpresosRecords(params as Parameters<typeof getImpresosRecords>[0])
      searched.value = true
    } catch (e) {
      console.error(e)
    } finally {
      loading.value = false
    }
  }

  function clearAll() {
    fechaInicio.value = ''
    fechaFin.value    = ''
    ;[medio, fuente, anunciante, marca, sector].forEach(f => { f.input = ''; f.tags = [] })
    checkAnunciante.value = checkMarca.value = checkSector.value = false
    data.value        = null
    searched.value    = false
    currentPage.value = 1
  }

  return {
    fechaInicio, fechaFin, perPage,
    medio, fuente, anunciante, marca, sector,
    checkAnunciante, checkMarca, checkSector,
    loading, data, searched, currentPage,
    buscar, clearAll,
  }
})
