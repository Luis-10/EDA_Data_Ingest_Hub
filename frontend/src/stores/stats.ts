import { defineStore } from 'pinia'
import { ref } from 'vue'
import {
  getStatsSummary,
  type GeneralStatsResponse, type ImpresosTopMarca, type TopMarcaRangeItem, type TopSectorItem,
} from '../services/api'

export const useStatsStore = defineStore('stats', () => {
  const tvStats       = ref<GeneralStatsResponse | null>(null)
  const radioStats    = ref<GeneralStatsResponse | null>(null)
  const impresosStats = ref<GeneralStatsResponse | null>(null)
  const topMarcas     = ref<ImpresosTopMarca[]>([])
  const tvTopMarcas      = ref<TopMarcaRangeItem[]>([])
  const radioTopMarcas   = ref<TopMarcaRangeItem[]>([])
  const impresosTopMarcas = ref<TopMarcaRangeItem[]>([])
  const tvTopSectores      = ref<TopSectorItem[]>([])
  const radioTopSectores   = ref<TopSectorItem[]>([])
  const impresosTopSectores = ref<TopSectorItem[]>([])
  const loading       = ref(false)
  const loaded        = ref(false)

  async function loadAll(attempt = 1) {
    if (loaded.value && tvTopMarcas.value.length > 0) return

    loading.value = true
    try {
      const summary = await getStatsSummary()

      if (summary.tv) {
        tvStats.value          = summary.tv.stats
        tvTopMarcas.value      = summary.tv.top_marcas
        tvTopSectores.value    = summary.tv.top_sectores
      }
      if (summary.radio) {
        radioStats.value       = summary.radio.stats
        radioTopMarcas.value   = summary.radio.top_marcas
        radioTopSectores.value = summary.radio.top_sectores
      }
      if (summary.impresos) {
        impresosStats.value       = summary.impresos.stats
        impresosTopMarcas.value   = summary.impresos.top_marcas
        impresosTopSectores.value = summary.impresos.top_sectores
        topMarcas.value           = summary.impresos.top_marcas as ImpresosTopMarca[]
      }

      loaded.value = true
    } catch (e) {
      console.error(`Error cargando stats (intento ${attempt}):`, e)
      if (attempt < 4) {
        const delay = Math.pow(2, attempt) * 1000
        setTimeout(() => loadAll(attempt + 1), delay)
      }
    } finally {
      loading.value = false
    }
  }

  async function refresh() {
    loaded.value = false
    tvTopMarcas.value = []
    await loadAll()
  }

  return {
    tvStats, radioStats, impresosStats, topMarcas,
    tvTopMarcas, radioTopMarcas, impresosTopMarcas,
    tvTopSectores, radioTopSectores, impresosTopSectores,
    loading, loaded, loadAll, refresh,
  }
})
