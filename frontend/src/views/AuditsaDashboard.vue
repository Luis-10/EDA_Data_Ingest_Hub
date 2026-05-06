<script setup lang="ts">
import { ref, reactive, computed, onMounted, watch } from "vue";
import { storeToRefs } from "pinia";
import * as XLSX from "xlsx";
import { Doughnut } from "vue-chartjs";
import {
  Chart as ChartJS,
  ArcElement,
  Tooltip,
  Legend,
} from "chart.js";
import {
  getAutocomplete,
  getExportRecords,
  dateToYYYYMMDD,
  type AutocompleteFilters,
  type TopMarcaRangeItem,
  type TopSectorItem,
} from "../services/api";
import { useStatsStore } from "../stores/stats";
import { useAuditsaStore } from "../stores/auditsa";
import type {
  GeneralStatsResponse,
  StatusRangeResponse,
} from "../services/api";

ChartJS.register(ArcElement, Tooltip, Legend);

type AcField =
  | "Anunciante"
  | "Marca"
  | "Submarca"
  | "Categoria"
  | "Localidad"
  | "TipoMedio"
  | "Medio"
  | "SectorIndustria";

interface TagField {
  input: string;
  tags: string[];
}

// ── Stores ──
const statsStore = useStatsStore();
const store = useAuditsaStore();

const {
  tvStats,
  radioStats,
  impresosStats,
  loading: statsLoading,
  tvTopMarcas: tvTopMarcasStats,
  radioTopMarcas: radioTopMarcasStats,
  impresosTopMarcas: impresosTopMarcasStats,
  tvTopSectores: tvTopSectoresStats,
  radioTopSectores: radioTopSectoresStats,
  impresosTopSectores: impresosTopSectoresStats,
} = storeToRefs(statsStore);
const {
  fechaInicio,
  fechaFin,
  selectedSource,
  loading,
  tvData,
  radioData,
  impresosData,
  tvTopMarcas,
  radioTopMarcas,
  impresosTopMarcas,
  tvTopSectores,
  radioTopSectores,
  impresosTopSectores,
  searched,
  tvLoading,
  radioLoading,
  impresosLoading,
  tvRecords,
  radioRecords,
  impresosRecords,
  tvRecordsLoading,
  radioRecordsLoading,
  impresosRecordsLoading,
} = storeToRefs(store);

// Reactive TagField objects (no necesitan storeToRefs)
const {
  anunciante,
  marca,
  submarca,
  categoria,
  localidad,
  tipoMedio,
  medio,
  sectorIndustria,
  industria,
  tvRadioColumns,
  impresosColumns,
  fetchRecords,
} = store;

// ── Columnas seleccionables por fuente ──
const tvRadioColumnsList = [
  "Fecha",
  "Anunciante",
  "Marca",
  "Submarca",
  "Producto",
  "Version",
  "Ins",
  "Tarifa",
  "Industria",
  "Mercado",
  "Segmento",
  "Campania",
  "Localidad",
  "Medio",
  "TipoMedio",
  "Canal",
  "HInicio",
  "HFinal",
  "FHoraria",
  "TipoSpot",
  "SubTipoSpot",
  "TipoCorte",
  "PEnCorte",
  "NoCorte",
  "Programa",
  "Genero",
  "GComercial",
  "GEstacion",
  "Testigo",
];
const impresosColumnsList = [
  "Fecha",
  "Anunciante",
  "Marca",
  "Submarca",
  "Producto",
  "Ins",
  "Costo",
  "Sector",
  "Subsector",
  "Categoria",
  "Medio",
  "Fuente",
  "Seccion",
  "Pagina",
  "TextoNota",
];
function toggleAllTvRadio(state: boolean) {
  tvRadioColumnsList.forEach((col) => (tvRadioColumns[col] = state));
}
function toggleAllImpresos(state: boolean) {
  impresosColumnsList.forEach((col) => (impresosColumns[col] = state));
}

onMounted(() => statsStore.loadAll());

// ── Autocomplete (estado local efímero) ──
const suggestions = reactive<Record<AcField, string[]>>({
  Anunciante: [],
  Marca: [],
  Submarca: [],
  Categoria: [],
  Localidad: [],
  TipoMedio: [],
  Medio: [],
  SectorIndustria: [],
});
const acVisible = reactive<Record<AcField, boolean>>({
  Anunciante: false,
  Marca: false,
  Submarca: false,
  Categoria: false,
  Localidad: false,
  TipoMedio: false,
  Medio: false,
  SectorIndustria: false,
});
const debounceTimers: Record<string, ReturnType<typeof setTimeout>> = {};
const abortControllers: Record<string, AbortController> = {};

// Campos exclusivos por tabla — no se envían como filtro cuando se busca en la tabla opuesta
const IMPRESOS_ONLY: AcField[] = ["Categoria"];
const TV_RADIO_ONLY: AcField[] = ["Localidad", "TipoMedio"];

function buildFilters(excludeField: AcField): AutocompleteFilters {
  const f: AutocompleteFilters = {};
  const isImpresosOnly = IMPRESOS_ONLY.includes(excludeField);
  const isTvRadioOnly = TV_RADIO_ONLY.includes(excludeField);

  // Filtros comunes a todas las tablas
  if (excludeField !== "Anunciante" && anunciante.tags.length)
    f.anunciante = anunciante.tags.join(",");
  if (excludeField !== "Marca" && marca.tags.length)
    f.marca = marca.tags.join(",");
  if (excludeField !== "Submarca" && submarca.tags.length)
    f.submarca = submarca.tags.join(",");
  if (excludeField !== "Medio" && medio.tags.length && !isImpresosOnly)
    f.medio = medio.tags.join(",");

  // Filtros exclusivos TV/Radio
  if (!isImpresosOnly) {
    if (excludeField !== "TipoMedio" && tipoMedio.tags.length)
      f.tipo_medio = tipoMedio.tags.join(",");
    if (excludeField !== "Localidad" && localidad.tags.length)
      f.localidad = localidad.tags.join(",");
  }

  // Filtros exclusivos Impresos
  if (!isTvRadioOnly) {
    if (excludeField !== "Categoria" && categoria.tags.length)
      f.categoria = categoria.tags.join(",");
  }

  // Filtro unificado Sector/Industria — aplica a todos los medios
  if (excludeField !== "SectorIndustria" && sectorIndustria.tags.length) {
    const val = sectorIndustria.tags.join(",");
    f.sector_industria = val;
    f.industria = val;
  }

  if (fechaInicio.value) f.fecha_inicio = fechaInicio.value;
  if (fechaFin.value) f.fecha_fin = fechaFin.value;
  return f;
}

function hasActiveFilters(excludeField: AcField): boolean {
  const f = buildFilters(excludeField);
  return Object.keys(f).length > 0;
}

async function onAcInput(apiField: AcField, field: TagField) {
  clearTimeout(debounceTimers[apiField]);
  const q = field.input.trim();
  if (!q && !hasActiveFilters(apiField)) {
    suggestions[apiField] = [];
    acVisible[apiField] = false;
    return;
  }
  debounceTimers[apiField] = setTimeout(async () => {
    // Cancelar petición anterior en vuelo para este campo
    abortControllers[apiField]?.abort();
    const ctrl = new AbortController();
    abortControllers[apiField] = ctrl;
    try {
      const results = await getAutocomplete(
        apiField,
        q,
        10000,
        buildFilters(apiField),
        ctrl.signal,
      );
      suggestions[apiField] = results.filter((s) => !field.tags.includes(s));
      acVisible[apiField] = suggestions[apiField].length > 0;
    } catch (e: unknown) {
      if (
        e instanceof Error &&
        e.name !== "CanceledError" &&
        e.name !== "AbortError"
      ) {
        suggestions[apiField] = [];
      }
    }
  }, 350);
}

// Al enfocar un campo, muestra sugerencias pre-cargadas si hay filtros activos
function onAcFocus(apiField: AcField, field: TagField) {
  if (suggestions[apiField].length > 0) {
    acVisible[apiField] = true;
    return;
  }
  if (hasActiveFilters(apiField)) {
    abortControllers[apiField]?.abort();
    const ctrl = new AbortController();
    abortControllers[apiField] = ctrl;
    getAutocomplete(
      apiField,
      field.input.trim(),
      10000,
      buildFilters(apiField),
      ctrl.signal,
    )
      .then((results) => {
        suggestions[apiField] = results.filter((s) => !field.tags.includes(s));
        acVisible[apiField] = suggestions[apiField].length > 0;
      })
      .catch(() => {});
  }
}

function selectSuggestion(apiField: AcField, field: TagField, value: string) {
  if (!field.tags.includes(value)) field.tags.push(value);
  field.input = "";
  suggestions[apiField] = [];
  acVisible[apiField] = false;
}

function hideAc(apiField: AcField) {
  setTimeout(() => {
    acVisible[apiField] = false;
  }, 150);
}

// Al cambiar cualquier filtro de tag, refresca las sugerencias de los demás campos visibles
const ALL_AC_FIELDS: AcField[] = [
  "Anunciante",
  "Marca",
  "Submarca",
  "Medio",
  "TipoMedio",
  "Localidad",
  "Categoria",
  "SectorIndustria",
];
const fieldTagMap: Record<AcField, TagField> = {
  Anunciante: anunciante,
  Marca: marca,
  Submarca: submarca,
  Medio: medio,
  TipoMedio: tipoMedio,
  Localidad: localidad,
  Categoria: categoria,
  SectorIndustria: sectorIndustria,
};

// AbortController global para cancelar todas las peticiones de cascade refresh
let cascadeAbort: AbortController | null = null;
let cascadeTimer: ReturnType<typeof setTimeout> | null = null;

function refreshRelatedSuggestions(changedField: AcField) {
  // Cancelar cascade anterior (debounce de 100ms para agrupar cambios rápidos)
  if (cascadeTimer) clearTimeout(cascadeTimer);
  cascadeAbort?.abort();

  // Limpiar sugerencias inmediatamente
  for (const f of ALL_AC_FIELDS) {
    if (f === changedField) continue;
    suggestions[f] = [];
    acVisible[f] = false;
  }

  cascadeTimer = setTimeout(() => {
    const ctrl = new AbortController();
    cascadeAbort = ctrl;

    for (const f of ALL_AC_FIELDS) {
      if (f === changedField) continue;
      if (!hasActiveFilters(f)) continue;
      getAutocomplete(f, "", 10000, buildFilters(f), ctrl.signal)
        .then((results) => {
          if (ctrl.signal.aborted) return;
          suggestions[f] = results.filter(
            (s) => !fieldTagMap[f].tags.includes(s),
          );
        })
        .catch(() => {});
    }
  }, 100);
}

watch(
  () => [...categoria.tags],
  () => refreshRelatedSuggestions("Categoria"),
);
watch(
  () => [...anunciante.tags],
  () => refreshRelatedSuggestions("Anunciante"),
);
watch(
  () => [...marca.tags],
  () => refreshRelatedSuggestions("Marca"),
);
watch(
  () => [...medio.tags],
  () => refreshRelatedSuggestions("Medio"),
);
watch(
  () => [...tipoMedio.tags],
  () => refreshRelatedSuggestions("TipoMedio"),
);
watch(
  () => [...localidad.tags],
  () => refreshRelatedSuggestions("Localidad"),
);
watch(
  () => [...sectorIndustria.tags],
  () => refreshRelatedSuggestions("SectorIndustria"),
);
watch(
  () => [...submarca.tags],
  () => refreshRelatedSuggestions("Submarca"),
);

function addTag(field: TagField) {
  const val = field.input.trim();
  if (val && !field.tags.includes(val)) field.tags.push(val);
  field.input = "";
}

function removeTag(field: TagField, i: number) {
  field.tags.splice(i, 1);
}
function onEnter(e: KeyboardEvent, field: TagField) {
  e.preventDefault();
  addTag(field);
}

function buildSearchFilters(): AutocompleteFilters {
  const f: AutocompleteFilters = {};
  if (anunciante.tags.length) f.anunciante = anunciante.tags.join(",");
  if (marca.tags.length) f.marca = marca.tags.join(",");
  if (submarca.tags.length) f.submarca = submarca.tags.join(",");
  if (medio.tags.length) f.medio = medio.tags.join(",");
  if (tipoMedio.tags.length) f.tipo_medio = tipoMedio.tags.join(",");
  if (localidad.tags.length) f.localidad = localidad.tags.join(",");
  if (categoria.tags.length) f.categoria = categoria.tags.join(",");
  if (sectorIndustria.tags.length) {
    const val = sectorIndustria.tags.join(",");
    f.sector_industria = val;
    f.industria = val;
  }
  return f;
}

function buscar() {
  const f = buildSearchFilters();
  store.buscar(Object.keys(f).length ? f : undefined);
}
const clearAll = () => store.clearAll();

// ── Exportación a Excel ──
const exporting = ref(false);

async function exportToExcel() {
  // Validaciones
  if (!fechaInicio.value || !fechaFin.value) {
    alert("Selecciona un rango de fechas antes de exportar.");
    return;
  }
  if (!selectedSource.value) {
    alert("Selecciona una fuente (TV, Radio o Impresos) antes de exportar.");
    return;
  }

  const tipo = selectedSource.value;
  const colsMap = tipo === "impresos" ? impresosColumns : tvRadioColumns;
  const activeCols = Object.keys(colsMap).filter((k) => colsMap[k]);

  if (activeCols.length === 0) {
    alert("Selecciona al menos una columna para exportar.");
    return;
  }

  exporting.value = true;
  try {
    const fi = dateToYYYYMMDD(fechaInicio.value);
    const ff = dateToYYYYMMDD(fechaFin.value);
    const filters = buildSearchFilters();

    const resp = await getExportRecords(tipo, fi, ff, activeCols, filters);

    if (resp.records.length === 0) {
      alert("La consulta no devolvió registros para exportar.");
      return;
    }

    // Construir hoja de Excel preservando el orden de las columnas seleccionadas
    const worksheet = XLSX.utils.json_to_sheet(resp.records, {
      header: resp.columns,
    });
    const workbook = XLSX.utils.book_new();
    const sheetName = tipo.charAt(0).toUpperCase() + tipo.slice(1);
    XLSX.utils.book_append_sheet(workbook, worksheet, sheetName);

    // Nombre de archivo: Auditsa_<tipo>_<fi>_<ff>.xlsx
    const filename = `Auditsa_${sheetName}_${fi}_${ff}.xlsx`;
    XLSX.writeFile(workbook, filename);

    if (resp.truncated) {
      alert(
        `Exportación truncada: se exportaron ${resp.total_exported} de ${resp.total_available} registros (límite máximo).`,
      );
    }
  } catch (err) {
    console.error("Error exportando a Excel:", err);
    alert("Ocurrió un error al exportar. Revisa la consola para más detalles.");
  } finally {
    exporting.value = false;
  }
}

// ── Computed ──
const cardTotal = (
  stats: GeneralStatsResponse | null,
  filtered: StatusRangeResponse | null,
) =>
  searched.value
    ? (filtered?.statistics?.total_records ?? 0)
    : (stats?.total_records ?? 0);

const grandTotal = computed(
  () =>
    cardTotal(tvStats.value, tvData.value) +
    cardTotal(radioStats.value, radioData.value) +
    cardTotal(impresosStats.value, impresosData.value),
);

interface TableRow {
  label: string;
  icon: string;
  records: number;
  dates: number;
  dateRange: string;
  valorTotal: string;
  status: "disponible" | "sin_datos" | "pending";
}

interface TopMarcaRow extends TopMarcaRangeItem {
  fuente: string;
  icon: string;
}

const fmtMoney = (v: number) =>
  "$" + v.toLocaleString("es-MX", { maximumFractionDigits: 0 });

const SECTOR_COLORS = [
  "#f97316", "#1e293b", "#0ea5e9", "#84cc16", "#8b5cf6",
  "#f43f5e", "#14b8a6", "#f59e0b", "#6366f1", "#ec4899",
];

function buildDoughnutData(items: TopSectorItem[]) {
  const top = items.slice(0, 10);
  return {
    labels: top.map((i) => i.sector),
    datasets: [
      {
        data: top.map((i) => i.total_inversion),
        backgroundColor: SECTOR_COLORS.slice(0, top.length),
        borderWidth: 2,
        borderColor: "#fff",
      },
    ],
  };
}

const doughnutOptions = {
  responsive: true,
  maintainAspectRatio: false,
  plugins: {
    legend: {
      position: "bottom" as const,
      labels: {
        font: { size: 11 },
        padding: 10,
        boxWidth: 12,
      },
    },
    tooltip: {
      callbacks: {
        label: (ctx: any) => {
          const val = ctx.raw as number;
          return " " + fmtMoney(val);
        },
      },
    },
  },
};

const tvSectorData = computed(() =>
  buildDoughnutData(searched.value ? tvTopSectores.value : tvTopSectoresStats.value)
);
const radioSectorData = computed(() =>
  buildDoughnutData(searched.value ? radioTopSectores.value : radioTopSectoresStats.value)
);
const impresosSectorData = computed(() =>
  buildDoughnutData(searched.value ? impresosTopSectores.value : impresosTopSectoresStats.value)
);

const tableRows = computed<TableRow[]>(() => {
  const makeRow = (
    label: string,
    icon: string,
    stats: GeneralStatsResponse | null,
    filtered: StatusRangeResponse | null,
  ): TableRow => {
    if (searched.value) {
      const s = filtered?.statistics;
      const inv =
        s?.total_tarifa !== undefined
          ? fmtMoney(s.total_tarifa)
          : s?.total_costo !== undefined
            ? fmtMoney(s.total_costo)
            : "N/A";
      return {
        label,
        icon,
        records: s?.total_records ?? 0,
        dates: s?.unique_dates ?? 0,
        dateRange: filtered?.data_exists
          ? `${filtered.fecha_inicio} – ${filtered.fecha_fin}`
          : "Sin datos",
        valorTotal: inv,
        status: filtered?.data_exists ? "disponible" : "sin_datos",
      };
    }
    const val =
      stats?.total_tarifa !== undefined
        ? fmtMoney(stats.total_tarifa)
        : stats?.total_costo !== undefined
          ? fmtMoney(stats.total_costo)
          : "N/A";
    return {
      label,
      icon,
      records: stats?.total_records ?? 0,
      dates: stats?.unique_dates ?? 0,
      dateRange:
        stats?.first_date && stats?.last_date
          ? `${stats.first_date} – ${stats.last_date}`
          : statsLoading.value
            ? "Cargando…"
            : "Sin datos",
      valorTotal: val,
      status:
        (stats?.total_records ?? 0) > 0
          ? "disponible"
          : statsLoading.value
            ? "pending"
            : "sin_datos",
    };
  };
  return [
    makeRow("TV", "📺", tvStats.value, tvData.value),
    makeRow("Radio", "📻", radioStats.value, radioData.value),
    makeRow("Impresos", "📰", impresosStats.value, impresosData.value),
  ];
});

const topMarcasGroups = computed<
  { fuente: string; icon: string; rows: TopMarcaRow[] }[]
>(() => {
  const tag = (
    items: TopMarcaRangeItem[],
    fuente: string,
    icon: string,
  ): TopMarcaRow[] => (items ?? []).map((i) => ({ ...i, fuente, icon }));
  const tvSrc = searched.value ? tvTopMarcas.value : tvTopMarcasStats.value;
  const radioSrc = searched.value
    ? radioTopMarcas.value
    : radioTopMarcasStats.value;
  const impresosSrc = searched.value
    ? impresosTopMarcas.value
    : impresosTopMarcasStats.value;
  return [
    { fuente: "TV", icon: "📺", rows: tag(tvSrc, "TV", "📺") },
    { fuente: "Radio", icon: "📻", rows: tag(radioSrc, "Radio", "📻") },
    {
      fuente: "Impresos",
      icon: "📰",
      rows: tag(impresosSrc, "Impresos", "📰"),
    },
  ];
});
</script>

<template>
  <div class="dash">
    <!-- Header -->
    <div class="dash-header">
      <h1 class="dash-title">📊 Dashboard de Ingesta de Datos</h1>
      <p class="dash-subtitle">Vista consolidada de TV, Radio e Impresos</p>
    </div>

    <!-- Filter card -->
    <div class="card">
      <div class="filter-header">
        <span class="filter-title">🔍 Filtros de Búsqueda</span>
        <button class="btn-clear" @click="clearAll">✕ Limpiar</button>
      </div>

      <!-- Row 1 -->
      <div class="filter-grid">
        <div class="field">
          <label class="label">Fecha Inicio</label>
          <input type="date" v-model="fechaInicio" class="input" />
        </div>
        <div class="field">
          <label class="label">Fecha Fin</label>
          <input type="date" v-model="fechaFin" class="input" />
        </div>
        <div class="field">
          <label class="label"
            >Tipo de Medio <span class="label-badge">Solo TV/Radio</span>
            <span v-if="suggestions.TipoMedio.length" class="label-available"
              >{{ suggestions.TipoMedio.length }} disponibles</span
            >
            <span v-else class="label-count"
              >({{ tipoMedio.tags.length }} seleccionados)</span
            >
          </label>
          <div class="ac-wrap">
            <div class="tag-input-wrap">
              <span v-for="(t, i) in tipoMedio.tags" :key="i" class="tag">
                {{ t }}
                <button @click="removeTag(tipoMedio, i)" class="tag-remove">
                  ×
                </button>
              </span>
              <input
                v-model="tipoMedio.input"
                @input="onAcInput('TipoMedio', tipoMedio)"
                @focus="onAcFocus('TipoMedio', tipoMedio)"
                @keydown.enter="onEnter($event, tipoMedio)"
                @blur="hideAc('TipoMedio')"
                placeholder="Escribe para buscar..."
                class="tag-input"
              />
            </div>
            <ul v-if="acVisible.TipoMedio" class="ac-dropdown">
              <li
                v-for="s in suggestions.TipoMedio"
                :key="s"
                @mousedown.prevent="selectSuggestion('TipoMedio', tipoMedio, s)"
              >
                {{ s }}
              </li>
            </ul>
          </div>
          <div
            v-if="!acVisible.TipoMedio && suggestions.TipoMedio.length"
            class="quick-chips"
          >
            <span
              v-for="s in suggestions.TipoMedio"
              :key="s"
              class="quick-chip"
              @click="selectSuggestion('TipoMedio', tipoMedio, s)"
              >+ {{ s }}</span
            >
          </div>
        </div>
        <div class="field">
          <label class="label"
            >Medio <span class="label-badge">TV/Radio/Impresos</span>
            <span v-if="suggestions.Medio.length" class="label-available"
              >{{ suggestions.Medio.length }} disponibles</span
            >
            <span v-else class="label-count"
              >({{ medio.tags.length }} seleccionados)</span
            >
          </label>
          <div class="ac-wrap">
            <div class="tag-input-wrap">
              <span v-for="(t, i) in medio.tags" :key="i" class="tag">
                {{ t }}
                <button @click="removeTag(medio, i)" class="tag-remove">
                  ×
                </button>
              </span>
              <input
                v-model="medio.input"
                @input="onAcInput('Medio', medio)"
                @focus="onAcFocus('Medio', medio)"
                @keydown.enter="onEnter($event, medio)"
                @blur="hideAc('Medio')"
                placeholder="Escribe para buscar..."
                class="tag-input"
              />
            </div>
            <ul v-if="acVisible.Medio" class="ac-dropdown">
              <li
                v-for="s in suggestions.Medio"
                :key="s"
                @mousedown.prevent="selectSuggestion('Medio', medio, s)"
              >
                {{ s }}
              </li>
            </ul>
          </div>
          <div
            v-if="!acVisible.Medio && suggestions.Medio.length"
            class="quick-chips"
          >
            <span
              v-for="s in suggestions.Medio"
              :key="s"
              class="quick-chip"
              @click="selectSuggestion('Medio', medio, s)"
              >+ {{ s }}</span
            >
          </div>
        </div>
      </div>

      <!-- Row 2 -->
      <div class="filter-grid" style="margin-top: 14px">
        <!-- Anunciante -->
        <div class="field">
          <label class="label"
            >Anunciante <span class="label-badge">TV/Radio/Impresos</span>
            <span v-if="suggestions.Anunciante.length" class="label-available"
              >{{ suggestions.Anunciante.length }} disponibles</span
            >
            <span v-else class="label-count"
              >({{ anunciante.tags.length }} seleccionados)</span
            >
          </label>
          <div class="ac-wrap">
            <div class="tag-input-wrap">
              <span v-for="(t, i) in anunciante.tags" :key="i" class="tag">
                {{ t }}
                <button @click="removeTag(anunciante, i)" class="tag-remove">
                  ×
                </button>
              </span>
              <input
                v-model="anunciante.input"
                @input="onAcInput('Anunciante', anunciante)"
                @focus="onAcFocus('Anunciante', anunciante)"
                @keydown.enter="onEnter($event, anunciante)"
                @blur="hideAc('Anunciante')"
                placeholder="Escribe para buscar..."
                class="tag-input"
              />
            </div>
            <ul v-if="acVisible.Anunciante" class="ac-dropdown">
              <li
                v-for="s in suggestions.Anunciante"
                :key="s"
                @mousedown.prevent="
                  selectSuggestion('Anunciante', anunciante, s)
                "
              >
                {{ s }}
              </li>
            </ul>
          </div>
          <div
            v-if="!acVisible.Anunciante && suggestions.Anunciante.length"
            class="quick-chips"
          >
            <span
              v-for="s in suggestions.Anunciante"
              :key="s"
              class="quick-chip"
              @click="selectSuggestion('Anunciante', anunciante, s)"
              >+ {{ s }}</span
            >
          </div>
        </div>

        <!-- Marca -->
        <div class="field">
          <label class="label"
            >Marca <span class="label-badge">TV/Radio/Impresos</span>
            <span v-if="suggestions.Marca.length" class="label-available"
              >{{ suggestions.Marca.length }} disponibles</span
            >
            <span v-else class="label-count"
              >({{ marca.tags.length }} seleccionados)</span
            >
          </label>
          <div class="ac-wrap">
            <div class="tag-input-wrap">
              <span v-for="(t, i) in marca.tags" :key="i" class="tag">
                {{ t }}
                <button @click="removeTag(marca, i)" class="tag-remove">
                  ×
                </button>
              </span>
              <input
                v-model="marca.input"
                @input="onAcInput('Marca', marca)"
                @focus="onAcFocus('Marca', marca)"
                @keydown.enter="onEnter($event, marca)"
                @blur="hideAc('Marca')"
                placeholder="Escribe para buscar..."
                class="tag-input"
              />
            </div>
            <ul v-if="acVisible.Marca" class="ac-dropdown">
              <li
                v-for="s in suggestions.Marca"
                :key="s"
                @mousedown.prevent="selectSuggestion('Marca', marca, s)"
              >
                {{ s }}
              </li>
            </ul>
          </div>
          <div
            v-if="!acVisible.Marca && suggestions.Marca.length"
            class="quick-chips"
          >
            <span
              v-for="s in suggestions.Marca"
              :key="s"
              class="quick-chip"
              @click="selectSuggestion('Marca', marca, s)"
              >+ {{ s }}</span
            >
          </div>
        </div>

        <!-- Categoría -->
        <div class="field">
          <label class="label"
            >Categoría <span class="label-badge">Solo Impresos</span>
            <span v-if="suggestions.Categoria.length" class="label-available"
              >{{ suggestions.Categoria.length }} disponibles</span
            >
            <span v-else class="label-count"
              >({{ categoria.tags.length }} seleccionados)</span
            >
          </label>
          <div class="ac-wrap">
            <div class="tag-input-wrap">
              <span v-for="(t, i) in categoria.tags" :key="i" class="tag">
                {{ t }}
                <button @click="removeTag(categoria, i)" class="tag-remove">
                  ×
                </button>
              </span>
              <input
                v-model="categoria.input"
                @input="onAcInput('Categoria', categoria)"
                @keydown.enter="onEnter($event, categoria)"
                @blur="hideAc('Categoria')"
                placeholder="Escribe para buscar..."
                class="tag-input"
              />
            </div>
            <ul v-if="acVisible.Categoria" class="ac-dropdown">
              <li
                v-for="s in suggestions.Categoria"
                :key="s"
                @mousedown.prevent="selectSuggestion('Categoria', categoria, s)"
              >
                {{ s }}
              </li>
            </ul>
          </div>
          <div
            v-if="!acVisible.Categoria && suggestions.Categoria.length"
            class="quick-chips"
          >
            <span
              v-for="s in suggestions.Categoria"
              :key="s"
              class="quick-chip"
              @click="selectSuggestion('Categoria', categoria, s)"
              >+ {{ s }}</span
            >
          </div>
        </div>

        <!-- Localidad -->
        <div class="field">
          <label class="label"
            >Localidad <span class="label-badge">Solo TV/Radio</span>
            <span v-if="suggestions.Localidad.length" class="label-available"
              >{{ suggestions.Localidad.length }} disponibles</span
            >
          </label>
          <div class="ac-wrap">
            <div class="tag-input-wrap">
              <span v-for="(t, i) in localidad.tags" :key="i" class="tag">
                {{ t }}
                <button @click="removeTag(localidad, i)" class="tag-remove">
                  ×
                </button>
              </span>
              <input
                v-model="localidad.input"
                @input="onAcInput('Localidad', localidad)"
                @focus="onAcFocus('Localidad', localidad)"
                @keydown.enter="onEnter($event, localidad)"
                @blur="hideAc('Localidad')"
                placeholder="Escribe para buscar..."
                class="tag-input"
              />
            </div>
            <ul v-if="acVisible.Localidad" class="ac-dropdown">
              <li
                v-for="s in suggestions.Localidad"
                :key="s"
                @mousedown.prevent="selectSuggestion('Localidad', localidad, s)"
              >
                {{ s }}
              </li>
            </ul>
          </div>
          <div
            v-if="!acVisible.Localidad && suggestions.Localidad.length"
            class="quick-chips"
          >
            <span
              v-for="s in suggestions.Localidad"
              :key="s"
              class="quick-chip"
              @click="selectSuggestion('Localidad', localidad, s)"
              >+ {{ s }}</span
            >
          </div>
        </div>
      </div>

      <!-- Row 3 -->
      <div class="filter-grid" style="margin-top: 14px">
        <!-- Submarca -->
        <div class="field">
          <label class="label"
            >Submarca <span class="label-badge">TV/Radio/Impresos</span>
            <span v-if="suggestions.Submarca.length" class="label-available"
              >{{ suggestions.Submarca.length }} disponibles</span
            >
            <span v-else class="label-count"
              >({{ submarca.tags.length }} seleccionados)</span
            >
          </label>
          <div class="ac-wrap">
            <div class="tag-input-wrap">
              <span v-for="(t, i) in submarca.tags" :key="i" class="tag">
                {{ t }}
                <button @click="removeTag(submarca, i)" class="tag-remove">
                  ×
                </button>
              </span>
              <input
                v-model="submarca.input"
                @input="onAcInput('Submarca', submarca)"
                @focus="onAcFocus('Submarca', submarca)"
                @keydown.enter="onEnter($event, submarca)"
                @blur="hideAc('Submarca')"
                placeholder="Escribe para buscar..."
                class="tag-input"
              />
            </div>
            <ul v-if="acVisible.Submarca" class="ac-dropdown">
              <li
                v-for="s in suggestions.Submarca"
                :key="s"
                @mousedown.prevent="selectSuggestion('Submarca', submarca, s)"
              >
                {{ s }}
              </li>
            </ul>
          </div>
          <div
            v-if="!acVisible.Submarca && suggestions.Submarca.length"
            class="quick-chips"
          >
            <span
              v-for="s in suggestions.Submarca"
              :key="s"
              class="quick-chip"
              @click="selectSuggestion('Submarca', submarca, s)"
              >+ {{ s }}</span
            >
          </div>
        </div>

        <!-- Sector/Industria -->
        <div class="field">
          <label class="label"
            >Sector/Industria
            <span class="label-badge">Todos los medios</span>
            <span
              v-if="suggestions.SectorIndustria.length"
              class="label-available"
              >{{ suggestions.SectorIndustria.length }} disponibles</span
            >
            <span v-else class="label-count"
              >({{ sectorIndustria.tags.length }} seleccionados)</span
            >
          </label>
          <div class="ac-wrap">
            <div class="tag-input-wrap">
              <span v-for="(t, i) in sectorIndustria.tags" :key="i" class="tag">
                {{ t }}
                <button
                  @click="removeTag(sectorIndustria, i)"
                  class="tag-remove"
                >
                  ×
                </button>
              </span>
              <input
                v-model="sectorIndustria.input"
                @input="onAcInput('SectorIndustria', sectorIndustria)"
                @focus="onAcFocus('SectorIndustria', sectorIndustria)"
                @keydown.enter="onEnter($event, sectorIndustria)"
                @blur="hideAc('SectorIndustria')"
                placeholder="Escribe para buscar..."
                class="tag-input"
              />
            </div>
            <ul v-if="acVisible.SectorIndustria" class="ac-dropdown">
              <li
                v-for="s in suggestions.SectorIndustria"
                :key="s"
                @mousedown.prevent="
                  selectSuggestion('SectorIndustria', sectorIndustria, s)
                "
              >
                {{ s }}
              </li>
            </ul>
          </div>
          <div
            v-if="
              !acVisible.SectorIndustria && suggestions.SectorIndustria.length
            "
            class="quick-chips"
          >
            <span
              v-for="s in suggestions.SectorIndustria"
              :key="s"
              class="quick-chip"
              @click="selectSuggestion('SectorIndustria', sectorIndustria, s)"
              >+ {{ s }}</span
            >
          </div>
        </div>

      </div>

      <!-- Actions -->
      <div class="filter-actions">
        <button
          class="btn-buscar"
          :disabled="loading || !fechaInicio || !fechaFin"
          @click="buscar"
        >
          <span v-if="loading" class="spinner"></span>
          🔍 Buscar
        </button>

        <div class="checks">
          <label class="check-label"
            ><input
              type="checkbox"
              :checked="selectedSource === 'tv'"
              @change="selectedSource = selectedSource === 'tv' ? null : 'tv'"
            />
            TV</label
          >
          <label class="check-label"
            ><input
              type="checkbox"
              :checked="selectedSource === 'radio'"
              @change="
                selectedSource = selectedSource === 'radio' ? null : 'radio'
              "
            />
            Radio</label
          >
          <label class="check-label"
            ><input
              type="checkbox"
              :checked="selectedSource === 'impresos'"
              @change="
                selectedSource =
                  selectedSource === 'impresos' ? null : 'impresos'
              "
            />
            Impresos</label
          >
        </div>

        <button
          class="btn-export"
          :disabled="
            exporting || loading || !selectedSource || !fechaInicio || !fechaFin
          "
          @click="exportToExcel"
        >
          <span v-if="exporting" class="spinner"></span>
          📥 {{ exporting ? "Exportando..." : "Exportar a Excel" }}
        </button>
      </div>

      <!-- Columnas seleccionables por fuente -->
      <div v-if="selectedSource === 'tv'" class="columns-selector">
        <p class="columns-selector-title">📺 Columnas de TV</p>
        <div class="columns-grid">
          <label
            v-for="col in tvRadioColumnsList"
            :key="col"
            class="column-check-label"
          >
            <input type="checkbox" v-model="tvRadioColumns[col]" />
            {{ col }}
          </label>
        </div>
        <div class="columns-actions">
          <button
            class="columns-btn columns-btn--all"
            @click="toggleAllTvRadio(true)"
          >
            Seleccionar todo
          </button>
          <button
            class="columns-btn columns-btn--none"
            @click="toggleAllTvRadio(false)"
          >
            Deseleccionar todo
          </button>
        </div>
      </div>

      <div v-if="selectedSource === 'radio'" class="columns-selector">
        <p class="columns-selector-title">📻 Columnas de Radio</p>
        <div class="columns-grid">
          <label
            v-for="col in tvRadioColumnsList"
            :key="col"
            class="column-check-label"
          >
            <input type="checkbox" v-model="tvRadioColumns[col]" />
            {{ col }}
          </label>
        </div>
        <div class="columns-actions">
          <button
            class="columns-btn columns-btn--all"
            @click="toggleAllTvRadio(true)"
          >
            Seleccionar todo
          </button>
          <button
            class="columns-btn columns-btn--none"
            @click="toggleAllTvRadio(false)"
          >
            Deseleccionar todo
          </button>
        </div>
      </div>

      <div v-if="selectedSource === 'impresos'" class="columns-selector">
        <p class="columns-selector-title">📰 Columnas de Impresos</p>
        <div class="columns-grid">
          <label
            v-for="col in impresosColumnsList"
            :key="col"
            class="column-check-label"
          >
            <input type="checkbox" v-model="impresosColumns[col]" />
            {{ col }}
          </label>
        </div>
        <div class="columns-actions">
          <button
            class="columns-btn columns-btn--all"
            @click="toggleAllImpresos(true)"
          >
            Seleccionar todo
          </button>
          <button
            class="columns-btn columns-btn--none"
            @click="toggleAllImpresos(false)"
          >
            Deseleccionar todo
          </button>
        </div>
      </div>
    </div>

    <!-- Summary cards -->
    <div class="cards-grid">
      <!-- TV -->
      <div class="stat-card stat-card--orange">
        <div class="stat-card-body">
          <div style="flex: 1">
            <p class="stat-label">TV</p>
            <template v-if="statsLoading || tvLoading">
              <div class="skel skel--value"></div>
              <div class="skel skel--sub"></div>
              <div class="skel skel--dates"></div>
            </template>
            <template v-else>
              <p class="stat-value">
                {{ cardTotal(tvStats, tvData).toLocaleString() }}
              </p>
              <p class="stat-sub">Registros</p>
              <p class="stat-dates">
                {{
                  cardTotal(tvStats, tvData) > 0
                    ? searched
                      ? (tvData?.statistics?.unique_dates ?? 0)
                      : (tvStats?.unique_dates ?? 0)
                    : 0
                }}
                fechas
              </p>
            </template>
          </div>
          <span class="stat-icon">📺</span>
        </div>
      </div>

      <!-- Radio -->
      <div class="stat-card stat-card--orange">
        <div class="stat-card-body">
          <div style="flex: 1">
            <p class="stat-label">RADIO</p>
            <template v-if="statsLoading || radioLoading">
              <div class="skel skel--value"></div>
              <div class="skel skel--sub"></div>
              <div class="skel skel--dates"></div>
            </template>
            <template v-else>
              <p class="stat-value">
                {{ cardTotal(radioStats, radioData).toLocaleString() }}
              </p>
              <p class="stat-sub">Registros</p>
              <p class="stat-dates">
                {{
                  cardTotal(radioStats, radioData) > 0
                    ? searched
                      ? (radioData?.statistics?.unique_dates ?? 0)
                      : (radioStats?.unique_dates ?? 0)
                    : 0
                }}
                fechas
              </p>
            </template>
          </div>
          <span class="stat-icon">📻</span>
        </div>
      </div>

      <!-- Impresos -->
      <div class="stat-card stat-card--orange">
        <div class="stat-card-body">
          <div style="flex: 1">
            <p class="stat-label">IMPRESOS</p>
            <template v-if="statsLoading || impresosLoading">
              <div class="skel skel--value"></div>
              <div class="skel skel--sub"></div>
              <div class="skel skel--dates"></div>
            </template>
            <template v-else>
              <p class="stat-value">
                {{ cardTotal(impresosStats, impresosData).toLocaleString() }}
              </p>
              <p class="stat-sub">Registros</p>
              <p class="stat-dates">
                {{
                  cardTotal(impresosStats, impresosData) > 0
                    ? searched
                      ? (impresosData?.statistics?.unique_dates ?? 0)
                      : (impresosStats?.unique_dates ?? 0)
                    : 0
                }}
                fechas
              </p>
            </template>
          </div>
          <span class="stat-icon">📰</span>
        </div>
      </div>

      <!-- Total -->
      <div class="stat-card stat-card--navyblue">
        <div class="stat-card-body">
          <div style="flex: 1">
            <p class="stat-label">TOTAL</p>
            <template v-if="statsLoading">
              <div class="skel skel--value"></div>
              <div class="skel skel--sub"></div>
              <div class="skel skel--dates"></div>
            </template>
            <template v-else>
              <p class="stat-value">{{ grandTotal.toLocaleString() }}</p>
              <p class="stat-sub">Registros</p>
              <p class="stat-dates">3 fuentes</p>
            </template>
          </div>
          <span class="stat-icon">📊</span>
        </div>
      </div>
    </div>

    <!-- Table -->
    <!-- <div class="card" style="padding: 0; overflow: hidden">
      <table class="data-table">
        <thead>
          <tr>
            <th>Fuente</th>
            <th>Rango de Fechas</th>
            <th class="text-right">Registros</th>
            <th class="text-right">Fechas Únicas</th>
            <th class="text-right">Valor Total</th>
            <th class="text-center">Estado</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in tableRows" :key="row.label">
            <td class="td-fuente">
              <span>{{ row.icon }}</span> {{ row.label }}
            </td>
            <td class="td-muted">{{ row.dateRange }}</td>
            <td class="text-right td-strong">
              {{ row.records.toLocaleString() }}
            </td>
            <td class="text-right td-muted">{{ row.dates }}</td>
            <td class="text-right td-strong">{{ row.valorTotal }}</td>
            <td class="text-center">
              <span
                v-if="row.status === 'disponible'"
                class="badge badge--green"
                >DISPONIBLE</span
              >
              <span
                v-else-if="row.status === 'sin_datos'"
                class="badge badge--orange"
                >SIN DATOS</span
              >
              <span v-else class="td-muted">—</span>
            </td>
          </tr>
        </tbody>
      </table>
    </div> -->

        <!-- ── Gráficas por sector ── -->
    <div class="sector-charts-section">
      <div class="top-marcas-header">
        <span class="table-title">📊 Inversión por Sector</span>
        <span class="table-sub">{{
          searched ? "Filtrado · " + (tableRows[0]?.dateRange ?? "") : "Todos los tiempos"
        }}</span>
      </div>
      <div class="sector-charts-grid">
        <!-- TV -->
        <div class="sector-chart-card">
          <div class="sector-chart-header">
            <span class="sector-chart-icon">📺</span>
            <span class="sector-chart-title">TV</span>
          </div>
          <template v-if="tvLoading || statsLoading">
            <div class="sector-chart-skel">
              <div class="skel" style="width:140px;height:140px;border-radius:50%;margin:auto"></div>
            </div>
          </template>
          <template v-else-if="tvSectorData.labels.length">
            <div class="sector-chart-wrap">
              <Doughnut :data="tvSectorData" :options="doughnutOptions" />
            </div>
          </template>
          <div v-else class="sector-chart-empty">Sin datos de sector</div>
        </div>

        <!-- Radio -->
        <div class="sector-chart-card">
          <div class="sector-chart-header">
            <span class="sector-chart-icon">📻</span>
            <span class="sector-chart-title">Radio</span>
          </div>
          <template v-if="radioLoading || statsLoading">
            <div class="sector-chart-skel">
              <div class="skel" style="width:140px;height:140px;border-radius:50%;margin:auto"></div>
            </div>
          </template>
          <template v-else-if="radioSectorData.labels.length">
            <div class="sector-chart-wrap">
              <Doughnut :data="radioSectorData" :options="doughnutOptions" />
            </div>
          </template>
          <div v-else class="sector-chart-empty">Sin datos de sector</div>
        </div>

        <!-- Impresos -->
        <div class="sector-chart-card">
          <div class="sector-chart-header">
            <span class="sector-chart-icon">🗞</span>
            <span class="sector-chart-title">Impresos</span>
          </div>
          <template v-if="impresosLoading || statsLoading">
            <div class="sector-chart-skel">
              <div class="skel" style="width:140px;height:140px;border-radius:50%;margin:auto"></div>
            </div>
          </template>
          <template v-else-if="impresosSectorData.labels.length">
            <div class="sector-chart-wrap">
              <Doughnut :data="impresosSectorData" :options="doughnutOptions" />
            </div>
          </template>
          <div v-else class="sector-chart-empty">Sin datos de sector</div>
        </div>
      </div>
    </div>

    <!-- Top 10 Marcas por Inversión — tabla horizontal unificada -->
    <div class="top-marcas-section">
      <div class="top-marcas-header">
        <span class="table-title">🏆 Top 10 Marcas por Inversión</span>
        <span class="table-sub">{{
          searched
            ? "Filtrado · " + (tableRows[0]?.dateRange ?? "")
            : "Todos los tiempos"
        }}</span>
      </div>
      <div class="card" style="padding: 0; overflow: auto">
        <table class="data-table top-marcas-unified">
          <colgroup>
            <col class="col-rank" />
            <col class="col-marca" />
            <col class="col-inv" />
            <col class="col-divider" />
            <col class="col-rank" />
            <col class="col-marca" />
            <col class="col-inv" />
            <col class="col-divider" />
            <col class="col-rank" />
            <col class="col-marca" />
            <col class="col-inv" />
          </colgroup>
          <thead>
            <tr class="tm-group-row">
              <th colspan="3" class="tm-group-th">📺 TV</th>
              <th class="col-divider-th"></th>
              <th colspan="3" class="tm-group-th">📻 Radio</th>
              <th class="col-divider-th"></th>
              <th colspan="3" class="tm-group-th">📰 Impresos</th>
            </tr>
            <tr>
              <th class="text-center">#</th>
              <th>Anunciante / Marca</th>
              <th class="text-right">Registros / Inversión</th>
              <th class="col-divider-th"></th>
              <th class="text-center">#</th>
              <th>Anunciante / Marca</th>
              <th class="text-right">Registros / Inversión</th>
              <th class="col-divider-th"></th>
              <th class="text-center">#</th>
              <th>Anunciante / Marca</th>
              <th class="text-right">Registros / Inversión</th>
            </tr>
          </thead>
          <tbody>
            <template v-if="loading">
              <tr v-for="n in 10" :key="`skel-${n}`">
                <td v-for="col in 11" :key="col">
                  <div
                    v-if="col !== 4 && col !== 8"
                    class="skel"
                    style="height: 12px"
                  ></div>
                </td>
              </tr>
            </template>
            <template v-else>
              <tr v-for="i in 10" :key="i">
                <!-- TV -->
                <template v-if="topMarcasGroups[0].rows[i - 1]">
                  <td class="td-rank">{{ i }}</td>
                  <td>
                    <div class="td-strong tm-marca">
                      {{ topMarcasGroups[0].rows[i - 1].marca }}
                    </div>
                    <div class="td-muted tm-anunciante">
                      {{ topMarcasGroups[0].rows[i - 1].anunciante }}
                    </div>
                  </td>
                  <td class="text-right">
                    <div class="td-inversion tm-inv">
                      {{
                        fmtMoney(topMarcasGroups[0].rows[i - 1].total_inversion)
                      }}
                    </div>
                    <div class="td-muted tm-regs">
                      {{
                        topMarcasGroups[0].rows[
                          i - 1
                        ].registros.toLocaleString()
                      }}
                      regs.
                    </div>
                  </td>
                </template>
                <template v-else>
                  <td class="td-rank td-muted">{{ i }}</td>
                  <td colspan="2" class="td-muted tm-empty">—</td>
                </template>
                <td class="col-divider-td"></td>
                <!-- Radio -->
                <template v-if="topMarcasGroups[1].rows[i - 1]">
                  <td class="td-rank">{{ i }}</td>
                  <td>
                    <div class="td-strong tm-marca">
                      {{ topMarcasGroups[1].rows[i - 1].marca }}
                    </div>
                    <div class="td-muted tm-anunciante">
                      {{ topMarcasGroups[1].rows[i - 1].anunciante }}
                    </div>
                  </td>
                  <td class="text-right">
                    <div class="td-inversion tm-inv">
                      {{
                        fmtMoney(topMarcasGroups[1].rows[i - 1].total_inversion)
                      }}
                    </div>
                    <div class="td-muted tm-regs">
                      {{
                        topMarcasGroups[1].rows[
                          i - 1
                        ].registros.toLocaleString()
                      }}
                      regs.
                    </div>
                  </td>
                </template>
                <template v-else>
                  <td class="td-rank td-muted">{{ i }}</td>
                  <td colspan="2" class="td-muted tm-empty">—</td>
                </template>
                <td class="col-divider-td"></td>
                <!-- Impresos -->
                <template v-if="topMarcasGroups[2].rows[i - 1]">
                  <td class="td-rank">{{ i }}</td>
                  <td>
                    <div class="td-strong tm-marca">
                      {{ topMarcasGroups[2].rows[i - 1].marca }}
                    </div>
                    <div class="td-muted tm-anunciante">
                      {{ topMarcasGroups[2].rows[i - 1].anunciante }}
                    </div>
                  </td>
                  <td class="text-right">
                    <div class="td-inversion tm-inv">
                      {{
                        fmtMoney(topMarcasGroups[2].rows[i - 1].total_inversion)
                      }}
                    </div>
                    <div class="td-muted tm-regs">
                      {{
                        topMarcasGroups[2].rows[
                          i - 1
                        ].registros.toLocaleString()
                      }}
                      regs.
                    </div>
                  </td>
                </template>
                <template v-else>
                  <td class="td-rank td-muted">{{ i }}</td>
                  <td colspan="2" class="td-muted tm-empty">—</td>
                </template>
              </tr>
            </template>
          </tbody>
        </table>
      </div>
    </div>

    <!-- ── Tablas de detalle por medio ── -->
    <template v-if="searched">
      <!-- TV Records -->
      <div class="detail-table-section">
        <div class="detail-table-header">
          <span class="table-title">📺 Registros TV</span>
          <span v-if="tvRecords" class="table-sub">
            {{ tvRecords.total.toLocaleString() }} registros · Página
            {{ tvRecords.page }} de {{ tvRecords.pages }}
          </span>
        </div>
        <div class="card" style="padding: 0; overflow: auto">
          <template v-if="tvRecordsLoading">
            <div class="detail-skel-wrap">
              <div
                v-for="n in 5"
                :key="n"
                class="skel"
                style="height: 16px; margin: 10px 12px"
              ></div>
            </div>
          </template>
          <template v-else-if="tvRecords && tvRecords.records.length > 0">
            <table class="detail-table">
              <thead>
                <tr>
                  <th v-for="col in tvRecords.columns" :key="col">{{ col }}</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(row, idx) in tvRecords.records" :key="idx">
                  <td v-for="col in tvRecords.columns" :key="col">
                    {{ row[col] }}
                  </td>
                </tr>
              </tbody>
            </table>
          </template>
          <div v-else class="detail-empty">
            Sin registros de TV para este rango/filtros.
          </div>
        </div>
        <div v-if="tvRecords && tvRecords.records.length > 0" class="pagination-bar">
          <button :disabled="tvRecords.page <= 1" @click="fetchRecords('tv', tvRecords!.page - 1)">Anterior</button>
          <span>Página {{ tvRecords.page }} / {{ tvRecords.pages }}</span>
          <button :disabled="tvRecords.page >= tvRecords.pages" @click="fetchRecords('tv', tvRecords!.page + 1)">Siguiente</button>
        </div>
      </div>

      <!-- Radio Records -->
      <div class="detail-table-section">
        <div class="detail-table-header">
          <span class="table-title">📻 Registros Radio</span>
          <span v-if="radioRecords" class="table-sub">
            {{ radioRecords.total.toLocaleString() }} registros · Página
            {{ radioRecords.page }} de {{ radioRecords.pages }}
          </span>
        </div>
        <div class="card" style="padding: 0; overflow: auto">
          <template v-if="radioRecordsLoading">
            <div class="detail-skel-wrap">
              <div
                v-for="n in 5"
                :key="n"
                class="skel"
                style="height: 16px; margin: 10px 12px"
              ></div>
            </div>
          </template>
          <template v-else-if="radioRecords && radioRecords.records.length > 0">
            <table class="detail-table">
              <thead>
                <tr>
                  <th v-for="col in radioRecords.columns" :key="col">{{ col }}</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(row, idx) in radioRecords.records" :key="idx">
                  <td v-for="col in radioRecords.columns" :key="col">
                    {{ row[col] }}
                  </td>
                </tr>
              </tbody>
            </table>
          </template>
          <div v-else class="detail-empty">
            Sin registros de Radio para este rango/filtros.
          </div>
        </div>
        <div v-if="radioRecords && radioRecords.records.length > 0" class="pagination-bar">
          <button :disabled="radioRecords.page <= 1" @click="fetchRecords('radio', radioRecords!.page - 1)">Anterior</button>
          <span>Página {{ radioRecords.page }} / {{ radioRecords.pages }}</span>
          <button :disabled="radioRecords.page >= radioRecords.pages" @click="fetchRecords('radio', radioRecords!.page + 1)">Siguiente</button>
        </div>
      </div>

      <!-- Impresos Records -->
      <div class="detail-table-section">
        <div class="detail-table-header">
          <span class="table-title">📰 Registros Impresos</span>
          <span v-if="impresosRecords" class="table-sub">
            {{ impresosRecords.total.toLocaleString() }} registros · Página
            {{ impresosRecords.page }} de {{ impresosRecords.pages }}
          </span>
        </div>
        <div class="card" style="padding: 0; overflow: auto">
          <template v-if="impresosRecordsLoading">
            <div class="detail-skel-wrap">
              <div
                v-for="n in 5"
                :key="n"
                class="skel"
                style="height: 16px; margin: 10px 12px"
              ></div>
            </div>
          </template>
          <template v-else-if="impresosRecords && impresosRecords.records.length > 0">
            <table class="detail-table">
              <thead>
                <tr>
                  <th v-for="col in impresosRecords.columns" :key="col">{{ col }}</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(row, idx) in impresosRecords.records" :key="idx">
                  <td v-for="col in impresosRecords.columns" :key="col">
                    {{ row[col] }}
                  </td>
                </tr>
              </tbody>
            </table>
          </template>
          <div v-else class="detail-empty">
            Sin registros de Impresos para este rango/filtros.
          </div>
        </div>
        <div v-if="impresosRecords && impresosRecords.records.length > 0" class="pagination-bar">
          <button :disabled="impresosRecords.page <= 1" @click="fetchRecords('impresos', impresosRecords!.page - 1)">Anterior</button>
          <span>Página {{ impresosRecords.page }} / {{ impresosRecords.pages }}</span>
          <button :disabled="impresosRecords.page >= impresosRecords.pages" @click="fetchRecords('impresos', impresosRecords!.page + 1)">Siguiente</button>
        </div>
      </div>
    </template>
  </div>
</template>

<style scoped>
.dash {
  padding: 24px 5rem;
  display: flex;
  flex-direction: column;
  gap: 20px;
  min-height: 100%;
  background: #f1f5f9;
}

/* Header */
.dash-title {
  margin: 0 0 4px;
  font-size: 22px;
  font-weight: 700;
  color: #1e293b;
}
.dash-subtitle {
  margin: 0;
  font-size: 14px;
  color: #64748b;
}

/* Card */
.card {
  background: #fff;
  border-radius: 12px;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.07);
  padding: 20px;
}

/* Filter header */
.filter-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}
.filter-title {
  font-size: 15px;
  font-weight: 600;
  color: #374151;
}
.btn-clear {
  background: none;
  border: 1px solid #e2e8f0;
  border-radius: 6px;
  padding: 5px 12px;
  font-size: 12.5px;
  color: #64748b;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 5px;
}
.btn-clear:hover {
  border-color: #94a3b8;
  color: #374151;
}

/* Grid */
.filter-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 14px;
}

.field {
  display: flex;
  flex-direction: column;
  gap: 5px;
}

.label {
  font-size: 11px;
  font-weight: 600;
  color: #64748b;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}
.label-badge {
  font-size: 10px;
  font-weight: 500;
  color: #94a3b8;
  text-transform: none;
  letter-spacing: 0;
  margin-left: 4px;
}
.label-count {
  font-size: 10.5px;
  color: #94a3b8;
  font-weight: 400;
  text-transform: none;
  letter-spacing: 0;
}

.input {
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  padding: 8px 12px;
  font-size: 13px;
  color: #374151;
  outline: none;
  background: #fff;
  width: 100%;
  box-sizing: border-box;
  font-family: inherit;
}
.input:focus {
  border-color: #f97316;
  box-shadow: 0 0 0 3px rgba(249, 115, 22, 0.12);
}

/* Tag input */
.tag-input-wrap {
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  padding: 5px 8px;
  min-height: 38px;
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
  align-items: center;
  background: #fff;
  cursor: text;
}
.tag-input-wrap:focus-within {
  border-color: #f97316;
  box-shadow: 0 0 0 3px rgba(249, 115, 22, 0.12);
}

.tag {
  background: #fff7ed;
  color: #c2410c;
  font-size: 11.5px;
  padding: 2px 8px 2px 8px;
  border-radius: 20px;
  display: flex;
  align-items: center;
  gap: 4px;
}
.tag-remove {
  background: none;
  border: none;
  color: #c2410c;
  cursor: pointer;
  padding: 0;
  font-size: 14px;
  line-height: 1;
}
.tag-input {
  flex: 1;
  min-width: 60px;
  border: none;
  outline: none;
  font-size: 12.5px;
  color: #374151;
  background: transparent;
  font-family: inherit;
  padding: 2px 0;
}
.tag-input::placeholder {
  color: #94a3b8;
}

.btn-add {
  background: none;
  border: none;
  color: #f97316;
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;
  padding: 2px 0;
  text-align: left;
  font-family: inherit;
}
.btn-add:hover {
  color: #ea6c0a;
}

/* Actions row */
.filter-actions {
  display: flex;
  align-items: center;
  gap: 20px;
  margin-top: 16px;
  flex-wrap: wrap;
}

.btn-buscar {
  background: #f97316;
  color: #fff;
  border: none;
  border-radius: 8px;
  padding: 10px 24px;
  font-size: 14px;
  font-weight: 600;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 8px;
  font-family: inherit;
  transition: background 0.15s;
}
.btn-buscar:hover:not(:disabled) {
  background: #ea6c0a;
}
.btn-buscar:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.spinner {
  width: 14px;
  height: 14px;
  border: 2px solid rgba(255, 255, 255, 0.4);
  border-top-color: #fff;
  border-radius: 50%;
  animation: spin 0.7s linear infinite;
  display: inline-block;
}
@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

.checks {
  display: flex;
  align-items: center;
  gap: 16px;
}
.check-label {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 13px;
  color: #4b5563;
  cursor: pointer;
}

.btn-export {
  margin-left: auto;
  background: #1e293b;
  color: #fff;
  border: none;
  border-radius: 8px;
  padding: 10px 20px;
  font-size: 13.5px;
  font-weight: 600;
  cursor: pointer;
  font-family: inherit;
  transition: background 0.15s;
}
.btn-export:hover {
  background: #0f172a;
}

/* Columns selector */
.columns-selector {
  margin-top: 18px;
  padding: 16px 20px;
  background: #f8fafc;
  border: 1px solid #e2e8f0;
  border-radius: 10px;
}
.columns-selector-title {
  font-size: 13.5px;
  font-weight: 700;
  color: #334155;
  margin: 0 0 12px 0;
}
.columns-grid {
  display: flex;
  flex-wrap: wrap;
  gap: 6px 14px;
}
.column-check-label {
  display: flex;
  align-items: center;
  gap: 5px;
  font-size: 12.5px;
  color: #475569;
  cursor: pointer;
  padding: 4px 10px;
  border-radius: 6px;
  border: 1px solid transparent;
  transition:
    background 0.12s,
    border-color 0.12s;
  user-select: none;
}
.column-check-label:hover {
  background: #e0e7ff;
  border-color: #c7d2fe;
}
.column-check-label input[type="checkbox"] {
  accent-color: #f97316;
  width: 14px;
  height: 14px;
  cursor: pointer;
}
.columns-actions {
  display: flex;
  gap: 10px;
  margin-top: 12px;
}
.columns-btn {
  font-size: 11.5px;
  font-weight: 600;
  padding: 5px 14px;
  border-radius: 6px;
  border: 1px solid #cbd5e1;
  cursor: pointer;
  font-family: inherit;
  transition: background 0.12s;
}
.columns-btn--all {
  background: #f97316;
  color: #fff;
  border-color: #f97316;
}
.columns-btn--all:hover {
  background: #ea6c0a;
}
.columns-btn--none {
  background: #fff;
  color: #64748b;
}
.columns-btn--none:hover {
  background: #f1f5f9;
}

/* Summary cards */
.cards-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 3rem;
  /* padding: 1.5rem; */
}

.stat-card {
  background: #fff;
  border-radius: 12px;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.07);
  padding: 18px 20px;
  border-left: 4px solid transparent;
  transition:
    transform 0.18s ease,
    box-shadow 0.18s ease;
  cursor: default;
}

.stat-card:hover {
  transform: translateY(-4px);
  box-shadow: 0 8px 24px rgba(0, 0, 0, 0.11);
}

.stat-card--orange {
  border-left-color: #f97316;
}

.stat-card--yellow {
  border-left-color: #e7cf65;
}

.stat-card--navyblue {
  border-left-color: #1e293b;
}

.stat-card--red {
  border-left-color: #ef4444;
}

.stat-card-body {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.stat-label {
  font-size: 11px;
  font-weight: 700;
  color: #64748b;
  text-transform: uppercase;
  letter-spacing: 0.6px;
  margin: 0 0 6px;
}
.stat-value {
  font-size: 30px;
  font-weight: 700;
  color: #1e293b;
  margin: 0 0 2px;
  line-height: 1;
}
.stat-sub {
  font-size: 13px;
  color: #64748b;
  margin: 0 0 4px;
}
.stat-dates {
  font-size: 12px;
  color: #f97316;
  margin: 0;
}
.stat-icon {
  font-size: 26px;
  opacity: 0.5;
}

/* Table */
.data-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13.5px;
}

.data-table thead tr {
  background: #1e293b;
}
.data-table thead th {
  padding: 13px 18px;
  font-size: 11.5px;
  font-weight: 600;
  color: #fff;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  text-align: left;
}
.data-table thead th.text-right {
  text-align: right;
}
.data-table thead th.text-center {
  text-align: center;
}

.data-table tbody tr {
  border-bottom: 1px solid #f1f5f9;
  transition: background 0.12s;
}
.data-table tbody tr:last-child {
  border-bottom: none;
}
.data-table tbody tr:hover {
  background: #f8fafc;
}

.data-table tbody td {
  padding: 14px 18px;
}

.td-fuente {
  display: flex;
  align-items: center;
  gap: 8px;
  font-weight: 600;
  color: #1e293b;
}
.td-muted {
  color: #64748b;
}
.td-strong {
  color: #1e293b;
  font-weight: 600;
}
.text-right {
  text-align: right;
}
.text-center {
  text-align: center;
}

/* Badges */
.badge {
  display: inline-flex;
  align-items: center;
  padding: 3px 10px;
  border-radius: 20px;
  font-size: 11.5px;
  font-weight: 600;
}
.badge--green {
  background: #dcfce7;
  color: #15803d;
}
.badge--orange {
  background: #fff7ed;
  color: #c2410c;
}

/* ── Autocomplete ── */
.ac-wrap {
  position: relative;
}

.ac-dropdown {
  position: absolute;
  top: calc(100% + 2px);
  left: 0;
  right: 0;
  z-index: 200;
  background: #fff;
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  box-shadow: 0 4px 16px rgba(0, 0, 0, 0.1);
  margin: 0;
  padding: 4px 0;
  list-style: none;
  max-height: 200px;
  overflow-y: auto;
}

.ac-dropdown li {
  padding: 8px 12px;
  font-size: 13px;
  color: #374151;
  cursor: pointer;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  transition: background 0.1s;
}

.ac-dropdown li:hover {
  background: #fff7ed;
  color: #c2410c;
}

/* ── Skeleton loading ── */
@keyframes shimmer {
  0% {
    background-position: -400px 0;
  }
  100% {
    background-position: 400px 0;
  }
}

.skel {
  border-radius: 6px;
  background: linear-gradient(90deg, #e2e8f0 25%, #f1f5f9 50%, #e2e8f0 75%);
  background-size: 800px 100%;
  animation: shimmer 1.4s infinite linear;
}

.skel--value {
  height: 36px;
  width: 70%;
  margin: 4px 0 6px;
}
.skel--sub {
  height: 14px;
  width: 50%;
  margin-bottom: 6px;
}
.skel--dates {
  height: 12px;
  width: 40%;
}

/* ── Filtros en cascada: chips de sugerencias rápidas ── */
.label-available {
  font-size: 10px;
  font-weight: 600;
  color: #fff;
  background: #f97316;
  border-radius: 10px;
  padding: 1px 7px;
  text-transform: none;
  letter-spacing: 0;
  margin-left: 6px;
}

.quick-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
  margin-top: 5px;
  max-height: 80px;
  overflow-y: auto;
}

.quick-chip {
  background: #fff7ed;
  color: #c2410c;
  border: 1px solid #fed7aa;
  border-radius: 20px;
  padding: 2px 10px;
  font-size: 11.5px;
  cursor: pointer;
  transition:
    background 0.12s,
    border-color 0.12s;
  white-space: nowrap;
}
.quick-chip:hover {
  background: #ffedd5;
  border-color: #f97316;
}

/* ── Top marcas section ── */
.top-marcas-section {
  display: flex;
  flex-direction: column;
  gap: 12px;
}
.top-marcas-header {
  display: flex;
  align-items: baseline;
  gap: 10px;
}
.top-marcas-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 16px;
}
.top-marcas-card {
}
.top-marcas-card-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 12px 16px;
  background: #1e293b;
  border-radius: 0;
}
.top-marcas-icon {
  font-size: 16px;
}
.top-marcas-fuente {
  font-size: 13px;
  font-weight: 700;
  color: #fff;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

/* Tabla unificada horizontal */
.top-marcas-unified thead th {
  padding: 10px 12px;
  white-space: nowrap;
}
.top-marcas-unified tbody td {
  padding: 9px 12px;
  vertical-align: middle;
}

.top-marcas-unified .tm-group-row th {
  padding: 8px 12px;
  font-size: 12px;
}
.tm-group-th {
  background: #0f172a;
  text-align: center;
  letter-spacing: 1px;
}
.col-divider-th,
.col-divider-td {
  width: 2px;
  padding: 0 !important;
  background: #334155;
}
.col-rank {
  width: 32px;
}
.col-marca {
  min-width: 140px;
}
.col-inv {
  width: 140px;
  white-space: nowrap;
}

.tm-marca {
  font-size: 12.5px;
}
.tm-anunciante {
  font-size: 11px;
}
.tm-inv {
  font-size: 12.5px;
}
.tm-regs {
  font-size: 11px;
}
.tm-empty {
  text-align: center;
}

/* ── Shared table extras ── */
.table-header {
  display: flex;
  align-items: baseline;
  gap: 12px;
  padding: 14px 18px;
  border-bottom: 1px solid #f1f5f9;
}
.table-title {
  font-size: 14px;
  font-weight: 700;
  color: #1e293b;
}
.table-sub {
  font-size: 12px;
  color: #94a3b8;
}
.td-rank {
  width: 32px;
  text-align: center;
  font-size: 12px;
  font-weight: 700;
  color: #94a3b8;
}
.td-inversion {
  color: #15803d;
  font-weight: 600;
}

/* ── Detail tables ── */
.detail-table-section {
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.detail-table-header {
  display: flex;
  align-items: baseline;
  gap: 12px;
}
.detail-table-header .table-sub {
  font-size: 12px;
  color: #64748b;
}

/* Wrapper card: scroll horizontal + altura máxima */
.detail-table-section > .card {
  max-height: 520px;
  overflow: auto;
}

.detail-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12.5px;
}
.detail-table thead tr {
  background: #1e293b;
}
.detail-table thead th {
  position: sticky;
  top: 0;
  z-index: 2;
  background: #1e293b;
  padding: 10px 12px;
  text-align: left;
  font-size: 10.5px;
  font-weight: 600;
  color: #fff;
  text-transform: uppercase;
  letter-spacing: 0.4px;
  white-space: nowrap;
}
.detail-table tbody tr {
  border-bottom: 1px solid #f1f5f9;
  transition: background 0.12s;
}
.detail-table tbody tr:last-child {
  border-bottom: none;
}
.detail-table tbody tr:hover {
  background: #fffbeb;
}
.detail-table tbody td {
  padding: 7px 12px;
  white-space: nowrap;
  max-width: 280px;
  overflow: hidden;
  text-overflow: ellipsis;
  color: #334155;
}
.detail-table tbody tr:nth-child(even) {
  background: #f8fafc;
}
.detail-table tbody tr:nth-child(even):hover {
  background: #fffbeb;
}

.detail-skel-wrap {
  padding: 12px 0;
}
.detail-empty {
  padding: 32px;
  text-align: center;
  color: #94a3b8;
  font-size: 13px;
}
.pagination-bar {
  display: flex;
  align-items: center;
  justify-content: flex-start;
  gap: 16px;
  padding: 10px 16px;
  border: 1px solid #e2e8f0;
  border-top: none;
  border-radius: 0 0 12px 12px;
  font-size: 13px;
  color: #475569;
  background: #fff;
}
.pagination-bar button {
  padding: 6px 16px;
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  background: #fff;
  color: #334155;
  font-size: 12px;
  font-weight: 500;
  cursor: pointer;
  transition:
    background 0.15s,
    border-color 0.15s;
}
.pagination-bar button:hover:not(:disabled) {
  background: #fff7ed;
  border-color: #f97316;
  color: #f97316;
}
.pagination-bar button:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

/* ── Gráficas por sector ── */
.sector-charts-section {
  display: flex;
  flex-direction: column;
  gap: 12px;
}
.sector-charts-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 16px;
}
.sector-chart-card {
  background: #fff;
  border-radius: 10px;
  border: 1px solid #e2e8f0;
  overflow: hidden;
  box-shadow: 0 1px 3px rgba(0,0,0,.06);
}
.sector-chart-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 16px;
  background: #1e293b;
}
.sector-chart-icon {
  font-size: 16px;
}
.sector-chart-title {
  font-size: 13px;
  font-weight: 700;
  color: #fff;
  letter-spacing: 0.5px;
}
.sector-chart-wrap {
  padding: 16px;
  height: 320px;
  position: relative;
}
.sector-chart-skel {
  padding: 24px 16px;
  display: flex;
  align-items: center;
  justify-content: center;
}
.sector-chart-empty {
  padding: 40px 16px;
  text-align: center;
  color: #94a3b8;
  font-size: 13px;
}
</style>
