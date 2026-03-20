/**
 * Main entry point for the RDF Graph Database Performance Evaluation web app.
 * Handles routing, data fetching, theme management, and the aggregate metric tables display.
 */

/** @type {Object<string, Object>} Stores ag-Grid API instances keyed by knowledge base */
const mainGridApis = {};

/**
 * Mapping of metric keys to their display names for the main comparison table.
 * P=2 and P=10 refer to penalty factors applied to failed queries when computing the metric.
 * @type {Object<string, string>}
 */
const engineMetrics = {
    gmeanTime2: "Geom. Mean (P=2)",
    gmeanTime10: "Geom. Mean (P=10)",
    medianTime: "Median (P=2)",
    ameanTime: "Arith. Mean (P=2)",
    indexTime: "Index time",
    indexSize: "Index size",
    failed: "Failed",
    under1s: "<= 1s",
    between1to5s: "(1s, 5s]",
    over5s: "> 5s",
};

/**
 * Sets up event listeners for the main page.
 */
function setMainPageEvents() {
    // Allows users to show/hide metric columns across all KB tables.
    document.querySelector("#showMetricsContainer").addEventListener("change", applyMetricVisibility);
}

/**
 * Extracts aggregate metrics for each engine on the given knowledge base
 * into a columnar format suitable for ag-Grid.
 *
 * @param {Object<string, Object<string, any>>} performanceData - The performance data for all KBs and engines
 * @param {string} kb - The knowledge base key to extract data for
 * @returns {Object<string, Array>} Columnar data with engine_name and metric keys mapped to arrays of values
 */
function getAggregateMetricsByKb(performanceData, kb) {
    const enginesByName = performanceData[kb];
    const engineMetricsByKb = { engine_name: [] };

    // Initialize arrays for all metric keys
    Object.keys(engineMetrics).forEach((key) => {
        engineMetricsByKb[key] = [];
    });

    for (const [engine, engineStats] of Object.entries(enginesByName)) {
        engineMetricsByKb.engine_name.push(capitalize(engine));
        for (const metricKey of Object.keys(engineMetrics)) {
            engineMetricsByKb[metricKey].push(engineStats[metricKey]);
        }
    }
    return engineMetricsByKb;
}

/**
 * Reads the current state of the metric visibility checkboxes and updates
 * all main page ag-Grid instances to show/hide columns accordingly.
 */
function applyMetricVisibility() {
    const showMetricsContainer = document.querySelector("#showMetricsContainer");
    const metricsToDisplay = Array.from(showMetricsContainer.querySelectorAll('input[type="checkbox"]:checked')).map(
        (cb) => cb.value,
    );
    const metricsToHide = Object.keys(engineMetrics).filter((metric) => !metricsToDisplay.includes(metric));
    for (const mainGridApi of Object.values(mainGridApis)) {
        mainGridApi.setColumnsVisible(metricsToDisplay, true);
        mainGridApi.setColumnsVisible(metricsToHide, false);
    }
}

/**
 * Returns ag-Grid column definitions for the aggregate metrics table.
 * Configures column headers, formatters, filters, and tooltips for each metric.
 *
 * @returns {Array<Object>} Array of ag-Grid column definition objects
 */
function mainTableColumnDefs() {
    return [
        {
            headerName: "System",
            field: "engine_name",
            filter: "agTextColumnFilter",
            headerTooltip: "Name of the RDF graph database being benchmarked.",
            tooltipComponent: CustomDetailsTooltip,
            flex: 1.25,
        },
        {
            headerName: "Geom. Mean (P=2)",
            field: "gmeanTime2",
            filter: "agNumberColumnFilter",
            type: "numericColumn",
            valueFormatter: ({ value }) => (value != null ? `${value.toFixed(2)} s` : "N/A"),
            headerTooltip: `Geometric mean of all query runtimes. Failed queries are penalized with a runtime of timeout × 2`,
            tooltipComponent: CustomDetailsTooltip,
            flex: 1.5,
        },
        {
            headerName: "Geom. Mean (P=10)",
            field: "gmeanTime10",
            filter: "agNumberColumnFilter",
            type: "numericColumn",
            valueFormatter: ({ value }) => (value != null ? `${value.toFixed(2)} s` : "N/A"),
            headerTooltip: `Geometric mean of all query runtimes. Failed queries are penalized with a runtime of timeout × 10`,
            tooltipComponent: CustomDetailsTooltip,
            flex: 1.5,
        },
        {
            headerName: "Median (P=2)",
            field: "medianTime",
            filter: "agNumberColumnFilter",
            type: "numericColumn",
            valueFormatter: ({ value }) => (value != null ? `${value.toFixed(2)} s` : "N/A"),
            headerTooltip: `Median runtime of all queries. Failed queries are penalized with a runtime of timeout × 2`,
            tooltipComponent: CustomDetailsTooltip,
            flex: 1.25,
        },
        {
            headerName: "Arith. Mean (P=2)",
            field: "ameanTime",
            filter: "agNumberColumnFilter",
            type: "numericColumn",
            valueFormatter: ({ value }) => (value != null ? `${value.toFixed(2)} s` : "N/A"),
            headerTooltip: `Arithmetic mean of all query runtimes. Failed queries are penalized with a runtime of timeout × 2`,
            tooltipComponent: CustomDetailsTooltip,
            flex: 1.4,
        },
        {
            headerName: "Index time",
            field: "indexTime",
            filter: "agNumberColumnFilter",
            type: "numericColumn",
            valueFormatter: ({ value }) => (value != null ? value : "N/A"),
            headerTooltip: `Total indexing time for the system on the benchmark dataset`,
            tooltipComponent: CustomDetailsTooltip,
            flex: 1,
        },
        {
            headerName: "Index size",
            field: "indexSize",
            filter: "agNumberColumnFilter",
            type: "numericColumn",
            valueFormatter: ({ value }) => (value != null ? value : "N/A"),
            headerTooltip: `Total index size used by the system for the benchmark dataset`,
            tooltipComponent: CustomDetailsTooltip,
            flex: 1,
        },
        {
            headerName: "Failed",
            field: "failed",
            filter: "agNumberColumnFilter",
            type: "numericColumn",
            valueFormatter: ({ value }) => (value != null ? `${value.toFixed(2)} %` : "N/A"),
            headerTooltip: "Percentage of queries that failed to return results.",
            tooltipComponent: CustomDetailsTooltip,
            flex: 1,
        },
        {
            headerName: "<= 1s",
            field: "under1s",
            filter: "agNumberColumnFilter",
            type: "numericColumn",
            valueFormatter: ({ value }) => (value != null ? `${value.toFixed(2)} %` : "N/A"),
            headerTooltip: "Percentage of all queries that successfully finished in 1 second or less",
            tooltipComponent: CustomDetailsTooltip,
            flex: 0.8,
        },
        {
            headerName: "(1s, 5s]",
            field: "between1to5s",
            filter: "agNumberColumnFilter",
            type: "numericColumn",
            valueFormatter: ({ value }) => (value != null ? `${value.toFixed(2)} %` : "N/A"),
            headerTooltip:
                "Percentage of all queries that successfully completed in more than 1 second and up to 5 seconds",
            tooltipComponent: CustomDetailsTooltip,
            flex: 0.8,
        },
        {
            headerName: "> 5s",
            field: "over5s",
            filter: "agNumberColumnFilter",
            type: "numericColumn",
            valueFormatter: ({ value }) => (value != null ? `${value.toFixed(2)} %` : "N/A"),
            headerTooltip: "Percentage of all queries that successfully completed in more than 5 seconds",
            tooltipComponent: CustomDetailsTooltip,
            flex: 0.8,
        },
    ];
}

/**
 * Updates the main page with performance comparison tables for all knowledge bases.
 * Creates an ag-Grid table for each KB with aggregate metrics per engine.
 *
 * @param {Object} performanceData - Performance data for all KBs and engines
 * @param {Object} additionalData - Metadata including KB names, descriptions, and page title
 */
function updateMainPage(performanceData, additionalData) {
    document.querySelector("#main-page-header").innerHTML = additionalData.title;
    const container = document.getElementById("main-table-container");
    removeTitleInfoPill();

    // Skip re-rendering if the page has already been populated
    if (Object.keys(mainGridApis).length > 0) return;

    // Clear container if any existing content
    container.innerHTML = "";
    const fragment = document.createDocumentFragment();

    // Sort KBs by scale (ascending), then by name alphabetically
    const sortedKbNames = Object.entries(additionalData.kbs)
        .sort(([keyA, kbA], [keyB, kbB]) => {
            const scaleA = kbA?.scale ?? 0;
            const scaleB = kbB?.scale ?? 0;
            const nameA = kbA?.name ?? "";
            const nameB = kbB?.name ?? "";

            if (scaleB !== scaleA) return scaleA - scaleB;
            return nameA.localeCompare(nameB);
        })
        .map(([key, _kb]) => key);

    // Populate the metrics visibility checkboxes
    const showMetricsContainer = document.querySelector("#showMetricsContainer");
    showMetricsContainer.innerHTML = "";
    showMetricsContainer.appendChild(getColumnVisibilityMultiSelectFragment(engineMetrics));

    // On small screens, uncheck all but the most important metrics before grid creation
    if (!window.matchMedia("(min-width: 768px)").matches) {
        const keepOnSmall = ["gmeanTime2", "medianTime", "failed"];
        showMetricsContainer.querySelectorAll('input[type="checkbox"]').forEach((cb) => {
            if (!keepOnSmall.includes(cb.value)) {
                cb.checked = false;
            }
        });
    }

    // Clone the HTML template for each knowledge base section
    const template = document.getElementById("kb-section-template");
    const currentTheme = document.documentElement.getAttribute("data-bs-theme") || "light";
    const gridThemeClass = currentTheme === "light" ? "ag-theme-balham" : "ag-theme-balham-dark";

    for (const kb of sortedKbNames) {
        const section = template.content.firstElementChild.cloneNode(true);
        section.dataset.kb = kb;

        // Set benchmark title and optional info pill
        const titleEl = section.querySelector(".kb-title");
        titleEl.textContent = additionalData.kbs[kb].name || capitalize(kb);
        if (additionalData.kbs[kb].description) {
            const infoPill = createBenchmarkDescriptionInfoPill(additionalData.kbs[kb].description);
            titleEl.appendChild(infoPill);
            new bootstrap.Popover(infoPill);
        }

        // Navigate to execution tree comparison page
        const execTreeBtn = section.querySelector(".kb-exec-tree-btn");
        const execTreeEngines = getEnginesWithExecTrees(performanceData[kb]);
        setCompareExecTreesBtnState(execTreeBtn, execTreeEngines.length >= 2);
        execTreeBtn.addEventListener("click", () => {
            router.navigate(`/compareExecTrees?kb=${encodeURIComponent(kb)}&q=0`);
        });

        // Download as TSV button
        section.querySelector(".kb-download-btn").onclick = () => {
            if (!mainGridApis || !mainGridApis.hasOwnProperty(kb)) {
                alert(`The aggregate metrics table for ${kb} could not be downloaded!`);
                return;
            }
            mainGridApis[kb].exportDataAsCsv({
                fileName: `${kb}_aggregate_metrics.tsv`,
                columnSeparator: "\t",
            });
        };

        // Navigate to detailed results per query comparison page button
        section.querySelector(".kb-compare-btn").onclick = () => {
            router.navigate(`/comparison?kb=${encodeURIComponent(kb)}`);
        };

        // Apply current theme to the grid container
        const gridDiv = section.querySelector(".kb-grid");
        gridDiv.classList.add(gridThemeClass);

        fragment.appendChild(section);

        // Transform columnar data to row format for ag-Grid
        const tableData = getAggregateMetricsByKb(performanceData, kb);
        const rowCount = tableData.engine_name.length;
        const rowData = getGridRowData(rowCount, tableData);

        // Row click navigates to engine details page
        const onRowClicked = (event) => {
            const engine = event.data.engine_name.toLowerCase();
            router.navigate(`/details?kb=${encodeURIComponent(kb)}&engine=${encodeURIComponent(engine)}`);
        };

        // Initialize ag-Grid instance for this KB
        agGrid.createGrid(gridDiv, {
            columnDefs: mainTableColumnDefs(),
            rowData: rowData,
            defaultColDef: {
                sortable: true,
                filter: true,
                resizable: true,
                minWidth: 80,
            },
            domLayout: "autoHeight",
            rowStyle: { fontSize: "clamp(12px, 1vw + 8px, 14px)", cursor: "pointer" },
            tooltipShowDelay: 500,
            onRowClicked: onRowClicked,
            suppressDragLeaveHidesColumns: true,
            onGridReady: (params) => {
                mainGridApis[kb] = params.api;
                applyMetricVisibility();
            },
        });
    }
    container.appendChild(fragment);
}

/**
 * Initializes the light/dark theme manager.
 * Handles system preference detection, theme toggling, and updating
 * Bootstrap and ag-Grid theme classes accordingly.
 */
function initThemeManager() {
    const themeToggleBtn = document.getElementById("themeToggleBtn");
    const themeToggleIcon = document.getElementById("themeToggleIcon");
    const html = document.documentElement;

    /**
     * Applies the specified theme across all UI components.
     * @param {string} theme - The theme to apply ("light" or "dark")
     */
    function applyTheme(theme) {
        html.setAttribute("data-bs-theme", theme);
        themeToggleIcon.className = theme === "light" ? "bi bi-moon-fill" : "bi bi-sun-fill";
        themeToggleBtn.title = `Click to change to ${theme === "light" ? "dark" : "light"} mode!`;

        // Update all ag-Grid instances to match the theme
        const grids = document.querySelectorAll(".ag-theme-balham, .ag-theme-balham-dark");
        grids.forEach((grid) => {
            grid.classList.toggle("ag-theme-balham", theme === "light");
            grid.classList.toggle("ag-theme-balham-dark", theme === "dark");
        });
    }

    /**
     * Detects and applies the user's preferred color scheme from system settings.
     */
    function applyPreferredTheme() {
        const prefersDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
        applyTheme(prefersDark ? "dark" : "light");
    }

    /**
     * Toggles the theme between light and dark.
     */
    function toggleTheme() {
        const currentTheme = html.getAttribute("data-bs-theme") || "light";
        const newTheme = currentTheme === "light" ? "dark" : "light";
        applyTheme(newTheme);
    }

    // Initialize with user's preferred theme and listen for system changes
    applyPreferredTheme();
    window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", applyPreferredTheme);

    // Attach click listener to toggle button
    themeToggleBtn.addEventListener("click", toggleTheme);
}

/**
 * Preprocess the global `performanceData` object at webapp startup
 * Update the index time and size for each engine for every kb to be a formatted string with appropriate unit.
 * Format the full sparql query text for all queries using spfmt
 */
function preProcessPerformanceData() {
    for (const kb in performanceData) {
        // Determine appropriate units for index time and size across all engines
        const times = Object.values(performanceData[kb]).map((e) => e?.indexTime ?? null);
        const sizes = Object.values(performanceData[kb]).map((e) => e?.indexSize ?? null);
        const { unit: timeUnit, factor: timeFactor } = pickTimeUnit(times);
        const { unit: sizeUnit, factor: sizeFactor } = pickSizeUnit(sizes);

        for (const engine in performanceData[kb]) {
            const engineObj = performanceData[kb][engine];

            // Format index statistics with appropriate units
            engineObj.indexTime = formatIndexStat(engineObj.indexTime, timeFactor, timeUnit);
            engineObj.indexSize = formatIndexStat(engineObj.indexSize, sizeFactor, sizeUnit);

            // Format SPARQL queries for better readability
            const queries = engineObj.queries;
            if (Array.isArray(queries)) {
                queries.forEach((query) => {
                    try {
                        query.sparql = spfmt.format(query.sparql);
                    } catch (err) {
                        console.log(err);
                    }
                });
            }
        }
    }
}

/**
 * Application initialization on DOM ready.
 * - Sets up Navigo router with hash-based routing
 * - Fetches and processes benchmark data from server
 * - Configures routes for main, details, comparison, and execution tree pages
 * - Initializes page event handlers
 */
document.addEventListener("DOMContentLoaded", async () => {
    router = new Navigo("/", { hash: true });

    initThemeManager();

    try {
        showSpinner();

        // Construct the API URL relative to current path
        const yaml_path = window.location.origin + window.location.pathname.replace(/\/$/, "").replace(/\/[^/]*$/, "/");
        const response = await fetch(`${yaml_path}yaml_data`);
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const data = await response.json();
        performanceData = data.performance_data;
        const additionalData = data.additional_data;

        // Pre-process performance data: format index stats and SPARQL queries
        preProcessPerformanceData();

        // Configure application routes
        router
            .on({
                // Main page: shows aggregate metrics for all engines grouped by KBs.
                "/": () => {
                    showPage("main");
                    updateMainPage(performanceData, additionalData);
                },

                // Details page: per-query results for a specific engine
                "/details": (params) => {
                    const kb = params.params.kb;
                    const engine = params.params.engine;
                    if (
                        !Object.keys(performanceData).includes(kb) ||
                        !Object.keys(performanceData[kb]).includes(engine)
                    ) {
                        showPage(
                            "error",
                            `Query Details Page not found for ${engine} (${kb}) -> Make sure the url is correct!`,
                        );
                        return;
                    }
                    updateDetailsPage(performanceData, kb, engine, additionalData.kbs[kb]);
                    showPage("details");
                },

                // Comparison page: detailed per-query side-by-side engine comparison for a KB
                "/comparison": (params) => {
                    const kb = params.params.kb;
                    if (!Object.keys(performanceData).includes(kb)) {
                        showPage(
                            "error",
                            `Performance Comparison Page not found for ${capitalize(
                                kb,
                            )} -> Make sure the url is correct!`,
                        );
                        return;
                    }
                    updateComparisonPage(performanceData, kb, additionalData.kbs[kb]);
                    showPage("comparison");
                },

                // Execution tree comparison page: compare query plans across 2 QLever instances
                "/compareExecTrees": (params) => {
                    const kb = params.params.kb;
                    const queryIdx = params.params.q;
                    if (!Object.keys(performanceData).includes(kb)) {
                        showPage(
                            "error",
                            `Query Execution Tree Page not found for ${capitalize(kb)} -> Make sure the url is correct!`,
                        );
                        return;
                    }
                    const queryToEngineStats = getQueryToEngineStatsMap(performanceData[kb]);
                    if (
                        isNaN(parseInt(queryIdx)) ||
                        parseInt(queryIdx) < 0 ||
                        parseInt(queryIdx) >= Object.keys(queryToEngineStats).length
                    ) {
                        showPage(
                            "error",
                            `Query Execution Tree Page not found as the requested query is not available for ${capitalize(
                                kb,
                            )} -> Make sure the parameter q in the url is correct!`,
                        );
                        return;
                    }
                    const execTreeEngines = getEnginesWithExecTrees(performanceData[kb]);
                    const query = Object.keys(queryToEngineStats)[queryIdx];

                    // Filter all queries to only include engines with execution tree data
                    const execTreeQueryStats = {};
                    for (const [q, engines] of Object.entries(queryToEngineStats)) {
                        execTreeQueryStats[q] = {};
                        for (const engine of execTreeEngines) {
                            if (engines[engine]) {
                                execTreeQueryStats[q][engine] = engines[engine];
                            }
                        }
                    }
                    updateCompareExecTreesPage(kb, query, execTreeQueryStats);
                    showPage("compareExecTrees");
                    requestAnimationFrame(() => renderCompareExecTrees());
                },
            })
            .notFound(() => {
                // Fallback to main page for unknown routes
                showPage("main");
                updateMainPage(performanceData, additionalData);
            });

        router.resolve();

        // Initialize event handlers for all pages
        setMainPageEvents();
        setDetailsPageEvents();
        setComparisonPageEvents();
        setCompareExecTreesEvents();
    } catch (err) {
        console.error("Error loading /yaml_data:", err);
        showPage("error");
    } finally {
        hideSpinner();
    }
});
