const mainGridApis = {};
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
 * Given a knowledge base (kb), get all query stats for each engine to display
 * on the main page of evaluation web app as a table.
 *
 * @param {Object<string, Object<string, any>>} performanceData - The performance data for all KBs and engines
 * @param {string} kb - The knowledge base key to extract data for
 * @returns {Object<string, Array>} Object mapping metric keys and engine names to arrays of values
 */
function getAllQueryStatsByKb(performanceData, kb) {
    const enginesDict = performanceData[kb];
    const enginesDictForTable = { engine_name: [] };

    // Initialize arrays for all metric keys
    Object.keys(engineMetrics).forEach((key) => {
        enginesDictForTable[key] = [];
    });

    for (const [engine, engineStats] of Object.entries(enginesDict)) {
        enginesDictForTable.engine_name.push(capitalize(engine));
        for (const metricKey of Object.keys(engineMetrics)) {
            enginesDictForTable[metricKey].push(engineStats[metricKey]);
        }
    }
    return enginesDictForTable;
}

function setMainPageEvents() {
    const showMetricsContainer = document.querySelector("#showMetricsContainer");
    showMetricsContainer.addEventListener("change", (event) => {
        if (event.target && event.target.matches('input[type="checkbox"]')) {
            const metricsToDisplay = Array.from(
                showMetricsContainer.querySelectorAll('input[type="checkbox"]:checked')
            ).map((cb) => cb.value);
            const metricsToHide = Object.keys(engineMetrics).filter((metric) => !metricsToDisplay.includes(metric));
            for (const mainGridApi of Object.values(mainGridApis)) {
                mainGridApi.setColumnsVisible(metricsToDisplay, true);
                mainGridApi.setColumnsVisible(metricsToHide, false);
            }
        }
    });
}

/**
 * Returns ag-Grid gridOptions for comparing SPARQL engines for a given knowledge base.
 *
 * This grid displays various metrics like average time, failure rate, etc.
 * It applies proper formatting and filters based on the type of each metric.
 * @returns {Array<Object>} ag-Grid gridOptions object
 */
function mainTableColumnDefs() {
    // Define custom formatting and filters based on column keys
    return [
        {
            headerName: "System",
            field: "engine_name",
            filter: "agTextColumnFilter",
            headerTooltip: "Name of the RDF graph database being benchmarked.",
            tooltipComponent: CustomDetailsTooltip,
            flex: 1.1,
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
            flex: 1,
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
            flex: 1,
        },
        {
            headerName: "> 5s",
            field: "over5s",
            filter: "agNumberColumnFilter",
            type: "numericColumn",
            valueFormatter: ({ value }) => (value != null ? `${value.toFixed(2)} %` : "N/A"),
            headerTooltip: "Percentage of all queries that successfully completed in more than 5 seconds",
            tooltipComponent: CustomDetailsTooltip,
            flex: 1,
        },
    ];
}

function updateMainPage(performanceData, additionalData) {
    document.querySelector("#main-page-header").innerHTML = additionalData.title;
    const container = document.getElementById("main-table-container");
    removeTitleInfoPill();

    // Clear container if any existing content
    container.innerHTML = "";
    const fragment = document.createDocumentFragment();

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

    const showMetricsContainer = document.querySelector("#showMetricsContainer");
    showMetricsContainer.innerHTML = "";
    showMetricsContainer.appendChild(getColumnVisibilityMultiSelectFragment(engineMetrics));

    // For each knowledge base (kb) key in performanceData
    for (const kb of sortedKbNames) {
        // Create section wrapper
        const section = document.createElement("div");
        section.className = "mb-4";

        // Header with KB name and a compare button
        const header = document.createElement("div");
        header.className = "kg-header px-2 px-md-0";

        // const titleWrapper = document.createElement("div");
        // titleWrapper.className = "d-inline-flex align-items-center";

        const benchmarkDescription = additionalData.kbs[kb].description;
        const benchmarkName = additionalData.kbs[kb].name;

        const title = document.createElement("div");
        title.textContent = benchmarkName || capitalize(kb);
        // title.style.fontWeight = "bold";
        title.classList.add("fs-5", "fw-bold", "mb-1");

        let infoPill = null;
        if (benchmarkDescription) {
            infoPill = createBenchmarkDescriptionInfoPill(benchmarkDescription);
        }

        const btnGroup = document.createElement("div");
        btnGroup.className = "d-flex align-items-center gap-2";

        const downloadBtn = document.createElement("button");
        downloadBtn.className = "btn btn-outline-theme btn-sm";
        const downloadIcon = document.createElement("i");
        downloadIcon.className = "bi bi-download";
        downloadBtn.appendChild(downloadIcon);
        downloadBtn.title = "Download as TSV";
        downloadBtn.onclick = () => {
            if (!mainGridApis || !mainGridApis.hasOwnProperty(kb)) {
                alert(`The aggregate metrics table for ${kb} could not be downloaded!`);
                return;
            }
            mainGridApis[kb].exportDataAsCsv({
                fileName: `${kb}_aggregate_metrics.tsv`,
                columnSeparator: "\t",
            });
        };

        const compareBtn = document.createElement("button");
        compareBtn.className = "btn btn-outline-theme btn-sm";
        compareBtn.innerHTML = `<i class="bi bi-table d-inline d-md-none"></i><span class="d-none d-md-inline">Detailed Results per Query</span>`;
        compareBtn.title = "Compare per-query results";
        compareBtn.onclick = () => {
            router.navigate(`/comparison?kb=${encodeURIComponent(kb)}`);
        };

        btnGroup.appendChild(downloadBtn);
        btnGroup.appendChild(compareBtn);

        // titleWrapper.appendChild(title);
        if (infoPill) {
            title.appendChild(infoPill);
            new bootstrap.Popover(infoPill);
        }
        header.appendChild(title);
        header.appendChild(btnGroup);

        const html = document.documentElement;
        const currentTheme = html.getAttribute("data-bs-theme") || "light";

        // Grid div with ag-theme-balham styling
        const gridDiv = document.createElement("div");
        gridDiv.className = currentTheme === "light" ? "ag-theme-balham" : "ag-theme-balham-dark";
        gridDiv.style.width = "100%";

        // Append header and grid div to section
        section.appendChild(header);
        section.appendChild(gridDiv);
        fragment.appendChild(section);

        // Get table data from function you provided
        const tableData = getAllQueryStatsByKb(performanceData, kb);

        // Prepare row data as array of objects for ag-grid
        // tableData is {colName: [val, val, ...], ...}
        // We convert to [{engine_name: ..., ameanTime: ..., ...}, ...]
        const rowCount = tableData.engine_name.length;
        const rowData = getGridRowData(rowCount, tableData);

        const onRowClicked = (event) => {
            const engine = event.data.engine_name.toLowerCase();
            router.navigate(`/details?kb=${encodeURIComponent(kb)}&engine=${encodeURIComponent(engine)}`);
        };

        // const penaltyFactor = additionalData.penalty?.toString() ?? "Penalty Factor";

        // Initialize ag-Grid instance
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
            },
        });
    }
    container.appendChild(fragment);
}

/**
 * Handles light/dark theme management including:
 * - Setting the preferred theme based on system settings
 * - Toggling between light and dark modes on button click
 * - Updating Bootstrap and Ag-Grid theme classes
 * - Adjusting toggle button icon and title
 */
function initThemeManager() {
    const themeToggleBtn = document.getElementById("themeToggleBtn");
    const themeToggleIcon = document.getElementById("themeToggleIcon");
    const html = document.documentElement;

    /**
     * Updates the current theme across UI components.
     * @param {string} theme - The theme to apply ("light" or "dark").
     */
    function applyTheme(theme) {
        html.setAttribute("data-bs-theme", theme);
        themeToggleIcon.className = theme === "light" ? "bi bi-moon-fill" : "bi bi-sun-fill";
        themeToggleBtn.title = `Click to change to ${theme === "light" ? "dark" : "light"} mode!`;

        const grids = document.querySelectorAll(".ag-theme-balham, .ag-theme-balham-dark");
        grids.forEach((grid) => {
            grid.classList.toggle("ag-theme-balham", theme === "light");
            grid.classList.toggle("ag-theme-balham-dark", theme === "dark");
        });
    }

    /**
     * Detects and applies the user's preferred color scheme.
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

    // Initialize preferred theme
    applyPreferredTheme();

    // Attach click listener to toggle button
    themeToggleBtn.addEventListener("click", toggleTheme);
}

document.addEventListener("DOMContentLoaded", async () => {
    router = new Navigo("/", { hash: true });

    initThemeManager();

    try {
        showSpinner();
        const yaml_path = window.location.origin + window.location.pathname.replace(/\/$/, "").replace(/\/[^/]*$/, "/");
        const response = await fetch(`${yaml_path}yaml_data`);
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const data = await response.json();
        performanceData = data.performance_data;
        const additionalData = data.additional_data;

        for (const kb in performanceData) {
            // Gather all index stats for this KB
            const times = Object.values(performanceData[kb]).map((e) => e?.indexTime ?? null);
            const sizes = Object.values(performanceData[kb]).map((e) => e?.indexSize ?? null);
            const { unit: timeUnit, factor: timeFactor } = pickTimeUnit(times);
            const { unit: sizeUnit, factor: sizeFactor } = pickSizeUnit(sizes);

            for (const engine in performanceData[kb]) {
                const engineObj = performanceData[kb][engine];
                engineObj.indexTime = formatIndexStat(engineObj.indexTime, timeFactor, timeUnit);
                engineObj.indexSize = formatIndexStat(engineObj.indexSize, sizeFactor, sizeUnit);

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

        // Routes
        router
            .on({
                "/": () => {
                    showPage("main");
                    updateMainPage(performanceData, additionalData);
                },
                "/details": (params) => {
                    const kb = params.params.kb;
                    const engine = params.params.engine;
                    if (
                        !Object.keys(performanceData).includes(kb) ||
                        !Object.keys(performanceData[kb]).includes(engine)
                    ) {
                        showPage(
                            "error",
                            `Query Details Page not found for ${engine} (${kb}) -> Make sure the url is correct!`
                        );
                        return;
                    }
                    updateDetailsPage(performanceData, kb, engine, additionalData.kbs[kb]);
                    showPage("details");
                },
                "/comparison": (params) => {
                    const kb = params.params.kb;
                    if (!Object.keys(performanceData).includes(kb)) {
                        showPage(
                            "error",
                            `Performance Comparison Page not found for ${capitalize(
                                kb
                            )} -> Make sure the url is correct!`
                        );
                        return;
                    }
                    updateComparisonPage(performanceData, kb, additionalData.kbs[kb]);
                    showPage("comparison");
                },
                "/compareExecTrees": (params) => {
                    const kb = params.params.kb;
                    const queryIdx = params.params.q;
                    if (!Object.keys(performanceData).includes(kb)) {
                        showPage(
                            "error",
                            `Query Execution Tree Page not found for ${capitalize(kb)} -> Make sure the url is correct!`
                        );
                        return;
                    }
                    const queryToEngineStats = getQueryToEngineStatsDict(performanceData[kb]);
                    if (
                        isNaN(parseInt(queryIdx)) ||
                        parseInt(queryIdx) < 0 ||
                        parseInt(queryIdx) >= Object.keys(queryToEngineStats).length
                    ) {
                        showPage(
                            "error",
                            `Query Execution Tree Page not found as the requested query is not available for ${capitalize(
                                kb
                            )} -> Make sure the parameter q in the url is correct!`
                        );
                        return;
                    }
                    const execTreeEngines = getEnginesWithExecTrees(performanceData[kb]);
                    const query = Object.keys(queryToEngineStats)[queryIdx];

                    const engineStatForQuery = Object.fromEntries(
                        Object.entries(queryToEngineStats[query]).filter(([engine]) => execTreeEngines.includes(engine))
                    );
                    updateCompareExecTreesPage(kb, query, engineStatForQuery);
                    showPage("compareExecTrees");
                },
            })
            .notFound(() => {
                showPage("main");
                updateMainPage(performanceData, additionalData);
            });

        router.resolve();

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
