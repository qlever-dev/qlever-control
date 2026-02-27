/**
 * Comparison page functionality for detailed side-by-side per-query comparison of all engines.
 * Displays per-query runtimes with visual indicators for best performance, failures,
 * and result size discrepancies.
 */

/** @type {Object|null} ag-Grid API instance for the comparison table */
let gridApi;

/**
 * Sets up event listeners for the comparison page, including:
 * - Table column visibility checkboxes
 * - Column ordering dropdown
 * - Show aggregate metrics toggle
 * - Show result size toggle
 * - TSV download button
 * - Navigation to execution tree comparison
 */
function setComparisonPageEvents() {
    // Engine visibility checkboxes - show/hide engine columns
    document.querySelector("#columnCheckboxContainer").addEventListener("change", () => {
        const enginesToDisplay = Array.from(
            document.querySelectorAll('#columnCheckboxContainer input[type="checkbox"]:checked'),
        ).map((cb) => cb.value);
        updateHiddenColumns(enginesToDisplay);
    });

    // Column ordering dropdown - sort engines by selected metric
    document.querySelector("#orderColumnsDropdown").addEventListener("change", (event) => {
        const selectedValue = event.target.value;
        const [metric, order] = selectedValue.split("-");
        const kb = document.querySelector("#page-comparison").dataset.kb;
        const enginesToDisplay = Array.from(
            document.querySelectorAll('#columnCheckboxContainer input[type="checkbox"]:checked'),
        ).map((cb) => cb.value);
        const sortedEngines = sortEngines(enginesToDisplay, kb, metric, order);
        const showResultSize = document.querySelector("#showResultSize").checked;
        const sortedColumnDefs = getComparisonColumnDefs(sortedEngines, showResultSize);
        const showMetrics = document.querySelector("#showMetrics").checked;
        sortedColumnDefs[0].headerName = showMetrics ? "Metric/Query" : "Query";
        const colState = gridApi.getColumnState();
        gridApi.updateGridOptions({
            columnDefs: sortedColumnDefs,
            maintainColumnOrder: false,
        });
        gridApi.applyColumnState({
            state: colState,
        });
    });

    // Show aggregate metrics toggle - adds pinned rows on top of the table
    document.querySelector("#showMetrics").addEventListener("change", (event) => {
        if (!gridApi) return;
        const showMetrics = event.target.checked;
        const enginesToDisplay = gridApi
            .getColumns()
            .filter((col) => {
                return col.colId !== "query";
            })
            .map((col) => {
                return col.colId;
            });
        const columnDefs = gridApi.getColumnDefs();
        let pinnedMetricData = [];
        let queryHeader = "Query";
        if (showMetrics) {
            const kb = document.querySelector("#page-comparison").dataset.kb;
            pinnedMetricData = getPinnedMetricData(enginesToDisplay, kb);
            queryHeader = "Metric/Query";
        }
        columnDefs[0].headerName = queryHeader;
        const colState = gridApi.getColumnState();
        gridApi.updateGridOptions({
            pinnedTopRowData: pinnedMetricData,
            columnDefs: columnDefs,
        });
        gridApi.applyColumnState({
            state: colState,
        });
    });

    // Show result size toggle - displays result count below runtime in each cell
    document.querySelector("#showResultSize").addEventListener("change", (event) => {
        if (!gridApi) return;
        const showResultSize = event.target.checked;
        const enginesToDisplay = gridApi
            .getColumns()
            .filter((col) => {
                return col.colId !== "query";
            })
            .map((col) => {
                return col.colId;
            });
        const visibleColumnDefs = getComparisonColumnDefs(enginesToDisplay, showResultSize);
        const showMetrics = document.querySelector("#showMetrics").checked;
        visibleColumnDefs[0].headerName = showMetrics ? "Metric/Query" : "Query";
        const colState = gridApi.getColumnState();
        gridApi.updateGridOptions({
            columnDefs: visibleColumnDefs,
            maintainColumnOrder: true,
        });
        gridApi.applyColumnState({
            state: colState,
        });
    });

    // Navigate to execution tree comparison page
    document.querySelectorAll(".compare-exec-trees-btn").forEach((btn) => {
        btn.addEventListener("click", () => goToCompareExecTreesPage());
    });

    // Download comparison data as TSV file
    document.querySelector("#comparisonDownloadTsv").addEventListener("click", () => {
        const kb = document.querySelector("#page-comparison").dataset.kb;
        if (!gridApi) {
            alert(`The evaluation results table for ${kb} could not be downloaded!`);
            return;
        }
        gridApi.exportDataAsCsv({
            fileName: `${kb}_evaluation_results.tsv`,
            columnSeparator: "\t",
        });
    });
}

/**
 * Constructs a mapping from query string to per-engine statistics.
 * Used to organize data for the comparison grid.
 *
 * @param {Object} performanceData - The engine performance data for a specific KB
 * @returns {Object} Mapping: query => { engine => stats }
 */
function getQueryToEngineStatsMap(performanceData) {
    const queryToEngineStats = {};

    for (const [engine, data] of Object.entries(performanceData)) {
        const queriesData = data.queries;

        for (const queryData of queriesData) {
            const { query, ...restOfStats } = queryData;

            if (!queryToEngineStats[query]) {
                queryToEngineStats[query] = {};
            }

            queryToEngineStats[query][engine] = restOfStats;
        }
    }

    return queryToEngineStats;
}

/**
 * Finds the best (lowest) runtime among all engines for a single query.
 * Only considers successful queries (non-string results).
 *
 * @param {Object} engineStats - Stats per engine for a query
 * @returns {number|null} Minimum runtime or null if no valid runtimes
 */
function getBestRuntimeForQuery(engineStats) {
    const runtimes = Object.values(engineStats)
        .filter((stat) => typeof stat.results !== "string")
        .map((stat) => Number(stat.runtime_info.client_time.toFixed(2)));

    return runtimes.length > 0 ? Math.min(...runtimes) : null;
}

/**
 * Determines the majority result size across engines for a query.
 * Used to detect result size discrepancies between engines.
 *
 * @param {Object} engineStats - Stats per engine for a query
 * @returns {string|null} The majority size string, "no_consensus" if tied, or null if no results
 */
function getMajorityResultSizeForQuery(engineStats) {
    const sizeCounts = {};

    for (const stat of Object.values(engineStats)) {
        // Skip failed queries
        if (typeof stat.results === "string") continue;

        const singleResult = getSingleResult(stat);
        const resultSize = stat.result_size ?? 0;
        const key = singleResult === null ? resultSize.toLocaleString() : singleResult;

        sizeCounts[key] = (sizeCounts[key] || 0) + 1;
    }

    const entries = Object.entries(sizeCounts);
    if (entries.length === 0) return null;

    let [majorityResultSize, maxCount, tie] = [null, 0, false];

    for (const [size, count] of entries) {
        if (count > maxCount) {
            majorityResultSize = size;
            maxCount = count;
            tie = false;
        } else if (count === maxCount) {
            tie = true;
        }
    }

    return tie ? "no_consensus" : majorityResultSize;
}

/**
 * Creates a summary of performance per query per engine for the comparison grid.
 * Calculates best runtimes, majority result sizes, and warning flags.
 *
 * @param {Object} allEngineStats - Raw engine performance data for a KB
 * @param {string[]|null} enginesToDisplay - Optional filter for which engines to include
 * @returns {Object} Columnar data structure ready for ag-Grid
 */
function getPerformanceComparisonPerKb(allEngineStats, enginesToDisplay = null) {
    enginesToDisplay = enginesToDisplay === null ? Object.keys(allEngineStats) : enginesToDisplay;
    const performanceData = Object.fromEntries(
        Object.entries(allEngineStats).filter(([key]) => enginesToDisplay.includes(key)),
    );
    const engineNames = Object.keys(performanceData);

    // Columns: query name, row warning flag, and for each engine: runtime + stats object
    const columns = ["query", "row_warning", ...engineNames.flatMap((e) => [e, `${e}_stats`])];

    const result = {};
    for (const col of columns) result[col] = [];

    const queryToEngineStats = getQueryToEngineStatsMap(performanceData);

    for (const [query, engineStats] of Object.entries(queryToEngineStats)) {
        result["query"].push(query);

        const bestRuntime = getBestRuntimeForQuery(engineStats);
        const majoritySize = getMajorityResultSizeForQuery(engineStats);

        // Row warning indicates no consensus on result size across engines
        result["row_warning"].push(majoritySize === "no_consensus");

        for (const engine of engineNames) {
            const stat = engineStats[engine];
            if (!stat) {
                result[engine].push(null);
                result[`${engine}_stats`].push(null);
                continue;
            }

            const runtime = Number(stat.runtime_info.client_time.toFixed(2));
            const singleResult = getSingleResult(stat);
            const resultSize = stat.result_size ?? 0;
            const resultSizeFinal = singleResult === null ? resultSize.toLocaleString() : singleResult;

            // Size warning: this engine's result differs from the majority
            const sizeWarning =
                majoritySize !== "no_consensus" &&
                majoritySize !== null &&
                typeof stat.results !== "string" &&
                resultSizeFinal !== majoritySize;

            // Augment stats with computed display properties
            Object.assign(stat, {
                has_best_runtime: runtime === bestRuntime,
                majority_result_size: majoritySize,
                size_warning: sizeWarning,
                result_size_to_display: singleResult === null ? resultSize.toLocaleString() : `1 [${singleResult}]`,
            });

            result[engine].push(runtime);
            result[`${engine}_stats`].push(stat);
        }
    }

    return result;
}

/**
 * Updates the comparison grid to show only the selected engines.
 * Recalculates row data and column definitions for visible engines.
 *
 * @param {string[]} enginesToDisplay - Array of engine names to display
 */
function updateHiddenColumns(enginesToDisplay) {
    if (!gridApi) return;

    const kb = document.querySelector("#page-comparison").dataset.kb;
    const visibleTableData = getPerformanceComparisonPerKb(performanceData[kb], enginesToDisplay);
    const visibleRowData = getGridRowData(visibleTableData.query.length, visibleTableData);
    const showResultSize = document.querySelector("#showResultSize").checked;
    const [metric, order] = document.querySelector("#orderColumnsDropdown").value.split("-");
    const sortedEngines = sortEngines(enginesToDisplay, kb, metric, order);
    const visibleColumnDefs = getComparisonColumnDefs(sortedEngines, showResultSize);
    const showMetrics = document.querySelector("#showMetrics").checked;
    visibleColumnDefs[0].headerName = showMetrics ? "Metric/Query" : "Query";
    const colState = gridApi.getColumnState();
    gridApi.updateGridOptions({
        columnDefs: visibleColumnDefs,
        rowData: visibleRowData,
        maintainColumnOrder: false,
    });
    gridApi.applyColumnState({
        state: colState,
    });
}

/**
 * ag-Grid cell renderer that displays runtime values with warning icons.
 * Handles pinned metric rows, query column warnings, and engine result warnings.
 */
class WarningCellRenderer {
    init(params) {
        const value = params.value;
        const container = document.createElement("div");
        container.style.whiteSpace = "normal";

        const warning = getWarningSpan("bi-exclamation-triangle-fill");

        if (params.node.rowPinned) {
            // Pinned rows show aggregate metrics in bold
            container.classList.add("fw-bold");
            let textValue = "N/A";
            if (typeof value === "string") {
                textValue = value;
            } else if (typeof value === "number") {
                const unit = params.data.query === "Failed Queries" ? "%" : "s";
                textValue = `${value.toFixed(2)} ${unit}`;
            }
            container.appendChild(document.createTextNode(textValue));
        } else if (params.column.getColId() === "query") {
            // Query column: show warning if result sizes don't match across engines
            if (params.data.row_warning) {
                warning.title = "The result sizes for the engines do not match!";
                container.appendChild(warning);
            }
            container.appendChild(document.createTextNode(`${value}`));
        } else {
            // Engine column: show runtime with optional warnings
            const engine = params.column.getColId();
            const kb = document.querySelector("#page-comparison").dataset.kb;
            const timeout = performanceData[kb][engine].timeout;
            const engineStatsColumn = engine + "_stats";
            const engineStats = params.data[engineStatsColumn];
            let cellValue = `${value} s`;

            if (engineStats && typeof engineStats === "object") {
                // Show warning if result size differs from majority
                if (engineStats.size_warning) {
                    container.appendChild(warning);
                }
                // Show "timeout" or "failed" instead of runtime for failed queries
                if (typeof engineStats.results === "string") {
                    cellValue = timeout && value >= timeout ? "timeout" : "failed";
                    // Show reboot icon if server was restarted after this query
                    if (engineStats.serverRestarted) {
                        container.appendChild(getWarningSpan("bi-bootstrap-reboot"));
                    }
                }
            }
            container.appendChild(document.createTextNode(cellValue));

            // Optionally show result size below runtime
            if (params.showResultSize) {
                const resultSizeLine = document.createElement("div");
                resultSizeLine.textContent = engineStats?.result_size_to_display;
                resultSizeLine.style.color = "#888";
                resultSizeLine.style.fontSize = "90%";
                resultSizeLine.style.marginTop = "-8px";
                container.appendChild(resultSizeLine);
            }
        }
        this.eGui = container;

        /**
         * Creates a Bootstrap icon span for warnings.
         * @param {string} cls - Bootstrap icon class name
         * @returns {HTMLElement} Icon element
         */
        function getWarningSpan(cls) {
            const warning = document.createElement("i");
            warning.className = `bi ${cls} me-2`;
            return warning;
        }
    }

    getGui() {
        return this.eGui;
    }
}

/**
 * Returns cell background style based on query result status.
 * Green for best runtime, red for failed/timeout queries.
 *
 * @param {Object} params - ag-Grid cell style params
 * @returns {Object} CSS style object for the cell
 */
function comparisonGridCellStyle(params) {
    const engineStatsColumn = params.column.getColId() + "_stats";
    const engineStats = params.data[engineStatsColumn];

    if (engineStats && typeof engineStats === "object") {
        if (typeof engineStats.results === "string") {
            // Failed query - red background
            return { backgroundColor: "var(--bs-danger-border-subtle)" };
        } else if (engineStats.has_best_runtime) {
            // Best runtime - green background
            return { backgroundColor: "var(--bs-success-border-subtle)" };
        }
    }
    return {};
}

/**
 * Generates tooltip content for comparison grid cells.
 * Shows SPARQL query for query column, or error/warning details for engine columns.
 *
 * @param {Object} params - ag-Grid tooltip value getter params
 * @returns {Object|string|null} Tooltip content object or string
 */
function getTooltipValue(params) {
    if (params.column.getColId() === "query") {
        // Query column: show SPARQL query in tooltip
        for (const key in params.data) {
            const value = params.data[key];
            if (value && typeof value === "object" && typeof value.sparql === "string") {
                return { title: value.description || "", sparql: value.sparql || "" };
            }
        }
        return null;
    }

    // Engine column: show error details or warnings
    const engine = params.column.getColId();
    const kb = document.querySelector("#page-comparison").dataset.kb;
    const timeout = performanceData[kb][engine].timeout;
    const engineStatsColumn = engine + "_stats";
    const engineStats = params.data[engineStatsColumn];

    if (engineStats && typeof engineStats === "object") {
        let tooltipLines = [];

        if (typeof engineStats.results === "string") {
            // Failed query - show timeout/error details
            const runtime = params.value;
            const serverRestarted = engineStats.serverRestarted;
            const isTimeout = timeout && runtime >= timeout;

            if (isTimeout) {
                tooltipLines.push(`Query timed out after ${runtime} s`);

                if (serverRestarted) {
                    if (runtime >= timeout + 30) {
                        tooltipLines.push(
                            "Server was restarted after this query due to no response after timeout + 30s!",
                        );
                    } else {
                        tooltipLines.push("Server was restarted after this query because the server crashed!");
                    }
                }
            } else {
                tooltipLines.push(`Query failed in ${runtime} s`);

                if (serverRestarted) {
                    tooltipLines.push("Server was restarted after this query because the server crashed!");
                }
            }

            tooltipLines.push(engineStats.results);
        } else {
            // Successful query - show size warning if applicable
            if (engineStats.size_warning) {
                let resultSize = engineStats.result_size_to_display;
                if (resultSize.startsWith("1 [")) {
                    resultSize = resultSize.slice(3, -1);
                }
                tooltipLines.push(
                    `Result size ${resultSize} doesn't match the majority ${engineStats.majority_result_size}!`,
                );
            }
            tooltipLines.push(`Result size: ${engineStats.result_size_to_display}`);
        }
        return tooltipLines.join("\n\n");
    }
    return null;
}

/**
 * ag-Grid custom tooltip component with copy-to-clipboard functionality.
 * Allows users to copy SPARQL queries or error messages.
 */
class CustomTooltip {
    init(params) {
        const container = createTooltipContainer(params);

        const tooltipText = typeof params.value !== "string" ? params.value.sparql : params.value;

        // Only show copy button in secure context (HTTPS or localhost)
        if (window.isSecureContext) {
            const copyButton = document.createElement("button");
            copyButton.className = "copy-btn btn-sm";
            copyButton.title = "Copy";

            const copyIcon = document.createElement("i");
            copyIcon.className = "bi bi-copy";
            copyButton.appendChild(copyIcon);

            copyButton.onclick = () => {
                navigator.clipboard
                    .writeText(tooltipText)
                    .then(() => {
                        copyIcon.className = "bi bi-check-circle-fill";
                        setTimeout(() => (copyIcon.className = "bi bi-copy"), 1000);
                    })
                    .catch((err) => {
                        console.error("Failed to copy:", err);
                        copyIcon.className = "bi bi-x-circle-fill";
                        setTimeout(() => (copyIcon.className = "bi bi-copy"), 1000);
                    });
            };

            container.appendChild(copyButton);
        }

        this.eGui = container;
    }

    getGui() {
        return this.eGui;
    }
}

/**
 * Generates pinned row data for displaying aggregate metrics at the top of the grid.
 *
 * @param {string[]} engines - Array of engine names
 * @param {string} kb - Knowledge base key
 * @returns {Array<Object>} Array of metric row objects for pinned rows
 */
function getPinnedMetricData(engines, kb) {
    let pinnedMetricData = [];

    // Metrics to display in pinned rows
    const metricKeyNameObj = {
        gmeanTime2: "Geometric Mean (P=2)",
        gmeanTime10: "Geometric Mean (P=10)",
        failed: "Failed Queries",
        medianTime: "Median (P=2)",
        ameanTime: "Arithmetic Mean (P=2)",
        indexTime: "Index Time",
        indexSize: "Index Size",
    };

    for (const [metric, metricName] of Object.entries(metricKeyNameObj)) {
        let metricData = { query: metricName };
        for (const engine of engines) {
            metricData[engine] = performanceData[kb][engine][metric];
        }
        pinnedMetricData.push(metricData);
    }
    return pinnedMetricData;
}

/**
 * Returns column definitions for the comparison ag-Grid table.
 * Creates a query column plus one column per engine with custom rendering.
 *
 * @param {string[]} engines - Array of engine names in display order
 * @param {boolean} showResultSize - Whether to show result size below runtime
 * @returns {Array<Object>} Array of ag-Grid column definition objects
 */
function getComparisonColumnDefs(engines, showResultSize) {
    const columnDefs = [
        {
            headerName: "Query",
            field: "query",
            filter: "agTextColumnFilter",
            flex: 4,
            cellRenderer: WarningCellRenderer,
            autoHeight: showResultSize,
            tooltipValueGetter: getTooltipValue,
            tooltipComponent: CustomTooltip,
            minWidth: 120,
        },
    ];

    const kb = document.querySelector("#page-comparison").dataset.kb;

    for (const engine of engines) {
        const timeout = performanceData[kb][engine].timeout;
        columnDefs.push({
            field: engine,
            type: "numericColumn",
            filter: "agNumberColumnFilter",
            // Custom filter value getter to handle "failed" and "timeout" text filters
            filterValueGetter: (params) => {
                let value = params.data[engine];
                const engineStats = params.data[`${engine}_stats`];
                if (typeof engineStats.results === "string") {
                    // Use large values for failed/timeout to sort them last
                    value = timeout && value >= timeout ? timeout * 1000 : timeout * 100;
                }
                return value;
            },
            filterParams: {
                allowedCharPattern: "0-9\\.adefilmotuADEFILMOTU\\s",
                numberParser: (text) => {
                    if (text == null) return null;
                    const lower = text.toLowerCase().trim();
                    if ("failed".includes(lower)) return timeout * 100;
                    if ("timeout".includes(lower)) return timeout * 1000;
                    return parseFloat(text);
                },
                numberFormatter: (value) => {
                    if (value == null) return null;
                    if (value === timeout * 100) return "failed";
                    if (value === timeout * 1000) return "timeout";
                    return value.toString();
                },
            },
            flex: 1,
            cellRenderer: WarningCellRenderer,
            cellRendererParams: { showResultSize: showResultSize },
            cellStyle: comparisonGridCellStyle,
            autoHeight: true,
            tooltipValueGetter: getTooltipValue,
            tooltipComponent: CustomTooltip,
        });
    }
    return columnDefs;
}

/**
 * Updates the comparison page for a specific knowledge base.
 * Creates the per-query comparison grid with all engines.
 * Skips re-rendering if the same KB is already displayed.
 *
 * @param {Object} performanceData - Full performance data object
 * @param {string} kb - Knowledge base key
 * @param {Object} kbAdditionalData - Additional KB metadata (name, description)
 */
function updateComparisonPage(performanceData, kb, kbAdditionalData) {
    const pageNode = document.querySelector("#page-comparison");
    const lastKb = pageNode.dataset.kb;
    removeTitleInfoPill();

    // Set page title with optional info pill for benchmark description
    const titleNode = document.querySelector("#main-page-header");
    let kbHeader = kbAdditionalData?.name || capitalize(kb);
    let title = `Performance Evaluation for ${kbHeader}`;
    let infoPill = null;
    if (kbAdditionalData.description) {
        infoPill = createBenchmarkDescriptionInfoPill(kbAdditionalData.description, "bottom");
    }
    titleNode.innerHTML = title;
    if (infoPill) {
        titleNode.appendChild(infoPill);
        new bootstrap.Popover(infoPill);
    }

    // Skip re-rendering if already showing this KB
    if (lastKb === kb) return;
    pageNode.dataset.kb = kb;

    // Reset controls to default state
    document.querySelector("#orderColumnsDropdown").selectedIndex = 0;

    // Populate engine visibility checkboxes
    const showEnginesContainer = document.querySelector("#columnCheckboxContainer");
    showEnginesContainer.innerHTML = "";
    const showEnginesColumns = Object.fromEntries(
        Object.keys(performanceData[kb]).map((engine) => [engine, capitalize(engine)]),
    );
    showEnginesContainer.appendChild(getColumnVisibilityMultiSelectFragment(showEnginesColumns));

    // Reset toggles
    document.querySelector("#showResultSize").checked = false;
    document.querySelector("#showMetrics").checked = false;

    // Disable execution tree comparison buttons if not enough engines available
    const execTreeEngines = getEnginesWithExecTrees(performanceData[kb]);
    document.querySelectorAll(".compare-exec-trees-btn").forEach((btn) => {
        setCompareExecTreesBtnState(btn, execTreeEngines.length >= 2);
    });

    // Prepare grid data
    const tableData = getPerformanceComparisonPerKb(performanceData[kb]);
    const gridDiv = document.querySelector("#comparison-grid");

    const rowCount = tableData.query.length;
    const rowData = getGridRowData(rowCount, tableData);
    gridDiv.innerHTML = "";

    gridDiv.style.height = `${document.documentElement.clientHeight - 185}px`;

    // Default column ordering: sort by geometric mean (P=2) ascending
    const sortedEngines = sortEngines(Object.keys(performanceData[kb]), kb, "gmeanTime2", "asc");

    const comparisonGridOptions = {
        columnDefs: getComparisonColumnDefs(sortedEngines),
        rowData: rowData,
        defaultColDef: {
            sortable: true,
            filter: true,
            resizable: true,
            flex: 1,
            minWidth: 90,
        },
        domLayout: "normal",
        rowStyle: { fontSize: "clamp(12px, 1vw + 8px, 14px)", cursor: "pointer" },
        onGridReady: (params) => {
            gridApi = params.api;
        },
        tooltipShowDelay: 0,
        tooltipTrigger: "focus",
        tooltipInteraction: true,
        suppressDragLeaveHidesColumns: true,
    };

    // Initialize ag-Grid instance
    agGrid.createGrid(gridDiv, comparisonGridOptions);
}
