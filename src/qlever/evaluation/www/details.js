/**
 * Details page functionality for viewing individual query results per engine.
 * Displays query runtimes, full SPARQL queries, execution trees, and query results
 * in a tabbed interface.
 */

/** @type {Object|null} ag-Grid API instance for the query runtimes table */
let detailsGridApi = null;

/**
 * Sets up event listeners for the details page, including:
 * - Execution tree zoom controls
 * - Navigation to execution tree comparison page
 */
function setDetailsPageEvents() {
    // Zoom controls for the execution tree visualization
    document.querySelector('[aria-label="Details zoom controls"]').addEventListener("click", function (event) {
        if (event.target.tagName === "BUTTON") {
            const purpose = event.target.id;
            const treeId = "#result-tree";
            const tree = document.querySelector(treeId);
            const currentFontSize = tree
                .querySelector(".node[class*=font-size-]")
                .className.match(/font-size-(\d+)/)[1];
            const kb = new URLSearchParams(window.location.hash.split("?")[1]).get("kb");
            const engine = new URLSearchParams(window.location.hash.split("?")[1]).get("engine");
            const selectedNodes = detailsGridApi.getSelectedNodes();
            if (selectedNodes.length === 1) {
                const queryIdx = detailsGridApi.getSelectedNodes()[0].rowIndex;
                const runtime_info = performanceData[kb][engine].queries[queryIdx].runtime_info;
                renderExecTree(runtime_info, "#result-tree", "#meta-info", purpose, Number.parseInt(currentFontSize));
            }
        }
    });

    // Button to navigate to execution tree comparison page
    document.querySelector("#detailsCompareExecTreesBtn").addEventListener("click", () => {
        goToCompareExecTreesPage(detailsGridApi);
    });
}

/**
 * Extracts per-query data (name, description, SPARQL, runtime, failure status,
 * result size) for a given knowledge base and engine. Returns the data in
 * columnar format for ag-Grid display.
 *
 * @param {Object} performanceData - Full performance data object
 * @param {string} kb - Knowledge base key
 * @param {string} engine - Engine name
 * @returns {Object<string, Array>} Object with arrays for query, description, sparql, runtime, failed, and result_size
 */
function getQueryDetails(performanceData, kb, engine) {
    const allQueriesData = performanceData[kb][engine].queries;
    const queryDetails = {
        query: [],
        description: [],
        sparql: [],
        runtime: [],
        failed: [],
        result_size: [],
    };

    for (const queryData of allQueriesData) {
        queryDetails.query.push(queryData.query);
        queryDetails.description.push(queryData.description || "");
        queryDetails.sparql.push(queryData.sparql);
        const runtime = Number(queryData.runtime_info.client_time.toFixed(2));
        queryDetails.runtime.push(runtime);

        // A query is considered failed if results is a string (error message) or no headers
        const failed = typeof queryData.results === "string" || (queryData.headers?.length ?? 0) === 0;
        queryDetails.failed.push(failed);

        const resultSize = queryData.result_size ?? 0;
        const singleResult = getSingleResult(queryData);

        // For single-value results (e.g., COUNT), show the value in brackets
        const resultSizeToDisplay = singleResult === null ? resultSize.toLocaleString() : `1 [${singleResult}]`;

        queryDetails.result_size.push(resultSizeToDisplay);
    }
    return queryDetails;
}

/**
 * Converts query results from row-based format (list of lists) into a column-based
 * object mapping headers to their respective value arrays.
 *
 * @param {string[]} headers - List of header/column names
 * @param {string[][]} queryResults - List of result rows (each row is a list of values)
 * @returns {Object<string, string[]>} Object mapping header names to arrays of column values
 */
function getQueryResultsByColumn(headers, queryResults) {
    const queryResultsLists = headers.map(() => []);

    for (const result of queryResults) {
        for (let i = 0; i < headers.length; i++) {
            queryResultsLists[i].push(result[i]);
        }
    }

    const queryResultsByColumn = {};
    headers.forEach((header, i) => {
        queryResultsByColumn[header] = queryResultsLists[i];
    });

    return queryResultsByColumn;
}

/**
 * ag-Grid custom tooltip component for displaying SPARQL queries and descriptions.
 */
class CustomDetailsTooltip {
    eGui;

    init(params) {
        const container = createTooltipContainer(params);
        this.eGui = container;
    }

    getGui() {
        return this.eGui;
    }
}

/**
 * Returns column definitions for the query runtimes ag-Grid table.
 * Configures columns for query name, runtime, and optionally result size on wider screens.
 *
 * @returns {Array<Object>} Array of ag-Grid column definition objects
 */
function getQueryRuntimesColumnDefs() {
    let columnDefs = [
        {
            headerName: "SPARQL Query",
            field: "query",
            filter: "agTextColumnFilter",
            flex: 3,
            tooltipValueGetter: (params) => {
                return { title: params.data.description || "", sparql: params.data.sparql || "" };
            },
            tooltipComponent: CustomDetailsTooltip,
        },
        {
            headerName: "Runtime (s)",
            field: "runtime",
            type: "numericColumn",
            filter: "agNumberColumnFilter",
            flex: 1,
            valueFormatter: (params) => (params.value != null ? `${params.value.toFixed(2)} s` : ""),
        },
    ];

    // Only show result size column on medium+ screens to save space on mobile
    if (window.matchMedia("(min-width: 768px)").matches) {
        columnDefs.push(
            ...[
                {
                    headerName: "Result Size",
                    field: "result_size",
                    type: "numericColumn",
                    filter: "agTextColumnFilter",
                    flex: 1.5,
                },
            ],
        );
    }
    return columnDefs;
}

/**
 * Resets all detail tabs to their default state showing "please select a query" messages.
 */
function setTabsToDefault() {
    document.querySelectorAll("#page-details .tab-pane").forEach((node) => {
        if (node.id === "runtimes-tab-pane") return;
        for (const div of node.querySelectorAll("div")) {
            if (div.classList.contains("alert-info")) div.classList.remove("d-none");
            else div.classList.add("d-none");
        }
    });
}

/**
 * Updates the Full Query tab with the SPARQL query text and description.
 *
 * @param {Object} rowData - Query data object
 */
function updateQueryTab(rowData) {
    const sparqlQuery = rowData?.sparql;
    if (!sparqlQuery) return;

    for (const div of document.querySelectorAll("#query-tab-pane div")) {
        if (div.classList.contains("alert-info")) div.classList.add("d-none");
        else div.classList.remove("d-none");
    }
    const queryTitle = rowData?.description;
    if (queryTitle) {
        document.querySelector("#query-title").innerHTML = queryTitle;
        document.querySelector("#query-title").className = "fw-bold pb-3";
    }
    document.querySelector("#full-query").textContent = sparqlQuery;
}

let exec_tree_listener = null;

/**
 * Updates the Execution Tree tab. Renders lazily when the tab is shown
 * to avoid Treant.js layout issues with hidden containers.
 *
 * @param {Object} rowData - Query data object
 */
function updateExecTreeTab(rowData) {
    const runtime_info = rowData?.runtime_info;
    if (runtime_info?.query_execution_tree) {
        for (const div of document.querySelectorAll("#exec-tree-tab-pane div")) {
            if (div.classList.contains("alert-info")) div.classList.add("d-none");
            else div.classList.remove("d-none");
        }
        document.querySelector("#result-tree").innerHTML = "";
        const exec_tree_tab = document.querySelector("#exec-tree-tab");

        // Remove previous listener if exists
        if (exec_tree_listener) exec_tree_tab.removeEventListener("shown.bs.tab", exec_tree_listener);

        // Render tree only when tab is shown (performance optimization)
        exec_tree_listener = () => {
            renderExecTree(runtime_info, "#result-tree", "#meta-info");
            exec_tree_listener = null;
        };
        exec_tree_tab.addEventListener("shown.bs.tab", exec_tree_listener, { once: true });
    } else {
        // No execution tree available - show appropriate message
        document.querySelector("#exec-tree-tab-pane div.alert-info").classList.add("d-none");
        document.querySelector("#result-tree-div").classList.remove("d-none");
        document.querySelector("#result-tree-div div.alert-info").classList.remove("d-none");
    }
}

/**
 * Updates the Query Results tab with a results grid or an error message.
 *
 * @param {Object} rowData - Query data object
 */
function updateResultsTab(rowData) {
    const headers = rowData?.headers;
    const queryResults = rowData?.results;
    for (const div of document.querySelectorAll("#results-tab-pane div")) {
        if (div.classList.contains("alert-info")) div.classList.add("d-none");
        else div.classList.remove("d-none");
    }

    const gridDiv = document.querySelector("#results-grid");
    gridDiv.innerHTML = "";

    if (Array.isArray(queryResults) && Array.isArray(headers)) {
        // Successful query - display results in a grid
        const textDiv = document.querySelector("#results-container div.alert");
        textDiv.classList.remove("alert-danger");
        textDiv.classList.add("alert-secondary");
        textDiv.innerHTML = `Showing ${rowData.results.length} results out of ${
            rowData?.result_size ?? 0
        } total results`;

        const rowCount = queryResults.length;
        const tableData = getQueryResultsByColumn(headers, queryResults);

        // Use autoHeight for small result sets, fixed height for large ones
        let domLayout = "normal";
        if (rowCount < 25) domLayout = "autoHeight";

        if (domLayout === "normal") {
            gridDiv.style.height = `${document.documentElement.clientHeight - 275}px`;
        }

        const gridData = getGridRowData(rowCount, tableData);
        const columnDefs = headers.map((key) => ({
            field: key,
            headerName: key,
        }));

        agGrid.createGrid(gridDiv, {
            columnDefs: columnDefs,
            rowData: gridData,
            defaultColDef: {
                sortable: true,
                filter: true,
                resizable: true,
                flex: 1,
                minWidth: 100,
            },
            domLayout: domLayout,
            rowStyle: { fontSize: "clamp(12px, 1vw + 8px, 14px)", cursor: "pointer" },
            suppressDragLeaveHidesColumns: true,
        });
    } else {
        // Failed query - display error message
        const textDiv = document.querySelector("#results-container div.alert");
        textDiv.classList.add("alert-danger");
        textDiv.classList.remove("alert-secondary");
        textDiv.innerHTML = `<strong>Query failed in ${rowData.runtime_info.client_time.toFixed(
            2,
        )} s with error:</strong> <br><br>${rowData.results}`;
    }
}

/**
 * Updates all tab panes with data from the selected query row.
 *
 * @param {Object} rowData - Query data object containing sparql, runtime_info, headers, results
 */
function updateTabsWithSelectedRow(rowData) {
    updateQueryTab(rowData);
    updateExecTreeTab(rowData);
    updateResultsTab(rowData);
}

/**
 * Handler for query runtime table row selection events.
 * Updates all tabs with the selected query's data.
 *
 * @param {Object} event - ag-Grid row selection event
 * @param {Object} performanceData - Full performance data object
 * @param {string} kb - Knowledge base key
 * @param {string} engine - Engine name
 */
function onRuntimeRowSelected(event, performanceData, kb, engine) {
    const selectedNode = event.api.getSelectedNodes();
    if (selectedNode.length === 1) {
        let selectedRowIdx = selectedNode[0].rowIndex;
        updateTabsWithSelectedRow(performanceData[kb][engine]["queries"][selectedRowIdx]);
    } else {
        setTabsToDefault();
    }
}

/**
 * Updates the details page for a specific engine and knowledge base.
 * Creates the query runtime table and configures row selection handlers.
 * Skips re-rendering if the same kb/engine combination is already displayed.
 *
 * @param {Object} performanceData - Full performance data object
 * @param {string} kb - Knowledge base key
 * @param {string} engine - Engine name
 * @param {Object} kbAdditionalData - Additional KB metadata (name, description)
 */
function updateDetailsPage(performanceData, kb, engine, kbAdditionalData) {
    const pageNode = document.querySelector("#page-details");
    removeTitleInfoPill();

    // Format engine name for display (special case for QLever capitalization)
    let engineHeader = capitalize(engine);
    if (engineHeader === "Qlever") engineHeader = "QLever";
    let kbHeader = kbAdditionalData?.name || capitalize(kb);

    const titleNode = document.querySelector("#main-page-header");
    titleNode.innerHTML = `Per-query results for ${engineHeader} on ${kbHeader}`;

    // Skip re-rendering if already showing this kb/engine combination
    if (pageNode.dataset.kb === kb && pageNode.dataset.engine === engine) return;
    pageNode.dataset.kb = kb;
    pageNode.dataset.engine = engine;

    // Reset tabs and show the runtimes tab
    setTabsToDefault();
    const tab = new bootstrap.Tab(document.querySelector("#runtimes-tab"));
    tab.show();

    // Disable the compare execution trees button if not enough engines available
    const execTreeEngines = getEnginesWithExecTrees(performanceData[kb]);
    setCompareExecTreesBtnState(
        document.querySelector("#detailsCompareExecTreesBtn"),
        execTreeEngines.length >= 2 && execTreeEngines.includes(engine),
    );

    // Prepare and display the query runtimes grid
    const tableData = getQueryDetails(performanceData, kb, engine);
    const gridDiv = document.querySelector("#details-grid");

    const rowCount = tableData.query.length;
    const rowData = getGridRowData(rowCount, tableData);
    gridDiv.innerHTML = "";

    gridDiv.style.height = `${document.documentElement.clientHeight - 150}px`;

    let selectedRow = null;
    const detailsGridOptions = {
        columnDefs: getQueryRuntimesColumnDefs(),
        rowData: rowData,
        defaultColDef: {
            sortable: true,
            filter: true,
            resizable: true,
        },
        domLayout: "normal",
        onGridReady: (params) => {
            detailsGridApi = params.api;
        },
        // Style failed queries with danger background color
        getRowStyle: (params) => {
            let rowStyle = { fontSize: "clamp(12px, 1vw + 8px, 14px)", cursor: "pointer" };
            if (params.data.failed === true) {
                rowStyle.backgroundColor = "var(--bs-danger-border-subtle)";
            }
            return rowStyle;
        },
        rowSelection: { mode: "singleRow", headerCheckbox: false, enableClickSelection: true },
        onRowSelected: (event) => {
            // Prevent re-processing if same row is selected
            const query = Array.isArray(selectedRow) ? selectedRow[0].query : null;
            if (event.api.getSelectedRows()[0].query === query) return;
            selectedRow = event.api.getSelectedRows();
            onRuntimeRowSelected(event, performanceData, kb, engine);
        },
        tooltipShowDelay: 1500,
        suppressDragLeaveHidesColumns: true,
    };

    // Initialize ag-Grid instance
    agGrid.createGrid(gridDiv, detailsGridOptions);
}
