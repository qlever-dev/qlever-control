let detailsGridApi = null;

function setDetailsPageEvents() {
    // Adds functionality to buttons in the modal footer for zooming in/out the execution tree
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

    document.querySelector("#detailsCompareExecTreesBtn").addEventListener("click", () => {
        goToCompareExecTreesPage(detailsGridApi, "Query Runtimes");
    });
}

/**
 * Extracts runtime and related query info for a given knowledge base and engine.
 *
 * @param {Object<string, Object<string, any>>} performanceData - Performance data object
 * @param {string} kb - Knowledge base name
 * @param {string} engine - Engine name
 * @returns {Object<string, Array>} Object containing arrays for query, runtime, failed, and result_size
 */
function getQueryRuntimes(performanceData, kb, engine) {
    const allQueriesData = performanceData[kb][engine].queries;
    const queryRuntimes = {
        query: [],
        description: [],
        sparql: [],
        runtime: [],
        failed: [],
        result_size: [],
    };

    for (const queryData of allQueriesData) {
        queryRuntimes.query.push(queryData.query);
        queryRuntimes.description.push(queryData.description || "");
        queryRuntimes.sparql.push(queryData.sparql);
        const runtime = Number(queryData.runtime_info.client_time.toFixed(2));
        queryRuntimes.runtime.push(runtime);

        const failed = typeof queryData.results === "string" || (queryData.headers?.length ?? 0) === 0;
        queryRuntimes.failed.push(failed);

        const resultSize = queryData.result_size ?? 0;
        const singleResult = getSingleResult(queryData);

        const resultSizeToDisplay = singleResult === null ? resultSize.toLocaleString() : `1 [${singleResult}]`;

        queryRuntimes.result_size.push(resultSizeToDisplay);
    }
    return queryRuntimes;
}

/**
 * Converts query results represented as a list of lists into a dictionary
 * mapping headers to their respective columns (lists).
 *
 * @param {string[]} headers - List of header strings
 * @param {string[][]} queryResults - List of query results (each result is a list of strings)
 * @returns {Object<string, string[]>} Object mapping header names to lists of column values
 */
function getQueryResultsDict(headers, queryResults) {
    const queryResultsLists = headers.map(() => []);

    for (const result of queryResults) {
        for (let i = 0; i < headers.length; i++) {
            queryResultsLists[i].push(result[i]);
        }
    }

    const queryResultsDict = {};
    headers.forEach((header, i) => {
        queryResultsDict[header] = queryResultsLists[i];
    });

    return queryResultsDict;
}

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
 * Returns column definitions for ag-Grid to display query runtime results.
 * Expected input data keys: query, runtime, failed, result_size.
 *
 * @returns {Array<Object>} columnDefs for ag-Grid
 */
function getQueryRuntimesColumnDefs() {
    return [
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
            valueFormatter: (params) => (params.value != null ? `${params.value.toFixed(2)}s` : ""),
        },
        {
            headerName: "Result Size",
            field: "result_size",
            type: "numericColumn",
            filter: "agTextColumnFilter",
            flex: 1.5,
        },
    ];
}

function setTabsToDefault() {
    document.querySelectorAll("#page-details .tab-pane").forEach((node) => {
        if (node.id === "runtimes-tab-pane") return;
        for (const div of node.querySelectorAll("div")) {
            if (div.classList.contains("alert-info")) div.classList.remove("d-none");
            else div.classList.add("d-none");
        }
    });
}

let exec_tree_listener = null;

function updateTabsWithSelectedRow(rowData) {
    const sparqlQuery = rowData?.sparql;
    if (sparqlQuery) {
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

    const runtime_info = rowData?.runtime_info;
    if (runtime_info?.query_execution_tree) {
        for (const div of document.querySelectorAll("#exec-tree-tab-pane div")) {
            if (div.classList.contains("alert-info")) div.classList.add("d-none");
            else div.classList.remove("d-none");
        }
        document.querySelector("#result-tree").innerHTML = "";
        const exec_tree_tab = document.querySelector("#exec-tree-tab");
        if (exec_tree_listener) exec_tree_tab.removeEventListener("shown.bs.tab", exec_tree_listener);
        exec_tree_listener = () => {
            renderExecTree(runtime_info, "#result-tree", "#meta-info");
            exec_tree_listener = null;
        };
        exec_tree_tab.addEventListener("shown.bs.tab", exec_tree_listener, { once: true });
    } else {
        document.querySelector("#exec-tree-tab-pane div.alert-info").classList.add("d-none");
        document.querySelector("#result-tree-div").classList.remove("d-none");
        document.querySelector("#result-tree-div div.alert-info").classList.remove("d-none");
    }

    const headers = rowData?.headers;
    const queryResults = rowData?.results;
    for (const div of document.querySelectorAll("#results-tab-pane div")) {
        if (div.classList.contains("alert-info")) div.classList.add("d-none");
        else div.classList.remove("d-none");
    }
    const gridDiv = document.querySelector("#results-grid");
    gridDiv.innerHTML = "";
    if (Array.isArray(queryResults) && Array.isArray(headers)) {
        const textDiv = document.querySelector("#results-container div.alert");
        textDiv.classList.remove("alert-danger");
        textDiv.classList.add("alert-secondary");
        textDiv.innerHTML = `Showing ${rowData.results.length} results out of ${
            rowData?.result_size ?? 0
        } total results`;
        const rowCount = queryResults.length;
        const tableData = getQueryResultsDict(headers, queryResults);
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
            rowStyle: { fontSize: "14px", cursor: "pointer" },
            suppressDragLeaveHidesColumns: true,
        });
    } else {
        const textDiv = document.querySelector("#results-container div.alert");
        textDiv.classList.add("alert-danger");
        textDiv.classList.remove("alert-secondary");
        textDiv.innerHTML = `<strong>Query failed in ${rowData.runtime_info.client_time.toFixed(
            2
        )} s with error:</strong> <br><br>${rowData.results}`;
    }
}

/**
 * Called when a row is selected in the runtime table
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

function updateDetailsPage(performanceData, kb, engine) {
    const pageNode = document.querySelector("#page-details");
    removeTitleInfoPill();
    let engine_header = capitalize(engine);
    if (engine_header === "Qlever") engine_header = "QLever";
    const titleNode = document.querySelector("#main-page-header");
    titleNode.innerHTML = `Details - ${engine_header} (${capitalize(kb)})`;
    if (pageNode.dataset.kb === kb && pageNode.dataset.engine === engine) return;
    pageNode.dataset.kb = kb;
    pageNode.dataset.engine = engine;

    setTabsToDefault();
    const tab = new bootstrap.Tab(document.querySelector("#runtimes-tab"));
    tab.show();

    const execTreeEngines = getEnginesWithExecTrees(performanceData[kb]);
    if (execTreeEngines.length < 2 || !execTreeEngines.includes(engine)) {
        document.querySelector("#detailsCompareExecTreesBtn").classList.add("d-none");
    } else {
        document.querySelector("#detailsCompareExecTreesBtn").classList.remove("d-none");
    }

    const tableData = getQueryRuntimes(performanceData, kb, engine);
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
        getRowStyle: (params) => {
            let rowStyle = { fontSize: "14px", cursor: "pointer" };
            if (params.data.failed === true) {
                rowStyle.backgroundColor = "var(--bs-danger-border-subtle)";
            }
            return rowStyle;
        },
        rowSelection: { mode: "singleRow", headerCheckbox: false, enableClickSelection: true },
        onRowSelected: (event) => {
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
