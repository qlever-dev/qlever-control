/**
 * Execution tree comparison page functionality.
 * Allows side-by-side comparison of query execution trees from different engines
 * with synchronized scrolling and zoom controls.
 */

// ============================================================================
// Zoom Configuration Constants
// ============================================================================

/** @type {number} Base font size percentage for tree nodes */
const baseTreeTextFontSize = 80;

/** @type {number} Minimum zoom level percentage */
const minimumZoomPercent = 30;

/** @type {number} Maximum zoom level percentage */
const maximumZoomPercent = 80;

/** @type {number} Zoom step size percentage for each zoom in/out action */
const zoomChange = 10;

/** @type {Object|null} Current query's engine statistics for tree rendering */
let engineStatForQuery = null;

// ============================================================================
// Event Handlers
// ============================================================================

/**
 * Sets up event listeners for the execution tree comparison page, including:
 * - Drag-to-scroll for tree containers
 * - Synchronized scrolling between trees
 * - Zoom in/out controls for both trees
 * - Compare button to render selected engine trees
 */
function setCompareExecTreesEvents() {
    // Set up drag-to-scroll and synchronized scrolling for each tree container
    for (const treeDiv of ["#result-tree", "#tree1", "#tree2"]) {
        let isDragging = false;
        let initialX = 0;
        let initialY = 0;
        let currentTreeDiv = null;

        const treeDivNode = document.querySelector(treeDiv);

        // Mouse down: start dragging
        treeDivNode.addEventListener("mousedown", (e) => {
            currentTreeDiv = treeDiv;
            document.querySelector(currentTreeDiv).style.cursor = "grabbing";
            isDragging = true;
            initialX = e.clientX;
            initialY = e.clientY;
            e.preventDefault();
        });

        // Mouse move: scroll the tree container while dragging
        treeDivNode.addEventListener("mousemove", (e) => {
            if (isDragging) {
                const deltaX = e.clientX - initialX;
                const deltaY = e.clientY - initialY;
                document.querySelector(currentTreeDiv).scrollLeft -= deltaX;
                document.querySelector(currentTreeDiv).scrollTop -= deltaY;

                // Sync scroll position to other tree if enabled
                if (document.getElementById("syncScrollCheck").checked && treeDiv !== "#result-tree") {
                    syncScroll(currentTreeDiv);
                }
                initialX = e.clientX;
                initialY = e.clientY;
            }
        });

        // Scroll event: sync scrolling when using scrollbars or mouse wheel
        treeDivNode.addEventListener("scroll", () => {
            if (document.getElementById("syncScrollCheck").checked && treeDiv !== "#result-tree") {
                syncScroll(treeDiv);
            }
        });

        // Mouse up: stop dragging (attached to document to catch release outside element)
        document.addEventListener("mouseup", () => {
            isDragging = false;
            if (document.querySelector(currentTreeDiv)) {
                document.querySelector(currentTreeDiv).style.cursor = "grab";
            }
            currentTreeDiv = null;
        });
    }

    // Set up zoom controls for both tree panels
    document.querySelectorAll('[aria-label="CompareExecTrees zoom"]').forEach((node) => {
        node.addEventListener("click", function (event) {
            if (["BUTTON", "I"].includes(event.target.tagName)) {
                const engine1 = document.querySelector("#select1").value;
                const engine2 = document.querySelector("#select2").value;
                if (!engine1 || !engine2) return;

                // Extract zoom direction and target tree from button ID (e.g., "zoomIn1", "zoomOut2")
                const buttonId = event.target.closest("button").id;
                const purpose = buttonId.slice(0, -1); // "zoomIn" or "zoomOut"
                const treeId = `#tree${buttonId.slice(-1)}`; // "#tree1" or "#tree2"

                const currentFontSize = document
                    .querySelector(treeId)
                    .querySelector(".node[class*=font-size-]")
                    .className.match(/font-size-(\d+)/)[1];

                // Get runtime info for both engines
                const kb = new URLSearchParams(window.location.hash.split("?")[1]).get("kb");
                const queryIdx = new URLSearchParams(window.location.hash.split("?")[1]).get("q");
                const runtimeInfo1 = performanceData[kb][engine1].queries[queryIdx].runtime_info;
                const runtimeInfo2 = performanceData[kb][engine2].queries[queryIdx].runtime_info;

                // If sync is enabled, zoom both trees together
                if (document.querySelector("#syncScrollCheck").checked) {
                    for (let [runtimeInfo, id] of [
                        [runtimeInfo1, "1"],
                        [runtimeInfo2, "2"],
                    ]) {
                        renderExecTree(
                            runtimeInfo,
                            `#tree${id}`,
                            `#meta-info-${id}`,
                            purpose,
                            Number.parseInt(currentFontSize),
                        );
                    }
                } else {
                    // Zoom only the target tree
                    let runtimeInfo = treeId === "#tree1" ? runtimeInfo1 : runtimeInfo2;
                    renderExecTree(
                        runtimeInfo,
                        `#tree${buttonId.slice(-1)}`,
                        `#meta-info-${buttonId.slice(-1)}`,
                        purpose,
                        Number.parseInt(currentFontSize),
                    );
                }
            }
        });
    });

    // Query dropdown: navigate to the selected query's execution tree comparison
    document.querySelector("#compareExecQuerySelect").addEventListener("change", (e) => {
        const kb = new URLSearchParams(window.location.hash.split("?")[1]).get("kb");
        router.navigate(`/compareExecTrees?kb=${encodeURIComponent(kb)}&q=${e.target.value}`);
    });

    // Re-render trees when either engine selection changes
    document.querySelector("#select1").addEventListener("change", renderCompareExecTrees);
    document.querySelector("#select2").addEventListener("change", renderCompareExecTrees);
}

// ============================================================================
// Scroll Synchronization
// ============================================================================

/**
 * Synchronizes the scroll position between the two comparison tree containers.
 * Called when scrolling one tree to keep both trees aligned.
 *
 * @param {string} sourceTree - CSS selector of the tree where scrolling occurred ("#tree1" or "#tree2")
 */
function syncScroll(sourceTree) {
    const sourceDiv = document.querySelector(sourceTree);

    for (const targetTree of ["#tree1", "#tree2"]) {
        if (targetTree !== sourceTree) {
            const targetDiv = document.querySelector(targetTree);

            // Match scroll position
            targetDiv.scrollLeft = sourceDiv.scrollLeft;
            targetDiv.scrollTop = sourceDiv.scrollTop;
        }
    }
}

// ============================================================================
// Zoom Utilities
// ============================================================================

/**
 * Calculates the appropriate font size for tree nodes based on the action being performed.
 * For initial display, adjusts size based on tree depth. For zoom actions, increments/decrements.
 *
 * @param {Object} tree - The Treant tree configuration object
 * @param {string} purpose - The action: "showTree" for initial display, "zoomIn", or "zoomOut"
 * @param {number} currentFontSize - The current font size percentage (used for zoom actions)
 * @returns {number} The new font size percentage
 */
function getNewFontSizeForTree(tree, purpose, currentFontSize) {
    let treeDepth;
    let newFontSize = currentFontSize ? currentFontSize : maximumZoomPercent;

    if (purpose === "showTree") {
        // Initial display: calculate appropriate size based on tree depth
        treeDepth = calculateTreeDepth(tree.nodeStructure);
        newFontSize = getFontSizeForDepth(baseTreeTextFontSize, treeDepth);
    } else if (purpose === "zoomIn" && currentFontSize < maximumZoomPercent) {
        // Zoom in: increase font size (up to maximum)
        newFontSize += zoomChange;
    } else if (purpose === "zoomOut" && currentFontSize > minimumZoomPercent) {
        // Zoom out: decrease font size (down to minimum)
        newFontSize -= zoomChange;
    }

    return newFontSize;
}

// ============================================================================
// UI Helpers
// ============================================================================

/**
 * Populates a select dropdown with engine options.
 *
 * @param {HTMLSelectElement} selectEl - The select element to populate
 * @param {string[]} engines - Array of engine names
 * @param {number} selectIndex - Index of the option to pre-select
 */
function populateSelect(selectEl, engines, selectIndex) {
    // Clear existing options
    selectEl.innerHTML = "";

    // Add engine options
    engines.forEach((engine, index) => {
        const optionEl = document.createElement("option");
        optionEl.value = engine;
        optionEl.textContent = capitalize(engine);
        if (index === selectIndex) {
            optionEl.selected = true;
        }
        selectEl.appendChild(optionEl);
    });
}

/**
 * Filters engines to find those that have execution tree data available.
 * Only engines with at least one successful query containing a query_execution_tree are included.
 *
 * @param {Object} performanceDataForKb - Performance data for a specific knowledge base
 * @returns {string[]} Array of engine names that have execution tree data
 */
function getEnginesWithExecTrees(performanceDataForKb) {
    let execTreeEngines = [];

    for (let [engine, engineStat] of Object.entries(performanceDataForKb)) {
        const queries = engineStat.queries;
        for (const query of queries) {
            // Check if query succeeded (results is an array, not error string)
            if (Array.isArray(query.results)) {
                // Check if execution tree data is present
                if (!Object.hasOwn(query.runtime_info, "query_execution_tree")) {
                    break; // No exec tree for this engine
                } else {
                    execTreeEngines.push(engine);
                    break; // Found one, no need to check more queries
                }
            }
        }
    }
    return execTreeEngines;
}

// ============================================================================
// Tree Rendering
// ============================================================================

/**
 * Renders the execution trees for both currently selected engines.
 * Shows an error alert if the query failed for a given engine.
 */
function renderCompareExecTrees() {
    const select1Value = document.querySelector("#select1").value;
    const select2Value = document.querySelector("#select2").value;
    if (!select1Value || !select2Value || !engineStatForQuery) return;

    for (const [engine, treeIdx] of [
        [select1Value, "1"],
        [select2Value, "2"],
    ]) {
        const stats = engineStatForQuery[engine];
        const treeDiv = document.querySelector(`#tree${treeIdx}`);
        const metaDiv = document.querySelector(`#meta-info-${treeIdx}`);
        if (!stats || !stats.runtime_info || !stats.runtime_info.query_execution_tree) {
            metaDiv.innerHTML = "";
            const errorMsg = stats?.results || "Query failed";
            treeDiv.innerHTML = `<div class="alert alert-danger m-2 m-md-5">${errorMsg}</div>`;
        } else {
            renderExecTree(stats.runtime_info, `#tree${treeIdx}`, `#meta-info-${treeIdx}`);
        }
    }
}

// ============================================================================
// Page Update
// ============================================================================

/**
 * Updates the execution tree comparison page for a specific query.
 * Sets up the page title, query dropdown, and engine selection dropdowns.
 * Skips re-rendering if the same query is already displayed.
 *
 * @param {string} kb - Knowledge base key
 * @param {string} query - Query identifier/name
 * @param {Object} execTreeQueryStats - Engine statistics for all queries (filtered to exec tree engines),
 *                                      keyed by query name then engine name
 */
function updateCompareExecTreesPage(kb, query, execTreeQueryStats) {
    const titleNode = document.querySelector("#main-page-header");
    const querySelect = document.querySelector("#compareExecQuerySelect");
    const title = `Query Execution Tree comparison - ${capitalize(kb)}`;

    const allQueryNames = Object.keys(execTreeQueryStats);
    const queryIdx = allQueryNames.indexOf(query);

    removeTitleInfoPill();
    titleNode.innerHTML = title;

    // Populate query dropdown with all queries
    querySelect.innerHTML = "";
    allQueryNames.forEach((qName, idx) => {
        const option = document.createElement("option");
        option.value = idx;
        option.textContent = qName;
        if (idx === queryIdx) {
            option.selected = true;
        }
        querySelect.appendChild(option);
    });

    // Find the SPARQL query text from any engine's stats and show on hover
    const currentQueryStats = execTreeQueryStats[query];
    let sparql = null;
    for (const engineStat of Object.values(currentQueryStats)) {
        if (engineStat.sparql) {
            sparql = engineStat.sparql;
            break;
        }
    }
    querySelect.title = sparql || "";

    // Clear previous tree renderings
    for (let i = 1; i <= 2; i++) {
        document.querySelector(`#meta-info-${i}`).innerHTML = "";
        document.querySelector(`#tree${i}`).innerHTML = "";
    }

    // Store query stats for use by compare button
    engineStatForQuery = currentQueryStats;
    const engines = Object.keys(currentQueryStats);

    // Populate engine dropdowns with first two engines pre-selected
    populateSelect(document.querySelector("#select1"), engines, 0);
    populateSelect(document.querySelector("#select2"), engines, 1);
}
