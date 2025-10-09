// Zoom settings
const baseTreeTextFontSize = 80;
const minimumZoomPercent = 30;
const maximumZoomPercent = 80;
const zoomChange = 10;
let engineStatForQuery = null;

function setCompareExecTreesEvents() {
    // Events to handle drag and scroll horizontally on compareExecTrees page
    for (const treeDiv of ["#result-tree", "#tree1", "#tree2"]) {
        let isDragging = false;
        let initialX = 0;
        let initialY = 0;
        let currentTreeDiv = null;

        const treeDivNode = document.querySelector(treeDiv);

        treeDivNode.addEventListener("mousedown", (e) => {
            currentTreeDiv = treeDiv;
            document.querySelector(currentTreeDiv).style.cursor = "grabbing";
            isDragging = true;
            initialX = e.clientX;
            initialY = e.clientY;
            e.preventDefault();
        });
        treeDivNode.addEventListener("mousemove", (e) => {
            if (isDragging) {
                const deltaX = e.clientX - initialX;
                const deltaY = e.clientY - initialY;
                document.querySelector(currentTreeDiv).scrollLeft -= deltaX;
                document.querySelector(currentTreeDiv).scrollTop -= deltaY;
                if (document.getElementById("syncScrollCheck").checked && treeDiv !== "#result-tree") {
                    syncScroll(currentTreeDiv);
                }
                initialX = e.clientX;
                initialY = e.clientY;
            }
        });
        // Sync scrolling and zooming if the option is selected
        treeDivNode.addEventListener("scroll", () => {
            if (document.getElementById("syncScrollCheck").checked && treeDiv !== "#result-tree") {
                syncScroll(treeDiv);
            }
        });
        document.addEventListener("mouseup", () => {
            isDragging = false;
            if (document.querySelector(currentTreeDiv)) document.querySelector(currentTreeDiv).style.cursor = "grab";
            currentTreeDiv = null;
        });
    }

    // Event to handle zoom in/out of execution trees
    document.querySelectorAll('[aria-label="CompareExecTrees zoom"]').forEach((node) => {
        node.addEventListener("click", function (event) {
            if (event.target.tagName === "BUTTON") {
                const engine1 = document.querySelector("#select1").value;
                const engine2 = document.querySelector("#select2").value;
                if (!engine1 || !engine2) return;
                const buttonId = event.target.id;
                const purpose = buttonId.slice(0, -1);
                const treeId = `#tree${buttonId.slice(-1)}`;
                const currentFontSize = document
                    .querySelector(treeId)
                    .querySelector(".node[class*=font-size-]")
                    .className.match(/font-size-(\d+)/)[1];
                // Zoom in and out for both trees when sync option enabled
                const kb = new URLSearchParams(window.location.hash.split("?")[1]).get("kb");
                const queryIdx = new URLSearchParams(window.location.hash.split("?")[1]).get("q");
                const runtimeInfo1 = performanceData[kb][engine1].queries[queryIdx].runtime_info;
                const runtimeInfo2 = performanceData[kb][engine2].queries[queryIdx].runtime_info;
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
                            Number.parseInt(currentFontSize)
                        );
                    }
                } else {
                    let runtimeInfo = treeId === "#tree1" ? runtimeInfo1 : runtimeInfo2;
                    renderExecTree(
                        runtimeInfo,
                        `#tree${buttonId.slice(-1)}`,
                        `#meta-info-${buttonId.slice(-1)}`,
                        purpose,
                        Number.parseInt(currentFontSize)
                    );
                }
            }
        });
    });

    document.querySelector("#compareExecTreesBtn").addEventListener("click", () => {
        const select1Value = document.querySelector("#select1").value;
        const select2Value = document.querySelector("#select2").value;

        if (!select1Value || !select2Value) {
            alert("Please select both QLever instances before comparing.");
            return;
        }
        if (!engineStatForQuery) {
            showPage("error", "No Query Execution Tree Information found!");
            return;
        }

        // Continue with comparison logic
        let s1RuntimeTree = null;
        let s2RuntimeTree = null;
        for (const [engine, stats] of Object.entries(engineStatForQuery)) {
            const runtimeInfo = stats.runtime_info;
            if (engine === select1Value) {
                s1RuntimeTree = runtimeInfo;
            }
            if (engine === select2Value) {
                s2RuntimeTree = runtimeInfo;
            }
        }

        for (let [runtime_info, tree_idx] of [
            [s1RuntimeTree, "1"],
            [s2RuntimeTree, "2"],
        ]) {
            renderExecTree(runtime_info, `#tree${tree_idx}`, `#meta-info-${tree_idx}`);
        }
    });
}

/**
 * Synchronize the scrolling between the 2 compare exec trees if enabled
 * @param {string} sourceTree - ID of the tree where the scroll is performed. Can be #tree1 or #tree2
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

/**
 * Calculates the new font size for tree nodes based on the purpose (zoom in/out or modal show) and current size.
 * @param {Object} tree - The tree structure object.
 * @param {string} purpose - The purpose of font size adjustment (e.g., "showTree", "zoomIn", "zoomOut").
 * @param {number} currentFontSize - The current font size of the tree nodes.
 * @returns {number} - The new font size.
 */
function getNewFontSizeForTree(tree, purpose, currentFontSize) {
    let treeDepth;
    let newFontSize = currentFontSize ? currentFontSize : maximumZoomPercent;
    if (purpose === "showTree") {
        treeDepth = calculateTreeDepth(tree.nodeStructure);
        newFontSize = getFontSizeForDepth(baseTreeTextFontSize, treeDepth);
    } else if (purpose === "zoomIn" && currentFontSize < maximumZoomPercent) {
        newFontSize += zoomChange;
    } else if (purpose === "zoomOut" && currentFontSize > minimumZoomPercent) {
        newFontSize -= zoomChange;
    }
    return newFontSize;
}

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

function getEnginesWithExecTrees(performanceDataForKb) {
    let execTreeEngines = [];

    for (let [engine, engineStat] of Object.entries(performanceDataForKb)) {
        const queries = engineStat.queries;
        for (const query of queries) {
            if (Array.isArray(query.results)) {
                if (!Object.hasOwn(query.runtime_info, "query_execution_tree")) break;
                else {
                    // execTreeEngines.push({ engine: engine, stats: engineStat });
                    execTreeEngines.push(engine);
                    break;
                }
            }
        }
    }
    return execTreeEngines;
}

function updateCompareExecTreesPage(kb, query, queryEngineStat) {
    const titleNode = document.querySelector("#main-page-header");
    const queryNode = document.querySelector("#compareExecQuery");
    const title = `Query Execution Tree comparison - ${capitalize(kb)}`;

    const queryTitle = `<strong>QUERY:</strong>   ${query}`;
    let sparql = null;
    for (const engineStat of Object.values(queryEngineStat)) {
        if (engineStat.sparql) {
            sparql = engineStat.sparql;
            break;
        }
    }

    if (titleNode.innerHTML === title && queryNode.innerHTML === queryTitle) return;
    removeTitleInfoPill();
    titleNode.innerHTML = title;
    queryNode.innerHTML = queryTitle;
    queryNode.title = sparql;

    for (let i = 1; i <= 2; i++) {
        document.querySelector(`#meta-info-${i}`).innerHTML = "";
        document.querySelector(`#tree${i}`).innerHTML = "";
    }

    engineStatForQuery = queryEngineStat;
    const engines = Object.keys(queryEngineStat);

    populateSelect(document.querySelector("#select1"), engines, 0);
    populateSelect(document.querySelector("#select2"), engines, 1);
}
