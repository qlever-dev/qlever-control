var performanceData;
var router;
/**
 * Capitalizes the first letter of a string.
 *
 * @param {string} str - Input string
 * @returns {string} Capitalized string
 */
function capitalize(str) {
    return str.charAt(0).toUpperCase() + str.slice(1);
}

/**
 * Extract the core value from a SPARQL result value.
 *
 * @param {string | string[]} sparqlValue - The raw SPARQL value or list of values
 * @returns {string} The extracted core value or empty string if none
 */
function extractCoreValue(sparqlValue) {
    if (Array.isArray(sparqlValue)) {
        if (sparqlValue.length === 0) return "";
        sparqlValue = sparqlValue[0];
    }

    if (typeof sparqlValue !== "string" || !sparqlValue.trim()) return "";

    // URI enclosed in angle brackets
    if (sparqlValue.startsWith("<") && sparqlValue.endsWith(">")) {
        return sparqlValue.slice(1, -1);
    }

    // Literal string like "\"Some value\""
    const literalMatch = sparqlValue.match(/^"((?:[^"\\]|\\.)*)"/);
    if (literalMatch) {
        const raw = literalMatch[1];
        return raw.replace(/\\(.)/g, "$1");
    }

    // Fallback - return as is
    return sparqlValue;
}

function showPage(pageId, siteErrorMsg = null) {
    // Hide all pages
    document.querySelectorAll(".page").forEach((p) => {
        p.classList.remove("visible");
        p.classList.add("hidden");
    });

    // Show requested page with animation
    const page = document.getElementById(`page-${pageId}`);
    if (page) {
        page.classList.remove("hidden");
        // Force reflow for transition to trigger
        void page.offsetWidth;
        page.classList.add("visible");
        if (pageId === "error" && siteErrorMsg !== null) {
            document.querySelector("#siteErrorMsg").innerText = siteErrorMsg;
        }
    }
}

function getGridRowData(rowCount, tableData) {
    return Array.from({ length: rowCount }, (_, i) => {
        const row = {};
        for (const col of Object.keys(tableData)) {
            row[col] = tableData[col][i];
        }
        return row;
    });
}

/**
 * Extracts a single result string from query data if exactly one result exists.
 *
 * @param {Object} queryData - Single query data object
 * @returns {string | null} Formatted single result or null if not applicable
 */
function getSingleResult(queryData) {
    let resultSize = queryData.result_size ?? 0;
    let singleResult = null;

    if (
        resultSize === 1 &&
        Array.isArray(queryData.headers) &&
        queryData.headers.length === 1 &&
        Array.isArray(queryData.results) &&
        queryData.results.length === 1
    ) {
        const resultValue = extractCoreValue(queryData.results[0]);
        // Try formatting as int with commas
        const intVal = parseInt(resultValue, 10);
        if (!isNaN(intVal)) {
            singleResult = intVal.toLocaleString();
        }
    }
    return singleResult;
}

/**
 * Displays the loading spinner by updating the relevant CSS classes.
 */
function showSpinner() {
  document.querySelector("#spinner").classList.remove("d-none", "d-flex");
  document.querySelector("#spinner").classList.add("d-flex");
}

/**
 * Hides the loading spinner by updating the relevant CSS classes.
 */
function hideSpinner() {
  document.querySelector("#spinner").classList.remove("d-none", "d-flex");
  document.querySelector("#spinner").classList.add("d-none");
}

function addTextElementsToExecTreeForTreant(tree_node, is_ancestor_cached = false) {
    if (tree_node["text"] == undefined) {
        var text = {};
        if (tree_node["column_names"] == undefined) {
            tree_node["column_names"] = ["not yet available"];
        }
        // Rewrite runtime info from QLever as follows:
        //
        // 1. Abbreviate IRIs (only keep part after last / or # or dot)
        // 2. Remove qlc_ and _qlever_internal_... prefixes from variable names
        // 3. Lowercase fully capitalized words (with _)
        // 4. Separate CamelCase word parts by hyphen (Camel-Case)
        // 5. First word in ALL CAPS (like JOIN or INDEX-SCAN)
        // 6. Replace hyphen in all caps by space (INDEX SCAN)
        // 7. Abbreviate long QLever-internal variable names
        //
        text["name"] = tree_node["description"]
            .replace(/<[^>]*[#\/\.]([^>]*)>/g, "<$1>")
            .replace(/qlc_/g, "")
            .replace(/_qlever_internal_variable_query_planner/g, "")
            .replace(/\?[A-Z_]*/g, function (match) {
                return match.toLowerCase();
            })
            .replace(/([a-z])([A-Z])/g, "$1-$2")
            .replace(/^([a-zA-Z-])*/, function (match) {
                return match.toUpperCase();
            })
            .replace(/([A-Z])-([A-Z])/g, "$1 $2")
            .replace(/AVAILABLE /, "")
            .replace(/a all/, "all");

        text["cols"] = tree_node["column_names"]
            .join(", ")
            .replace(/qlc_/g, "")
            .replace(/_qlever_internal_variable_query_planner/g, "")
            .replace(/\?[A-Z_]*/g, function (match) {
                return match.toLowerCase();
            });
        text["size"] = formatInteger(tree_node["result_rows"]) + " x " + formatInteger(tree_node["result_cols"]);
        text["size-estimate"] = "[~ " + formatInteger(tree_node["estimated_size"]) + "]";
        text["cache-status"] = is_ancestor_cached
            ? "ancestor_cached"
            : tree_node["cache_status"]
            ? tree_node["cache_status"]
            : tree_node["was_cached"]
            ? "cached_not_pinned"
            : "computed";
        text["time"] =
            tree_node["cache_status"] == "computed" || tree_node["was_cached"] == false
                ? formatInteger(tree_node["operation_time"])
                : formatInteger(tree_node["original_operation_time"]);
        text["cost-estimate"] = "[~ " + formatInteger(tree_node["estimated_operation_cost"]) + "]";
        text["status"] = tree_node["status"];
        if (text["status"] == "not started") {
            text["status"] = "not yet started";
        }
        text["total"] = text["time"];
        if (tree_node["details"]) {
            text["details"] = JSON.stringify(tree_node["details"]);
        }

        // Delete all other keys except "children" (we only needed them here to
        // create a proper "text" element) and the "text" element.
        for (var key in tree_node) {
            if (key != "children") {
                delete tree_node[key];
            }
        }
        tree_node["text"] = text;

        // Check out https://fperucic.github.io/treant-js
        // TODO: Do we still need / want this?
        tree_node["stackChildren"] = true;

        // Recurse over all children. Propagate "cached" status.
        tree_node["children"].map((child) =>
            addTextElementsToExecTreeForTreant(child, is_ancestor_cached || text["cache-status"] != "computed")
        );
    }
}

function formatInteger(number) {
    return number.toString().replace(/(\d)(?=(\d{3})+(?!\d))/g, "$1,");
}

function renderExecTree(runtime_info, treeNodeId, metaNodeId, purpose = "showTree", currentFontSize) {
    // Show meta information (if it exists).
    const meta_info = runtime_info["meta"];

    const time_query_planning =
        "time_query_planning" in meta_info
            ? formatInteger(meta_info["time_query_planning"]) + " ms"
            : "[not available]";

    const time_index_scans_query_planning =
        "time_index_scans_query_planning" in meta_info
            ? formatInteger(meta_info["time_index_scans_query_planning"]) + " ms"
            : "[not available]";

    const total_time_computing =
        "total_time_computing" in meta_info ? formatInteger(meta_info["total_time_computing"]) + " ms" : "N/A";

    // Inject meta info into the DOM
    document.querySelector(metaNodeId).innerHTML = `<p>Time for query planning: ${time_query_planning}<br/>
    Time for index scans during query planning: ${time_index_scans_query_planning}<br/>
    Total time for computing the result: ${total_time_computing}</p>`;

    // Show the query execution tree (using Treant.js)
    addTextElementsToExecTreeForTreant(runtime_info["query_execution_tree"]);

    const treant_tree = {
        chart: {
            container: treeNodeId,
            rootOrientation: "NORTH",
            connectors: { type: "step" },
            node: { HTMLclass: "font-size-" + maximumZoomPercent },
        },
        nodeStructure: runtime_info["query_execution_tree"],
    };
    const newFontSize = getNewFontSizeForTree(treant_tree, purpose, currentFontSize);
    treant_tree.chart.node.HTMLclass = "font-size-" + newFontSize.toString();

    // Create new Treant tree
    new Treant(treant_tree);

    // Add tooltips with parsed .node-details info
    document.querySelectorAll("div.node").forEach(function (node) {
        const detailsChild = node.querySelector(".node-details");
        if (detailsChild) {
            const topPos = parseFloat(window.getComputedStyle(node).top);
            node.setAttribute("data-bs-toggle", "tooltip");
            node.setAttribute("data-bs-html", "true");
            node.setAttribute("data-bs-placement", topPos > 100 ? "top" : "bottom");

            let detailHTML = "";
            const details = JSON.parse(detailsChild.textContent);
            for (const key in details) {
                detailHTML += `<span>${key}: <strong>${details[key]}</strong></span><br>`;
            }

            node.setAttribute(
                "data-bs-title",
                `<div style="width: 250px">
                    <h6> Details </h6>
                    <div style="margin-top: 10px; margin-bottom: 10px;">
                    ${detailHTML}
                    </div>
                </div>`
            );

            // Manually initialize Bootstrap tooltip
            new bootstrap.Tooltip(node);
        }
    });

    const high_query_time_ms = 100;
    const very_high_query_time_ms = 1000;

    // Highlight high/very high node-time values
    document.querySelectorAll("p.node-time").forEach(function (p) {
        const time = parseInt(p.textContent.replace(/,/g, ""));
        if (time >= high_query_time_ms) {
            p.parentElement.classList.add("high");
        }
        if (time >= very_high_query_time_ms) {
            p.parentElement.classList.add("veryhigh");
        }
    });

    // Add cache status classes
    document.querySelectorAll("p.node-cache-status").forEach(function (p) {
        const status = p.textContent;
        const parent = p.parentElement;

        if (status === "cached_not_pinned") {
            parent.classList.add("cached-not-pinned", "cached");
        } else if (status === "cached_pinned") {
            parent.classList.add("cached-pinned", "cached");
        } else if (status === "ancestor_cached") {
            parent.classList.add("ancestor-cached", "cached");
        }
    });

    // Add status classes
    document.querySelectorAll("p.node-status").forEach(function (p) {
        const status = p.textContent;
        const parent = p.parentElement;

        switch (status) {
            case "fully materialized":
                p.classList.add("fully-materialized");
                break;
            case "lazily materialized":
                p.classList.add("lazily-materialized");
                break;
            case "failed":
                p.classList.add("failed");
                break;
            case "failed because child failed":
                p.classList.add("child-failed");
                break;
            case "not yet started":
                parent.classList.add("not-started");
                break;
            case "optimized out":
                p.classList.add("optimized-out");
                break;
        }
    });

    // Add title for truncated node names and cols
    document.querySelectorAll("#result-tree p.node-name, #result-tree p.node-cols").forEach(function (p) {
        p.setAttribute("title", p.textContent);
    });
}

/**
 * Calculates the depth of a tree structure, where depth is the longest path from the root to any leaf node.
 * @param {Object} obj - The tree node or root object.
 * @returns {number} - The depth of the tree.
 */
function calculateTreeDepth(obj) {
    // Base case: if the object has no children, return 1
    if (!obj.children || obj.children.length === 0) {
        return 1;
    }
    // Initialize maxDepth to track the maximum depth
    let maxDepth = 0;
    // Calculate depth for each child and find the maximum depth
    obj.children.forEach((child) => {
        const depth = calculateTreeDepth(child);
        maxDepth = Math.max(maxDepth, depth);
    });
    // Return maximum depth + 1 (to account for the current node)
    return maxDepth + 1;
}

/**
 * Determines the font size for a tree visualization based on its depth, ensuring text is appropriately sized.
 * @param {number} fontSize - The base font size.
 * @param {number} depth - The depth of the tree.
 * @returns {number} - The adjusted font size.
 */
function getFontSizeForDepth(fontSize, depth) {
    // If depth is greater than 4, reduce font size by 10 for each increment beyond 4
    if (depth > 4) {
        fontSize -= (depth - 4) * zoomChange;
    }
    // Ensure font size doesn't go below 30
    fontSize = Math.max(fontSize, minimumZoomPercent);
    return fontSize;
}

function goToCompareExecTreesPage(agGridApi, tableName) {
    if (!agGridApi) return;
    const selectedNode = agGridApi.getSelectedNodes();
    if (selectedNode.length === 1) {
        const selectedRowIdx = selectedNode[0].rowIndex;
        const kb = new URLSearchParams(window.location.hash.split("?")[1]).get("kb");
        router.navigate(`/compareExecTrees?kb=${encodeURIComponent(kb)}&q=${selectedRowIdx}`);
    } else {
        alert(`Please select a query from the ${tableName} Table!`);
    }
}

function sortEngines(engines, kb, metric, order) {
    return engines.slice().sort((a, b) => {
        const left = order === "asc" ? a : b;
        const right = order === "asc" ? b : a;
        return performanceData[kb][left][metric] - performanceData[kb][right][metric];
    });
}

function extractFirstUrl(text) {
    // Regex matches http(s):// and www. patterns, stops before spaces or closing punctuation
    const regex = /\b((?:https?:\/\/|www\.)[^\s<>"]+[^.,;:!?()\[\]{}<>\s"])/i;
    const match = text.match(regex);

    if (match) {
        let url = match[1].trim();

        // Normalize URLs starting with www.
        if (url.startsWith("www.")) {
            url = "http://" + url;
        }

        return url;
    }
    return null;
}

function createBenchmarkDescriptionInfoPill(indexDescription, tooltipPlacement = "right") {
    const infoPill = document.createElement("a");
    infoPill.setAttribute("tabindex", 0);
    
    infoPill.className = "mx-2";
    infoPill.style.color = "var(--bs-body-color)";
    infoPill.style.cursor = "pointer";
    
    const icon = document.createElement("i");
    icon.className = "bi bi-info-circle-fill";
    icon.style.fontSize = "0.8rem";
    infoPill.appendChild(icon);

    // Bootstrap popover attributes
    infoPill.setAttribute("data-bs-toggle", "popover");
    infoPill.setAttribute("data-bs-trigger", "focus");
    infoPill.setAttribute("data-bs-placement", tooltipPlacement);
    infoPill.setAttribute("data-bs-html", "true");
    infoPill.setAttribute(
        "data-bs-content",
        anchorme({
            input: indexDescription,
            options: { attributes: { target: "_blank", class: "text-primary" } },
        })
    );

    return infoPill;
}


function removeTitleInfoPill() {
    document.querySelector("#mainTitleWrapper a")?.remove();
}

function createTooltipContainer(params) {
    const isSparql = typeof params.value !== "string";
    const tooltipText = isSparql ? params.value.sparql : params.value;
    const tooltipTitle = params.value.title;

    const container = document.createElement("div");
    container.className = "custom-tooltip";

    const textDiv = document.createElement("div");
    textDiv.className = "tooltip-text";
    const pre = document.createElement("pre");
    pre.textContent = tooltipText;
    if (tooltipTitle) {
        textDiv.innerHTML = `<b>${tooltipTitle}</b><br><br>`;
    }
    if (isSparql) {
        textDiv.appendChild(pre);
    } else {
        textDiv.textContent = tooltipText;
    }
    container.appendChild(textDiv);
    return container;
}

function escapeLatex(str) {
    if (!str) return "";

    const replacements = {
        "\\": "\\textbackslash{}",
        "{": "\\{",
        "}": "\\}",
        $: "\\$",
        "&": "\\&",
        "#": "\\#",
        _: "\\_",
        "%": "\\%",
        "~": "\\textasciitilde{}",
        "^": "\\textasciicircum{}",
        "<": "\\textless{}",
        ">": "\\textgreater{}",
    };

    return str.replace(/([\\{}$&#_%~^<>])/g, (match) => replacements[match] || match).trim();
}

function csvToLatexTable(csvText) {
    // Split CSV into rows
    const rows = csvText
        .trim()
        .split("\n")
        .map((row) => row.split(","));

    const colAlign = "l" + "r".repeat(rows[0].length - 1);

    // Build LaTeX table
    let latex = `% Required packages (uncomment in LaTeX preamble):
% \\usepackage{booktabs}
% \\usepackage[table]{xcolor}

\\rowcolors{2}{gray!15}{white}
\\begin{tabular}{${colAlign}}
\\toprule
${rows[0].map((h) => `\\textbf{${escapeLatex(h)}}`).join(" & ")} \\\\ 
\\midrule
${rows
    .slice(1)
    .map((r) => r.map((c) => escapeLatex(c)).join(" & ") + " \\\\")
    .join("\n")}
\\bottomrule
\\end{tabular}`;

    return latex;
}
