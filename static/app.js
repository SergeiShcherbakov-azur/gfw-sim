const state = {
  sim: null,
  nodes: [],
  selectedNode: null,
  sortKey: "default",
  sortDir: "desc",
  sortMode: "req", 
  sortPodKey: "default",
  sortPodDir: "desc",
  pendingMove: null,
  highlightedNodes: new Set(),
  lastMovedPodId: null
};

function fmtUsd(x) {
  if (x === null || x === undefined || isNaN(x)) return "0.00";
  return Number(x).toFixed(4);
}

function fmtMiB(bytes) {
  if (!bytes) return "0";
  return (bytes / (1024 * 1024)).toFixed(0);
}

function clampPct(x) {
  if (!isFinite(x)) return 0;
  return Math.max(0, Math.min(100, x));
}

function el(id) {
  return document.getElementById(id);
}

async function apiGet(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error("HTTP " + r.status);
  return await r.json();
}

async function apiPost(url, body) {
  const r = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: body ? JSON.stringify(body) : "{}",
  });
  const txt = await r.text();
  if (!r.ok) {
    throw new Error(url + " → HTTP " + r.status + ": " + (txt || ""));
  }
  return txt ? JSON.parse(txt) : {};
}

async function apiMutateOne(op) {
  return await apiPost("/mutate", {
    operations: [op]
  });
}

function setBusy(b) {
  const btns = ["btnRefresh", "btnReset", "btnRefreshPrices", "btnCapture", "selSnapshots"];
  btns.forEach(id => {
    if (el(id)) el(id).disabled = b;
  });
  const p = el("pillLoaded");
  if (p) p.textContent = b ? "loading…" : "ok";
}

function setMsg(text, isErr = false) {
  const m = el("msg");
  m.textContent = text;
  m.className = "msg" + (isErr ? " err" : "");
}

// --- Snapshot Management ---
async function loadSnapshotsList() {
  try {
    const list = await apiGet("/snapshots");
    const sel = el("selSnapshots");
    sel.innerHTML = "";

    list.forEach(s => {
      const opt = document.createElement("option");
      opt.value = s.id;
      // Используем toLocaleString для даты
      const date = s.id.startsWith("k8s-") ?
        new Date(parseInt(s.id.split("-")[1]) * 1000).toLocaleString() :
        s.id;
      opt.textContent = `${date} (${s.nodes_count}n / ${s.pods_count}p)`;
      if (s.is_active) opt.selected = true;
      sel.appendChild(opt);
    });
  } catch (e) {
    console.error("Failed to load snapshots list", e);
  }
}

el("selSnapshots").addEventListener("change", async (e) => {
  const id = e.target.value;
  setBusy(true);
  try {
    await apiPost(`/snapshots/${id}/activate`);
    await loadSim();
    setMsg(`Switched to snapshot: ${id}`);
  } catch (err) {
    setMsg("Error switching: " + err, true);
  } finally {
    setBusy(false);
  }
});

el("btnCapture").addEventListener("click", async () => {
  if (!confirm("Собрать текущее состояние кластера? Это может занять несколько секунд.")) return;
  setBusy(true);
  setMsg("Connecting to K8s & capturing state...");
  try {
    const res = await apiPost("/snapshots/capture");
    await loadSnapshotsList();

    el("selSnapshots").value = res.id;
    await apiPost(`/snapshots/${res.id}/activate`);

    await loadSim();
    setMsg("Snapshot captured successfully!");
  } catch (err) {
    setMsg("Capture failed: " + (err.message || err), true);
  } finally {
    setBusy(false);
  }
});

// --- Modal Logic ---
function openModal(podId, targetNode, plan) {
  state.pendingMove = {
    podId,
    targetNode, 
    plan
  };
  el("mPodId").textContent = podId;
  el("mTargetNode").textContent = targetNode; 
  
  if (plan.owner_name && plan.owner_kind) {
    el("mOwnerGroup").style.display = "flex";
    el("mCheckOwner").checked = false;
    el("mOwnerKind").textContent = plan.owner_kind;
    el("mOwnerName").textContent = plan.owner_name;
  } else {
    el("mOwnerGroup").style.display = "none";
  }
  el("mCpu").value = plan.current_req_cpu_m;
  el("mMem").value = plan.current_req_mem_b;
  el("mTolerations").value = JSON.stringify(plan.suggested_tolerations, null, 2);
  el("mSelector").value = JSON.stringify(plan.suggested_node_selector, null, 2);
  el("moveModalOverlay").style.display = "flex";
}

function closeModal() {
  el("moveModalOverlay").style.display = "none";
  state.pendingMove = null;
}

// --- Changelog Modal Logic ---
el("btnChangelog").addEventListener("click", () => {
    el("changelogModal").style.display = "flex";
});

const closeChangelog = () => el("changelogModal").style.display = "none";
el("btnCloseChangelog").addEventListener("click", closeChangelog);
el("btnCloseChangelogBtn").addEventListener("click", closeChangelog);


async function commitMove() {
  if (!state.pendingMove) return;
  const {
    podId,
    targetNode,
    plan
  } = state.pendingMove;
  const isOwner = el("mCheckOwner").checked;
  let req_cpu, req_mem, tols, sel;
  try {
    req_cpu = parseInt(el("mCpu").value);
    req_mem = parseInt(el("mMem").value);
    tols = JSON.parse(el("mTolerations").value);
    sel = JSON.parse(el("mSelector").value);
  } catch (e) {
    alert("Ошибка парсинга JSON или чисел: " + e.message);
    return;
  }
  const overrides = {
    req_cpu_m: req_cpu,
    req_mem_b: req_mem,
    tolerations: tols,
    node_selector: sel
  };
  
  const targetNodeObj = state.nodes.find(n => n.node === targetNode);
  const targetPool = targetNodeObj ? targetNodeObj.nodepool : null;
  
  // Track affected pod to highlight its new node later
  state.lastMovedPodId = podId;
  state.highlightedNodes.clear();

  let op = {};
  if (isOwner && plan.owner_name && targetPool) {
    op = {
      op: "move_owner_to_pool",
      namespace: podId.split("/")[0],
      owner_kind: plan.owner_kind,
      owner_name: plan.owner_name,
      target_pool: targetPool,
      overrides: overrides
    };
  } else {
    op = {
      op: "move_pod_to_node",
      pod_ids: [podId],
      node_name: targetNode,
      overrides: overrides
    };
  }
  closeModal();
  setBusy(true);
  setMsg("Выполняется перенос...");
  try {
    await apiMutateOne(op);
    await loadSim(); 
    setMsg("Успешно перенесено!");
  } catch (e) {
    setMsg("Ошибка переноса: " + e.message, true);
    state.lastMovedPodId = null;
  } finally {
    setBusy(false);
  }
}

// --- Render logic ---

function buildRequestBar(parts, allocTotal) {
  const gfw = parts?.gfw ?? 0;
  const ds = parts?.ds ?? 0;
  const oth = parts?.oth ?? 0;
  const used = Math.max(0, gfw + ds + oth);
  const free = Math.max(0, allocTotal - used);
  const denom = Math.max(1, allocTotal);

  const wG = clampPct(100 * gfw / denom);
  const wD = clampPct(100 * ds / denom);
  const wO = clampPct(100 * oth / denom);
  const wF = clampPct(100 * free / denom);

  const bar = document.createElement("div");
  bar.className = "bar";
  const mk = (cls, w) => {
    const s = document.createElement("div");
    s.className = "seg " + cls;
    s.style.width = w.toFixed(3) + "%";
    return s;
  };
  bar.appendChild(mk("gfw", wG));
  bar.appendChild(mk("ds", wD));
  bar.appendChild(mk("oth", wO));
  bar.appendChild(mk("free", wF));
  return bar;
}

function buildUsageBar(usage, allocTotal) {
  const denom = Math.max(1, allocTotal);
  const usageVal = Math.max(0, usage);
  const usagePct = 100 * usageVal / denom;
  const bar = document.createElement("div");
  bar.className = "bar thin";
  
  const diff = usageVal - allocTotal;
  const diffText = diff > 0 ? `+${diff}` : "";
  bar.title = `Usage: ${usageVal} / Alloc: ${allocTotal} (${diffText})`;

  if (usageVal <= allocTotal) {
    const s = document.createElement("div");
    s.className = "seg usage";
    s.style.width = clampPct(usagePct).toFixed(3) + "%";
    bar.appendChild(s);
  } else {
    const s1 = document.createElement("div");
    s1.className = "seg usage";
    s1.style.width = "100%";
    
    const label = document.createElement("span");
    label.style.position = "absolute";
    label.style.right = "0";
    label.style.top = "-12px";
    label.style.color = "#ff6b6b";
    label.style.fontSize = "10px";
    label.style.fontWeight = "bold";
    label.textContent = `+${diff}`;
    
    bar.style.position = "relative";
    bar.style.overflow = "visible"; 
    
    const s2 = document.createElement("div");
    s2.className = "seg over"; // Red
    s2.style.width = "100%";
    bar.appendChild(s2);
    bar.appendChild(label);
  }
  return bar;
}

function nodeCpuParts(n) {
  return {
    gfw: n.parts?.gfw_cpu_m ?? 0,
    ds: n.parts?.ds_cpu_m ?? 0,
    oth: n.parts?.other_cpu_m ?? 0
  };
}

function nodeMemPartsMiB(n) {
  return {
    gfw: (n.parts?.gfw_mem_b ?? 0) / (1024 * 1024),
    ds: (n.parts?.ds_mem_b ?? 0) / (1024 * 1024),
    oth: (n.parts?.other_mem_b ?? 0) / (1024 * 1024),
  };
}

function updateSortHeaders() {
    document.querySelectorAll("th[data-sort]").forEach(th => {
        const label = th.getAttribute("data-label");
        if(!label) return;
        
        let suffix = "";
        const key = th.dataset.sort;
        
        if (state.sortKey === key) {
            const arrow = state.sortDir === "asc" ? "↑" : "↓";
            let modeInfo = "";
            if ((key === "cpu" || key === "ram") && state.sortMode === "use") {
                modeInfo = " (Use)";
            } else if ((key === "cpu" || key === "ram") && state.sortMode === "req") {
                modeInfo = " (Req)";
            }
            suffix = ` ${arrow}${modeInfo}`;
            th.style.color = "#fff";
        } else {
            th.style.color = "";
        }
        th.textContent = label + suffix;
    });
}

function sortNodes() {
  const key = state.sortKey;
  const dir = state.sortDir === "asc" ? 1 : -1;
  const mode = state.sortMode; 

  const val = (n) => {
    if (n.is_virtual) return -999999999 * dir; 
    
    if (key === "default") {
        const aKeda = (n.nodepool || "").toLowerCase().includes("keda");
        if (aKeda) return -1000000 * dir;
        return n.node.localeCompare(n.node) * dir;
    }

    if (key === "node") return n.node || "";
    if (key === "nodepool") return n.nodepool || "";
    if (key === "cost") return n.cost_daily_usd || 0;
    
    if (key === "cpu") {
        const div = Math.max(1, n.alloc_cpu_m || 1);
        if (mode === "use") return (n.sum_usage_cpu_m || 0) / div;
        return (n.sum_req_cpu_m || 0) / div;
    }
    
    if (key === "ram") {
        const div = Math.max(1, n.alloc_mem_b || 1);
        if (mode === "use") return (n.sum_usage_mem_b || 0) / div;
        return (n.sum_req_mem_b || 0) / div;
    }
    return 0;
  };

  state.nodes.sort((a, b) => {
    if (a.is_virtual && !b.is_virtual) return -1;
    if (!a.is_virtual && b.is_virtual) return 1;

    const va = val(a), vb = val(b);
    if (typeof va === "string" || typeof vb === "string") {
      return String(va).localeCompare(String(vb)) * dir;
    }
    return (va - vb) * dir;
  });
  
  updateSortHeaders();
}

function setupSortListeners() {
    document.querySelectorAll("th[data-sort]").forEach(th => {
        th.setAttribute("data-label", th.textContent);
        
        th.addEventListener("click", () => {
            const key = th.dataset.sort;
            
            if (state.sortKey !== key) {
                state.sortKey = key;
                state.sortDir = 'desc';
                state.sortMode = 'req'; 
            } else {
                if (key === 'cpu' || key === 'ram') {
                    if (state.sortMode === 'req' && state.sortDir === 'desc') {
                        state.sortDir = 'asc';
                    } else if (state.sortMode === 'req' && state.sortDir === 'asc') {
                        state.sortMode = 'use';
                        state.sortDir = 'desc';
                    } else if (state.sortMode === 'use' && state.sortDir === 'desc') {
                        state.sortDir = 'asc';
                    } else {
                        state.sortMode = 'req';
                        state.sortDir = 'desc';
                    }
                } else {
                    state.sortDir = state.sortDir === 'asc' ? 'desc' : 'asc';
                }
            }
            renderNodes();
        });
    });
}

function updatePodSortHeaders() {
    document.querySelectorAll("th[data-sort-pod]").forEach(th => {
        const label = th.getAttribute("data-label");
        if(!label) return;
        
        let suffix = "";
        const key = th.dataset.sortPod;
        
        if (state.sortPodKey === key) {
            const arrow = state.sortPodDir === "asc" ? "↑" : "↓";
            suffix = ` ${arrow}`;
            th.style.color = "#fff";
        } else {
            th.style.color = "";
        }
        th.textContent = label + suffix;
    });
}

function sortPodsList(list) {
    const key = state.sortPodKey;
    const dir = state.sortPodDir === "asc" ? 1 : -1;

    return list.slice().sort((a, b) => {
        if (key === "default") {
             if (a.is_daemon !== b.is_daemon) {
                 return a.is_daemon ? 1 : -1;
             }
             return (b.req_cpu_m || 0) - (a.req_cpu_m || 0);
        }

        let va, vb;
        if (key === "ns") { va = a.namespace; vb = b.namespace; }
        else if (key === "name") { va = a.name; vb = b.name; }
        else if (key === "active") { va = a.active_ratio ?? 1; vb = b.active_ratio ?? 1; }
        else if (key === "type") { va = a.is_daemon ? 1 : 0; vb = b.is_daemon ? 1 : 0; }
        else if (key === "cpu") { va = a.req_cpu_m || 0; vb = b.req_cpu_m || 0; }
        else if (key === "mem") { va = a.req_mem_b || 0; vb = b.req_mem_b || 0; }
        else { return 0; }

        if (typeof va === "string") return va.localeCompare(vb) * dir;
        return (va - vb) * dir;
    });
}

function setupPodSortListeners() {
    document.querySelectorAll("th[data-sort-pod]").forEach(th => {
         th.setAttribute("data-label", th.textContent);
         th.addEventListener("click", () => {
             const key = th.dataset.sortPod;
             if (state.sortPodKey === key) {
                 state.sortPodDir = state.sortPodDir === 'asc' ? 'desc' : 'asc';
             } else {
                 state.sortPodKey = key;
                 state.sortPodDir = 'desc'; 
                 if (key === 'ns' || key === 'name') state.sortPodDir = 'asc';
             }
             renderPods();
         });
    });
}

function renderLogs(sim) {
  const logs = sim.logs || [];
  const list = el("changelogList");
  
  const countEl = el("logCount");
  if(countEl) countEl.textContent = String(logs.length);

  list.innerHTML = "";
  
  if (!logs.length) {
      list.innerHTML = `<div style="padding:12px;color:var(--muted);text-align:center">Нет изменений</div>`;
  } else {
      logs.slice(0, 50).forEach(log => {
          const item = document.createElement("div");
          item.className = "log-item";
          const time = new Date(log.timestamp * 1000).toLocaleTimeString();
          let html = `
            <div style="display:flex;justify-content:space-between">
                <span class="log-msg">${log.message}</span>
                <span class="log-time">${time}</span>
            </div>
          `;
          if (log.details && Object.keys(log.details).length > 0) {
              const json = JSON.stringify(log.details, null, 2);
              html += `<div class="log-details">${json}</div>`;
              item.style.cursor = "pointer";
              item.onclick = () => item.classList.toggle("open");
          }
          item.innerHTML = html;
          list.appendChild(item);
      });
  }
}

// Helper: Универсальная функция планирования перемещения в Пул
async function handleMoveToPool(poolName, podId) {
    if (!poolName) {
        setMsg("Не удалось определить целевой NodePool.", true);
        return;
    }
    
    setMsg("Симулятор ищет лучшую ноду в пуле " + poolName + "...");
    try {
        const plan = await apiPost("/plan_move", {
          pod_id: podId,
          target_pool: poolName 
        });
        
        const chosenNode = plan.target_node;
        if (!chosenNode) {
            throw new Error("Симулятор не вернул целевую ноду в плане.");
        }
        
        openModal(podId, chosenNode, plan);
        setMsg("Симулятор выбрал ноду " + chosenNode);
    } catch (err) {
        setMsg("Ошибка планирования: " + err.message, true);
    }
}


function renderNodes() {
  const tb = el("nodesBody");
  tb.innerHTML = "";

  sortNodes();
  el("nodesCount").textContent = String(state.nodes.length);

  for (const n of state.nodes) {
    const tr = document.createElement("tr");
    tr.dataset.node = n.node;
    
    if (n.is_virtual) {
        tr.style.background = "rgba(52, 152, 219, 0.08)";
        tr.style.borderLeft = "2px solid #3498db";
    }
    
    if (state.highlightedNodes.has(n.node)) {
        tr.classList.add("row-highlight");
    }

    if (state.selectedNode && n.node === state.selectedNode) {
      tr.classList.add("selected");
    }
    tr.addEventListener("dragover", (e) => {
      e.preventDefault();
      tr.classList.add("droptarget");
    });
    tr.addEventListener("dragleave", () => {
      tr.classList.remove("droptarget");
    });
    tr.addEventListener("drop", async (e) => {
      e.preventDefault();
      tr.classList.remove("droptarget");
      const podId = e.dataTransfer.getData("text/plain");
      if (!podId) return;
      
      if (n.nodepool) {
          handleMoveToPool(n.nodepool, podId);
      } else {
          setMsg("У выбранной ноды нет NodePool!", true);
      }
    });
    tr.addEventListener("click", () => {
      state.selectedNode = n.node;
      renderNodes();
      renderPods();
    });

    const tdNode = document.createElement("td");
    tdNode.className = "col-node";
    const nn = document.createElement("span");
    nn.className = "node-name mono";
    nn.title = n.node;
    nn.textContent = n.node;
    if (n.is_virtual) nn.style.color = "#3498db";
    tdNode.appendChild(nn);

    const tdPool = document.createElement("td");
    const np = document.createElement("span");
    np.className = "nodepool mono";
    np.title = n.nodepool || "";
    np.textContent = n.nodepool || "(none)";
    tdPool.appendChild(np);

    const tdCost = document.createElement("td");
    tdCost.className = "right mono";
    tdCost.textContent = fmtUsd(n.cost_daily_usd);

    const tdCpu = document.createElement("td");
    const mCpu = document.createElement("div");
    mCpu.className = "metric";
    const topCpu = document.createElement("div");
    topCpu.className = "topline";
    const v1 = document.createElement("span");
    v1.className = "val";
    v1.textContent = `${n.sum_req_cpu_m} / ${n.sum_usage_cpu_m || 0} m`;
    topCpu.innerHTML = `<span class="label">CPU</span>`;
    topCpu.appendChild(v1);

    const barsCpu = document.createElement("div");
    barsCpu.className = "bar-group";
    barsCpu.appendChild(buildRequestBar(nodeCpuParts(n), Math.max(1, n.alloc_cpu_m || 1)));
    if (n.sum_usage_cpu_m) {
      barsCpu.appendChild(buildUsageBar(n.sum_usage_cpu_m, n.alloc_cpu_m));
    }

    mCpu.appendChild(topCpu);
    mCpu.appendChild(barsCpu);
    tdCpu.appendChild(mCpu);

    const tdRam = document.createElement("td");
    const mRam = document.createElement("div");
    mRam.className = "metric";
    const topRam = document.createElement("div");
    topRam.className = "topline";
    const v2 = document.createElement("span");
    v2.className = "val";
    const reqMiB = fmtMiB(n.sum_req_mem_b || 0);
    const useMiB = fmtMiB(n.sum_usage_mem_b || 0);
    v2.textContent = `${reqMiB} / ${useMiB} MiB`;
    topRam.innerHTML = `<span class="label">RAM</span>`;
    topRam.appendChild(v2);

    const barsRam = document.createElement("div");
    barsRam.className = "bar-group";
    const memParts = nodeMemPartsMiB(n);
    const allocMiB = (n.alloc_mem_b || 0) / (1024 * 1024);
    const useMiBVal = (n.sum_usage_mem_b || 0) / (1024 * 1024);

    barsRam.appendChild(buildRequestBar(memParts, Math.max(1, allocMiB)));
    if (n.sum_usage_mem_b) {
      barsRam.appendChild(buildUsageBar(useMiBVal, allocMiB));
    }

    mRam.appendChild(topRam);
    mRam.appendChild(barsRam);
    tdRam.appendChild(mRam);

    tr.appendChild(tdNode);
    tr.appendChild(tdPool);
    tr.appendChild(tdCost);
    tr.appendChild(tdCpu);
    tr.appendChild(tdRam);

    tb.appendChild(tr);
  }
}

// ВОТ ЗДЕСЬ ДОБАВЛЕНА ПРОПУЩЕННАЯ ФУНКЦИЯ RENDERPODS
function renderPods() {
  const sim = state.sim;
  const node = state.selectedNode;
  el("selNode").textContent = node || "—";
  const tb = el("podsBody");
  tb.innerHTML = "";
  if (!sim || !node) {
    el("podsCount").textContent = "0";
    return;
  }
  
  const rawList = (sim.pods_by_node && sim.pods_by_node[node]) ? sim.pods_by_node[node] : [];
  const list = sortPodsList(rawList);
  updatePodSortHeaders();
  el("podsCount").textContent = String(list.length);

  for (const p of list) {
    const tr = document.createElement("tr");
    tr.className = "drag";
    tr.draggable = true;
    tr.addEventListener("dragstart", (e) => {
      e.dataTransfer.setData("text/plain", p.pod_id);
      e.dataTransfer.effectAllowed = "move";
    });
    const tdNs = document.createElement("td");
    tdNs.className = "mono";
    tdNs.textContent = p.namespace;
    const tdName = document.createElement("td");
    tdName.className = "mono";
    tdName.title = p.name;
    tdName.textContent = p.name;

    const tdActive = document.createElement("td");
    tdActive.className = "right mono";
    const ratio = p.active_ratio !== undefined ? p.active_ratio : 1.0;
    const hours = (ratio * 24).toFixed(1);
    const pct = (ratio * 100).toFixed(0);
    let color = "var(--text)";
    if (ratio < 0.95) color = "#f1c40f"; 
    if (ratio < 0.55) color = "#2ecc71"; 

    tdActive.style.color = color;
    tdActive.innerHTML = `${hours}h <span style="opacity:0.5;font-size:11px">(${pct}%)</span>`;

    const tdType = document.createElement("td");
    tdType.textContent = p.is_daemon ? "DaemonSet" : "Workload";
    
    const tdCpu = document.createElement("td");
    tdCpu.className = "right mono";
    tdCpu.textContent = String(p.req_cpu_m || 0);
    if (p.usage_cpu_m) {
        const uSpan = document.createElement("span");
        uSpan.style.color = "var(--muted)";
        uSpan.style.fontSize = "11px";
        uSpan.style.marginLeft = "4px";
        uSpan.textContent = `(${p.usage_cpu_m})`;
        tdCpu.appendChild(uSpan);
    }

    const tdMem = document.createElement("td");
    tdMem.className = "right mono";
    const memMiB = Math.round((p.req_mem_b || 0) / (1024 * 1024));
    tdMem.textContent = String(memMiB);
    if (p.usage_mem_b) {
        const uSpan = document.createElement("span");
        uSpan.style.color = "var(--muted)";
        uSpan.style.fontSize = "11px";
        uSpan.style.marginLeft = "4px";
        uSpan.textContent = `(${Math.round(p.usage_mem_b / 1048576)})`;
        tdMem.appendChild(uSpan);
    }

    const tdFlags = document.createElement("td");
    if (p.is_system) tdFlags.appendChild(tag("system", "sys"));
    if (p.is_daemon) tdFlags.appendChild(tag("daemonset", "ds"));

    tr.appendChild(tdNs);
    tr.appendChild(tdName);
    tr.appendChild(tdActive);
    tr.appendChild(tdType);
    tr.appendChild(tdCpu);
    tr.appendChild(tdMem);
    tr.appendChild(tdFlags);
    tb.appendChild(tr);
  }
}

function tag(txt, cls) {
  const s = document.createElement("span");
  s.className = "tag " + cls;
  s.textContent = txt;
  return s;
}

function applySummary(sim) {
  const histStats = sim.summary?.pool_stats || {};
  const projStats = sim.summary?.projected_pool_stats || {};
  
  const histTotal = sim.summary?.total_cost_daily_usd || 0;
  const projTotal = sim.summary?.projected_total_cost_usd || 0;

  const container = el("summaryBlock");
  container.innerHTML = "";

    // 1. ПОДСЧЕТ РЕАЛЬНЫХ СТАТИСТИК (ЭВРИСТИКА)
  // Чтобы исправить баг с "default" пулом (когда история = 0, а по факту ноды есть),
  // мы считаем, сколько реальных нод и какая у них стоимость прямо сейчас.
  const realStats = {}; 
  (state.nodes || []).forEach(n => {
      if (!n.is_virtual) {
          // Если nodepool не задан, считаем его 'default'
          let pName = n.nodepool || "default";
          
          if (!realStats[pName]) realStats[pName] = { count: 0, cost: 0 };
          realStats[pName].count++;
          realStats[pName].cost += (n.cost_daily_usd || 0);
      }
  });

  const diffTotal = projTotal - histTotal;
  const signTotal = diffTotal > 0 ? "+" : "";
  const colorTotal = diffTotal > 0.01 ? "#e74c3c" : (diffTotal < -0.01 ? "#2ecc71" : "rgba(255,255,255,0.5)");

  const cardTotal = document.createElement("div");
  cardTotal.className = "card";
  cardTotal.innerHTML = `
      <div class="hdr"><h3>Total Daily Cost</h3><span class="pill" id="pillLoaded">ok</span></div>
      <div class="body">
        <div class="big">${fmtUsd(histTotal)} <span style="font-size:14px;color:var(--muted)">ACTUAL</span></div>
        <div style="margin-top:4px; font-size:13px; font-family:var(--mono); color:var(--text)">
           Proj: ${fmtUsd(projTotal)} 
           <span style="color:${colorTotal}">(${signTotal}${fmtUsd(diffTotal)})</span>
        </div>
      </div>`;
  container.appendChild(cardTotal);

  const allPools = new Set([...Object.keys(histStats), ...Object.keys(projStats)]);
  const sorted = Array.from(allPools).sort((a, b) => (histStats[b]?.cost || 0) - (histStats[a]?.cost || 0));

  sorted.forEach(pool => {
    const h = histStats[pool] || { cost: 0, nodes_count: 0 };
    const p = projStats[pool] || { cost: 0, nodes_count: 0 };

    const real = realStats[pool] || { count: 0, cost: 0 };
    const baseNodes = (h.nodes_count === 0 && real.count > 0) ? real.count : h.nodes_count;
    const baseCost = (h.cost < 0.001 && real.cost > 0) ? real.cost : h.cost;

    // Считаем разницу от СКОРРЕКТИРОВАННОЙ базы
    const costDiff = p.cost - baseCost;
    const countDiff = p.nodes_count - baseNodes;

    const sCost = costDiff > 0 ? "+" : "";
    const cCost = Math.abs(costDiff) > 0.001 ? (costDiff > 0 ? "#e74c3c" : "#2ecc71") : "rgba(255,255,255,0.3)";
    let subTextCost = `<span style="color:${cCost}">${sCost}${fmtUsd(costDiff)}</span>`;
    
    let diffCostStr = "";
    if (Math.abs(costDiff) > 0.001) {
        const s = costDiff > 0 ? "+" : "";
        const c = costDiff > 0 ? "#e74c3c" : "#2ecc71";
        diffCostStr = `<span style="color:${c}">(${s}${fmtUsd(costDiff)})</span>`;
    }

    // --- NODE COUNT DISPLAY LOGIC ---
    let countText = `${p.nodes_count} nodes`;
    if (countDiff !== 0) {
        const s = countDiff > 0 ? "+" : "";
        // Red (#e74c3c) for ADD (+), Green (#2ecc71) for REMOVE (-)
        const c = countDiff > 0 ? "#e74c3c" : "#2ecc71";
        // Format: "Old -> New nodes (Diff)"
        countText = `${baseNodes} → ${p.nodes_count} nodes <span style="color:${c};font-size:11px">(${s}${countDiff})</span>`;
    }

    // Main Value: используем baseCost (который может быть скорректирован), чтобы не показывать $0.00, если данные не загрузились
    let mainVal = fmtUsd(baseCost);
    let subValLabel = "Actual"; 
    
    // Sub Value is Projected
    let subText = `Proj: ${fmtUsd(p.cost)} ${diffCostStr}`;

    // Special case: New Pool (действительно новый, которого нет в списке нод)
    if (baseCost === 0 && baseNodes === 0 && (p.cost > 0 || p.nodes_count > 0)) {
        subValLabel = "New";
    }

    const card = document.createElement("div");
    card.className = "card";
    // Add drag listeners
    card.dataset.pool = pool;
    card.addEventListener("dragover", (e) => {
        e.preventDefault();
        card.classList.add("droptarget");
    });
    card.addEventListener("dragleave", () => {
        card.classList.remove("droptarget");
    });
    card.addEventListener("drop", (e) => {
        e.preventDefault();
        card.classList.remove("droptarget");
        const podId = e.dataTransfer.getData("text/plain");
        if (podId) handleMoveToPool(pool, podId);
    });

    card.innerHTML = `
          <div class="hdr">
            <h3>${pool}</h3>
            <span class="pill" style="font-size:11px;padding:2px 6px">${countText}</span>
          </div>
          <div class="body">
            <div class="big">${mainVal}</div>
            <div style="margin-top:4px; font-size:12px; font-family:var(--mono);">
               Proj: ${fmtUsd(p.cost)} (${subTextCost})
            </div>
          </div>`;
    container.appendChild(card);
  });
}

async function loadSim() {
  setBusy(true);
  try {
    const sim = await apiGet("/simulate");
    state.sim = sim;
    state.nodes = (sim.nodes || []).slice();
    if (!state.selectedNode) {
      state.selectedNode = state.nodes.length ? state.nodes[0].node : null;
    } else {
      if (!state.nodes.find(x => x.node === state.selectedNode)) {
        state.selectedNode = state.nodes.length ? state.nodes[0].node : null;
      }
    }
    
    applySummary(sim);
    renderLogs(sim);
    
    state.highlightedNodes.clear();
    if (state.lastMovedPodId) {
        for (const [nodeName, pods] of Object.entries(sim.pods_by_node || {})) {
            if (pods.find(p => p.pod_id === state.lastMovedPodId)) {
                state.highlightedNodes.add(nodeName);
                state.selectedNode = nodeName;
                break;
            }
        }
        state.lastMovedPodId = null; // Reset
    }

    renderNodes();
    renderPods();
    setMsg("Готово. Перетащите pod на ноду для настройки переноса.");
  } catch (err) {
    setMsg("Ошибка загрузки /simulate: " + (err?.message || err), true);
  } finally {
    setBusy(false);
  }
}
el("btnRefresh").addEventListener("click", loadSim);
el("btnReset").addEventListener("click", async () => {
  try {
    setBusy(true);
    setMsg("Сбрасываю к baseline…");
    await apiMutateOne({
      op: "reset_to_baseline"
    });
    await loadSim();
    setMsg("Сброшено к baseline.");
  } catch (err) {
    setMsg("Ошибка reset: " + (err?.message || err), true);
  } finally {
    setBusy(false);
  }
});
el("btnRefreshPrices").addEventListener("click", async () => {
  try {
    setBusy(true);
    setMsg("Обновляю прайсы из AWS…");
    await apiPost("/admin/refresh-prices", {});
    await loadSim();
    setMsg("Прайсы обновлены.");
  } catch (err) {
    setMsg("Ошибка refresh-prices: " + (err?.message || err), true);
  } finally {
    setBusy(false);
  }
});
el("selSnapshots").addEventListener("change", async (e) => {
  const id = e.target.value;
  setBusy(true);
  try {
    await apiPost(`/snapshots/${id}/activate`);
    await loadSim();
    setMsg(`Switched to snapshot: ${id}`);
  } catch (err) {
    setMsg("Error switching: " + err, true);
  } finally {
    setBusy(false);
  }
});
el("btnCapture").addEventListener("click", async () => {
  if (!confirm("Собрать текущее состояние кластера?")) return;
  setBusy(true);
  setMsg("Connecting to K8s & capturing state...");
  try {
    const res = await apiPost("/snapshots/capture");
    await loadSnapshotsList();
    el("selSnapshots").value = res.id;
    await apiPost(`/snapshots/${res.id}/activate`);
    await loadSim();
    setMsg("Snapshot captured successfully!");
  } catch (err) {
    setMsg("Capture failed: " + (err.message || err), true);
  } finally {
    setBusy(false);
  }
});

setupSortListeners();
setupPodSortListeners();

loadSnapshotsList().then(loadSim);