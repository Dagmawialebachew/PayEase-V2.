/**
 * PAYEASE PRO - 2030 ULTRA ENGINE
 * Integrated with Python Engine @ localhost:5000
 */

// --- CONFIGURATION & STATE ---
const Telegram = window.Telegram?.WebApp;
const API_BASE = "https://payease-v2.onrender.com/api";
// const API_BASE = "http://localhost:5000/api";
let currentWorkerDetailData = null;
// Local State
let state = {
    activeTab: 'dashboard',
    workers: [],
    pendingActions: new Map(), // Prevents double-taps
    idempotencyKey: () => crypto.randomUUID(),
    currentWorkerList: [],
    isActionPending: false, // Prevents double-taps
    undoTimeout: null,

};
let payoutChart = null
// --- CORE INITIALIZATION ---
document.addEventListener('DOMContentLoaded', async () => { // Added async
    if (window.Telegram?.WebApp) {
        const tg = window.Telegram.WebApp;
        tg.ready();
        tg.expand();
        tg.setHeaderColor('#050505');
        tg.enableClosingConfirmation();
    }

    initIcons();
    setupEventListeners();

    // 1. Fetch the data FIRST
    await refreshData(); 

    // 2. NOW restore the view once the data is in the 'state'
    const lastView = localStorage.getItem('payease_last_view') || 'dashboard';
    window.switchTab(lastView); 
});

function initIcons() {
    if (window.lucide) lucide.createIcons();
}

// --- DATA ENGINE ---
// --- SWR ENGINE ---

// Add this helper to manage the visual state
function showSyncing(active) {
    const syncInd = document.getElementById('sync-indicator');
    if (syncInd) {
        syncInd.style.opacity = active ? '1' : '0';
    }
}

async function refreshData() {
    showSyncing(true);
    try {
        const [statsRes, workersRes] = await Promise.all([
            fetch(`${API_BASE}/dashboard`),
            fetch(`${API_BASE}/workers`)
        ]);

        const statsData = await statsRes.json();
        const workersData = await workersRes.json();

        // 1. Save the data to the global state
        state.workers = workersData;
        localStorage.setItem('payease_dashboard_cache', JSON.stringify(statsData));
        
        // 2. Update the Dashboard
        updateDashboardStats(statsData, false); 
        
        // 3. IMPORTANT: Explicitly render the workers now that we have them
        renderWorkerCards(state.workers);

        if (window.Telegram?.WebApp?.HapticFeedback) {
            Telegram.WebApp.HapticFeedback.notificationOccurred('success');
        }
    } catch (err) {
        console.error("SWR Sync Error:", err);
    } finally {
        showSyncing(false);
    }
}


function updateDashboardStats(data, isStatic = false) {
    const duration = isStatic ? 0 : 800;

    // 1. The Big Counter (This part is already correct)
    animateValue("unpaid-payroll-counter", 0, data.total_unpaid || 0, duration);

    // 2. Map API keys to HTML data-stats
    // Inside app.js
    const selectors = {
        'total-workers': data.active_workers,
        'total-loans': data.total_outstanding_loans,
        'total-unpaid': data.total_unpaid,     // Use the new key
        'total-money-out': data.total_money_out, // Use the new key
        'total-clubs': data.total_clubs
    };

    Object.entries(selectors).forEach(([stat, value]) => {
        const el = document.querySelector(`[data-stat="${stat}"]`);
        if (el) {
            const numValue = parseFloat(value) || 0;
            if (isStatic) {
                el.innerText = numValue.toLocaleString();
            } else {
                // FIX: Pass 'el' as the first argument (target) and remove the 5th argument
                animateValue(el, 0, numValue, duration); 
            }
        }
    });

    // 3. Trigger Chart Update
    if (data.weekly_stats && data.weekly_stats.length > 0) {
        initPayoutChart(data.weekly_stats);
    }
}

// Ensure this is declared at the TOP of app.js (outside any function)

function initPayoutChart(weeklyData) {
    const ctx = document.getElementById('payoutChart');
    if (!ctx) return;

    if (payoutChart) payoutChart.destroy();

    // 1. Get unique days for the X-axis labels
    const labels = [...new Set(weeklyData.map(d => d.day))];
    
    // 2. Identify all unique clubs in the data
    const clubs = [...new Set(weeklyData.map(d => d.club))].filter(c => c);

    // 3. Define colors for different clubs
    const colors = {
        'Alpha': '#00d2ff', // Neon Blue
        'Beta': '#9d50bb',  // Neon Purple
        'Gamma': '#43e97b'  // Neon Green
    };

    // 4. Create a dataset for each club
    const datasets = clubs.map(clubName => {
        return {
            label: clubName,
            data: labels.map(day => {
                // Find the entry for this club on this specific day
                const entry = weeklyData.find(d => d.day === day && d.club === clubName);
                return entry ? entry.total : 0;
            }),
            borderColor: colors[clubName] || '#ffffff',
            borderWidth: 3,
            pointRadius: 2,
            tension: 0.4,
            fill: false
        };
    });

    // If no club data is available, show the "Total" as a fallback
    if (datasets.length === 0) {
        datasets.push({
            label: 'Total Spending',
            data: weeklyData.map(d => d.total),
            borderColor: '#00d2ff',
            borderWidth: 3,
            tension: 0.4,
            fill: true,
            backgroundColor: 'rgba(0, 210, 255, 0.1)'
        });
    }

    payoutChart = new Chart(ctx, {
        type: 'line',
        data: { labels, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { 
                legend: { 
                    display: true, 
                    position: 'top',
                    labels: { color: '#888', font: { size: 10, weight: 'bold' }, usePointStyle: true }
                } 
            },
            scales: {
                y: { 
                    display: true, 
                    grid: { color: 'rgba(255,255,255,0.05)' },
                    ticks: { color: '#444', font: { size: 9 } }
                },
                x: {
                    grid: { display: false },
                    ticks: { color: '#888', font: { size: 10, weight: 'bold' } }
                }
            }
        }
    });
}
function showToast(msg, type = "success") {
    // Cleanup
    document.querySelectorAll('.toast-msg').forEach(t => {
        t.style.opacity = '0';
        t.style.transform = 'translate(-50%, -20px)'; // Slide UP to hide
        setTimeout(() => t.remove(), 300);
    });

    const toast = document.createElement('div');
    // Positioned at TOP-8
    toast.className = `toast-msg fixed top-8 left-1/2 -translate-x-1/2 min-w-[200px] rounded-2xl glass border border-white/10 z-[600] flex items-center shadow-2xl transition-all duration-500 overflow-hidden pointer-events-none`;
    
    const accentColor = type === 'error' ? 'bg-red-500' : 'bg-premium-neonGreen';
    const iconColor = type === 'error' ? 'text-red-500' : 'text-premium-neonGreen';
    const iconName = type === 'error' ? 'alert-circle' : 'check-circle';

    toast.innerHTML = `
        <div class="absolute left-0 top-0 bottom-0 w-1 ${accentColor} shadow-[0_0_10px_rgba(0,0,0,0.3)]"></div>
        
        <div class="flex items-center gap-3 pl-4 pr-6 py-3">
            <i data-lucide="${iconName}" class="w-3.5 h-3.5 ${iconColor}"></i>
            <span class="text-[10px] font-black uppercase tracking-[0.15em] text-white/90">${msg}</span>
        </div>
    `;

    document.body.appendChild(toast);
    if (window.lucide) lucide.createIcons();

    // Slide DOWN animation
    requestAnimationFrame(() => {
        toast.style.transform = 'translate(-50%, 0)';
        toast.style.opacity = '1';
    });

    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translate(-50%, -10px)'; 
        setTimeout(() => toast.remove(), 400);
    }, 2500);
}
function timeAgo(dateString) {
    if (!dateString) return "Recently";
    
    const now = new Date();
    const past = new Date(dateString);
    const seconds = Math.floor((now - past) / 1000);

    // 1. Handle "Today" logic first for better UX
    if (seconds < 86400 && now.getDate() === past.getDate()) {
        return "Today";
    }

    // 2. Years (seconds in a year: 31,536,000)
    let interval = Math.floor(seconds / 31536000);
    if (interval >= 1) return interval + (interval === 1 ? " year ago" : " years ago");
    
    // 3. Months (seconds in 30 days: 2,592,000)
    interval = Math.floor(seconds / 2592000);
    if (interval >= 1) return interval + (interval === 1 ? " month ago" : " months ago");
    
    // 4. Days (seconds in a day: 86,400)
    interval = Math.floor(seconds / 86400);
    if (interval >= 1) return interval + (interval === 1 ? " day ago" : " days ago");
    
    // 5. Hours
    interval = Math.floor(seconds / 3600);
    if (interval >= 1) return interval + (interval === 1 ? " hour ago" : " hours ago");
    
    // 6. Minutes
    interval = Math.floor(seconds / 60);
    if (interval >= 1) return interval + (interval === 1 ? " minute ago" : " minutes ago");
    
    return seconds < 10 ? "Just now" : Math.floor(seconds) + " seconds ago";
}
// --- UI COMPONENTS ---
function renderWorkerCards(workers) {
    const container = document.getElementById('worker-list-container');
    if (!container) return;

    state.currentWorkerList = workers;

    if (workers.length === 0) {
        container.innerHTML = `
            <div class="flex flex-col items-center justify-center py-20 opacity-30">
                <i data-lucide="search-x" class="w-12 h-12 mb-2"></i>
                <p class="text-[10px] font-black uppercase tracking-widest">No worker found</p>
            </div>`;
        if (window.lucide) lucide.createIcons();
        return;
    }

    container.innerHTML = workers.map((worker, index) => {
        // Safe initials logic
        const initials = worker.full_name.trim().split(/\s+/).map(n => n[0]).join('').toUpperCase().slice(0, 2);
        const isActive = worker.is_active;
        
        const debtBadge = worker.active_loan > 0 
            ? `<span class="px-2 py-0.5 rounded-md bg-premium-neonPurple/10 text-[8px] font-black uppercase text-premium-neonPurple border border-premium-neonPurple/20 animate-pulse whitespace-nowrap">Loan: ${worker.active_loan.toLocaleString()} ETB</span>` 
            : '';

        const unpaidBadge = worker.unpaid_value > 0
            ? `<div class="flex flex-col items-end shrink-0">
                <span class="text-[10px] font-black text-premium-neonGreen tracking-tight whitespace-nowrap">${worker.unpaid_value.toLocaleString()} ETB</span>
                <span class="text-[7px] text-gray-500 uppercase font-bold tracking-widest">Unpaid</span>
               </div>`
            : '';
            
        const regDate = worker.registered_at; 
        const relativeTime = timeAgo(regDate);

        return `
            <div class="glass rounded-[2.2rem] p-5 mb-4 border-l-4 ${isActive ? 'border-premium-neonGreen' : 'border-red-500/50'} animate-fade-in relative overflow-hidden" 
                 style="animation-delay: ${index * 0.03}s; opacity: ${isActive ? '1' : '0.6'}">
                
                <div class="flex items-start justify-between mb-6 active:opacity-60 transition-all cursor-pointer" 
                     onclick="openWorkerDetail(${worker.id})">
                    
                    <div class="flex items-center gap-4 min-w-0"> 
                        <div class="relative shrink-0">
                            <div class="w-14 h-14 rounded-2xl bg-gradient-to-br from-white/10 to-transparent flex items-center justify-center text-lg font-black border border-white/5 text-white/80">
                                ${initials}
                            </div>
                            <div class="absolute -bottom-1 -right-1 w-3.5 h-3.5 rounded-full border-2 border-[#050505] status-dot ${isActive ? 'bg-premium-neonGreen' : 'bg-red-500'}"></div>
                        </div>
                        
                        <div class="min-w-0"> 
                            <h4 class="text-md font-bold tracking-tight text-white leading-tight break-words">
                                ${worker.full_name}
                            </h4>
                            <div class="flex flex-wrap items-center gap-2 mt-1">
                                <span class="px-2 py-0.5 rounded-md bg-white/5 text-[8px] font-black uppercase tracking-widest text-premium-neonBlue border border-white/5">
                                    ${worker.club || 'GENERAL'}
                                </span>
                                <span class="text-[9px] text-gray-500 font-bold tracking-tighter whitespace-nowrap">${worker.daily_rate.toLocaleString()} / Day</span>
                                ${debtBadge}
                            </div>
                        </div>
                    </div>

                    <div class="flex items-center gap-3 shrink-0 ml-2">
                        ${unpaidBadge}
                        <i data-lucide="chevron-right" class="w-4 h-4 text-white/20"></i>
                    </div>
                </div>

                <div class="flex items-center justify-between gap-3">
                    <div class="grid grid-cols-2 gap-3 flex-1">
                        <button data-action="loan" data-index="${index}"
                                class="glass py-3.5 rounded-2xl flex items-center justify-center gap-2 active:scale-95 transition-all text-gray-400">
                            <i data-lucide="hand-coins" class="w-4 h-4"></i>
                            <span class="text-[9px] font-black uppercase tracking-[0.15em]">Loan</span>
                        </button>
                        
                        <button data-action="pay" data-index="${index}"
                                class="bg-white py-3.5 rounded-2xl flex items-center justify-center gap-2 active:scale-95 transition-all text-black shadow-lg shadow-white/5">
                            <i data-lucide="wallet" class="w-4 h-4 stroke-[3]"></i>
                            <span class="text-[9px] font-black uppercase tracking-[0.15em]">Pay Now</span>
                        </button>
                    </div>

                    <label class="relative inline-flex items-center cursor-pointer scale-75 shrink-0">
                        <input type="checkbox" ${isActive ? 'checked' : ''}
                               data-worker-id="${worker.id}" 
                               class="sr-only peer status-toggle">
                        <div class="w-12 h-6 bg-white/5 rounded-full peer peer-checked:bg-premium-neonGreen/20 peer-checked:after:translate-x-6 after:content-[''] after:absolute after:top-[3px] after:left-[3px] after:bg-gray-600 peer-checked:after:bg-premium-neonGreen after:rounded-full after:h-[18px] after:w-[18px] after:transition-all"></div>
                    </label>
                </div>
                
                <div class="mt-4 pt-3 border-t border-white/5 flex items-center justify-between">
                    <div class="flex items-center gap-2">
                        <div class="w-1.5 h-1.5 rounded-full bg-premium-neonBlue/40"></div>
                        <p class="text-[8px] font-black text-gray-500 uppercase tracking-widest">
                            Started ${relativeTime}
                        </p>
                    </div>
                </div>
            </div> `;
    }).join('');

    attachWorkerListeners();
    if (window.lucide) lucide.createIcons();
}


let currentDetailWorkerId = null; // To track who we are looking at
let currentActiveWorker = null;
async function openWorkerDetail(workerId) {
    currentDetailWorkerId = workerId; // Store for the edit function
    // currentActiveWorker = worker; // Save the whole object
    const modal = document.getElementById('detail-modal');
    const content = modal.querySelector('.glass');

    try {
        modal.classList.remove('hidden');
        setTimeout(() => content.classList.remove('translate-y-full'), 10);

        const res = await fetch(`${API_BASE}/workers/${workerId}/detail`);
        const data = await res.json();
        currentWorkerDetailData = data

        // Populate Data
        document.getElementById('detail-name').innerText = data.full_name;
        document.getElementById('detail-club').innerText = data.club;
        document.getElementById('detail-rate').innerText = `${data.daily_rate.toLocaleString()} ETB`;
        document.getElementById('detail-active-loan').innerText = `${data.active_loan.toLocaleString()} ETB`;
        
        // Replace the history list logic in your openWorkerDetail function:
const list = document.getElementById('detail-history-list');
const recentItems = data.payouts.slice(0, 3); // Grab only the last 3

list.innerHTML = recentItems.length ? '' : '<p class="text-center py-6 text-gray-600 text-[10px] font-black">No Recent Activity</p>';

recentItems.forEach(p => {
    const date = new Date(p.created_at).toLocaleDateString('en-GB', { day: '2-digit', month: 'short' });
    list.innerHTML += `
        <div class="flex items-center justify-between rounded-xl bg-white/5 p-4 border border-white/5 mb-2">
            <div>
                <p class="text-[12px] font-black text-white">${p.net.toLocaleString()} ETB</p>
                <p class="text-[9px] font-bold text-gray-500 uppercase">${date} • Payout</p>
            </div>
            <i data-lucide="arrow-up-right" class="w-4 h-4 text-premium-neonGreen opacity-50"></i>
        </div>
    `;
});

// Add "See All" button if there are more than 3
if (data.payouts.length > 3 || data.loans?.length > 0) {
    list.innerHTML += `
        <button onclick="openFullHistory()" class="w-full py-3 mt-2 text-[10px] font-black uppercase tracking-widest text-premium-neonBlue bg-white/5 rounded-xl border border-white/5">
            View Full History
        </button>
    `;
}
        
        if (window.lucide) lucide.createIcons();
        if (window.Telegram?.WebApp?.HapticFeedback) Telegram.WebApp.HapticFeedback.impactOccurred('light');

    } catch (err) {
        showToast("Error loading details", "error");
        closeDetailModal();
    }
}


function filterWorkers() {
    const input = document.getElementById('worker-search-input');
    if (!input) return;

    const searchTerm = input.value.toLowerCase().trim();
    
    // If the box is empty, show the full list from our state
    if (searchTerm === "") {
        renderWorkerCards(state.workers);
        return;
    }
    
    // Filter the workers we saved in refreshData
    const filtered = state.workers.filter(worker => {
        const nameMatch = (worker.full_name || '').toLowerCase().includes(searchTerm);
        const clubMatch = (worker.club || '').toLowerCase().includes(searchTerm);
        return nameMatch || clubMatch;
    });

    // Render the results (or the "No staff found" message)
    renderWorkerCards(filtered);
}
// Logic for the Edit Button
function openEditWorker() {
    if (!currentDetailWorkerId) return;
    
    // 1. Close detail modal first
    closeDetailModal();
    
    // 2. Reuse your existing 'Add Worker' modal but fill it with current data
    // Or call a specific edit function:
    const worker = state.currentWorkerList.find(w => w.id === currentDetailWorkerId);
    if (worker) {
        triggerEditFlow(worker); // You'll need to define this to open your form
    }
}


async function confirmDeleteWorker() {
    const worker = currentWorkerDetailData; // Assumes this is set when modal opens
    if (!worker) return;

    // 1. Native Telegram Confirmation (Safe & Fast)
    if (window.Telegram?.WebApp?.showConfirm) {
        window.Telegram.WebApp.showConfirm(
            `Are you sure you want to delete ${worker.full_name}? This will remove all history and records.`,
            async (confirmed) => {
                if (confirmed) await executeDelete(worker.id);
            }
        );
    } else {
        // Browser Fallback
        if (confirm(`Delete ${worker.full_name}?`)) await executeDelete(worker.id);
    }
}

function resetDeleteButton() {
    const btn = document.getElementById('delete-worker-btn');
    if (btn) {
        btn.disabled = false;
        // Reset to the original Lucide trash icon
        btn.innerHTML = `<i data-lucide="trash-2" class="w-3.5 h-3.5"></i> Delete`;
        // Re-initialize icons so Lucide renders the trash can
        if (typeof lucide !== 'undefined') lucide.createIcons();
    }
}
async function executeDelete(workerId) {
    const btn = document.getElementById('delete-worker-btn');
    // Save original state
    const originalContent = btn.innerHTML;

    try {
        btn.disabled = true;
        btn.innerHTML = `<span class="animate-spin text-[10px]">⌛</span>`;

        const response = await fetch(`${API_BASE}/workers/${workerId}/delete`, {
            method: 'POST'
        });

        if (!response.ok) throw new Error("Delete Failed");

        showToast("Worker Deleted", "success");
        
        closeDetailModal();
        await refreshData();
        
        // CRITICAL: Reset the button for the NEXT time the modal opens
        resetDeleteButton();

    } catch (err) {
        console.error(err);
        showToast("Delete Failed", "error");
        // Reset if it fails so the user can try again
        resetDeleteButton();
    }
}

function closeDetailModal() {
    const modal = document.getElementById('detail-modal');
    const content = modal.querySelector('.glass');
    
    // Slide down first
    content.classList.add('translate-y-full');
    
    // Hide the whole thing after animation finishes
    setTimeout(() => {
        modal.classList.add('hidden');
        currentDetailWorkerId = null;
    }, 400);

    if (window.Telegram?.WebApp?.HapticFeedback) {
        Telegram.WebApp.HapticFeedback.impactOccurred('light');
    }
}

let activeEditingId = null;

function triggerEditFlow(worker) {
    activeEditingId = worker.id; // Mark that we are EDITING, not adding

    // 1. Update Modal Labels
    const setTitle = (id, text) => {
        const el = document.getElementById(id);
        if (el) el.innerText = text;
    };
    setTitle('modal-title', "Edit Profile");
    setTitle('modal-worker-subtitle', `Updating ${worker.full_name}`);
    setTitle('edit-submit-btn', "Save Changes");

    // 2. Fill Inputs Safely (Check both old and new ID patterns)
    const nameEl = document.getElementById('new-worker-name') || document.getElementById('edit-name');
    const rateEl = document.getElementById('new-worker-rate') || document.getElementById('edit-rate');
    const clubEl = document.getElementById('edit-club');
    const phoneEl = document.getElementById('edit-phone');

    if (nameEl) nameEl.value = worker.full_name || '';
    if (rateEl) rateEl.value = worker.daily_rate || '';
    if (clubEl) clubEl.value = worker.club || '';
    if (phoneEl) phoneEl.value = worker.phone || '';

    // 3. HIDE the Registration Date during Edit
    // We don't want to ruin calculations by changing the start date
    const dateContainer = document.querySelector('#new-worker-reg-date')?.parentElement;
    if (dateContainer) {
        dateContainer.classList.add('hidden'); 
    }

    // 4. UI Transitions
    const actionGrid = document.getElementById('modal-action-grid');
    const editForm = document.getElementById('modal-edit-form');
    const workerModal = document.getElementById('worker-modal');

    if (actionGrid) actionGrid.classList.add('hidden');
    if (editForm) editForm.classList.remove('hidden');
    if (workerModal) workerModal.classList.remove('hidden');

    // 5. Update Preview Card
    updateWorkerPreview();

    if (window.Telegram?.WebApp?.HapticFeedback) {
        Telegram.WebApp.HapticFeedback.impactOccurred('medium');
    }
}

// 1. Enter Loan Mode
function triggerLoanMode(worker) {
    // 1. CRITICAL: Set the global state so submitLoan knows who to pay
    currentActiveWorker = worker; 

    // 2. UI Transitions
    document.getElementById('modal-action-grid').classList.add('hidden');
    document.getElementById('modal-loan-form').classList.remove('hidden');
    
    // 3. Update Labels safely
    document.getElementById('modal-title').innerText = "Issue Loan";
    document.getElementById('modal-worker-subtitle').innerText = `Entering loan for ${worker.full_name}`;

    // 4. Show the modal (if it's not already open)
    document.getElementById('worker-modal').classList.remove('hidden');

    if (window.Telegram?.WebApp?.HapticFeedback) {
        Telegram.WebApp.HapticFeedback.impactOccurred('medium');
    }
}

// 2. Submit Loan to Backend
async function submitLoan() {
    const amountInput = document.getElementById('loan-amount-input');
    const amount = parseFloat(amountInput.value);
    const btn = document.getElementById('loan-submit-btn');

    if (!amount || amount <= 0) {
        return showToast("Enter a valid amount", "error");
    }

    try {
        btn.disabled = true;
        btn.innerText = "PROCESSING...";

        const response = await fetch(`${API_BASE}/loans`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                worker_id: currentActiveWorker.id,
                amount: amount
            })
        });

        if (!response.ok) throw new Error();

        showToast(`Loan of ${amount} ETB recorded`, "success");
        closeModal(); // Reset and close
        refreshData();

    } catch (err) {
        showToast("Loan failed", "error");
    } finally {
        btn.disabled = false;
        btn.innerText = "Confirm Loan";
    }
}

// 3. Reset Modal on Close
function closeModal() {
    document.getElementById('worker-modal').classList.add('hidden');
    // Reset views for next time
    document.getElementById('modal-action-grid').classList.remove('hidden');
    document.getElementById('modal-edit-form').classList.add('hidden');
    document.getElementById('modal-loan-form').classList.add('hidden');
    document.getElementById('loan-amount-input').value = '';
}

function processAction(type) {
    if (!currentActiveWorker) return;

    if (type === 'loan') {
        triggerLoan(currentActiveWorker.id, currentActiveWorker.full_name);
    } else if (type === 'payout') {
        // Your payout logic here
        triggerPayFlow(currentActiveWorker);
    }
}
async function submitEdit() {
    // 1. Grab values from the new Ultra-Engine IDs
    const name = document.getElementById('new-worker-name')?.value.trim() || 
                 document.getElementById('edit-name')?.value.trim();
    
    const club = document.getElementById('edit-club').value;
    
    const rateInput = document.getElementById('new-worker-rate') || 
                      document.getElementById('edit-rate');
    const rate = rateInput ? rateInput.value : "";
    
    const phone = document.getElementById('edit-phone')?.value || "";
    
    // NEW: Capture the registration date for retroactive backfilling
    const regDate = document.getElementById('new-worker-reg-date')?.value;

    if (!name || !club || !rate) {
        return showToast("Please fill all fields", "error");
    }

    const btn = document.getElementById('edit-submit-btn');
    const originalText = btn.innerText;

    try {
        btn.disabled = true;
        // 2030 Visual: Cinematic saving state
        btn.innerHTML = `<span class="flex items-center justify-center gap-2">
            <div class="w-3 h-3 border-2 border-white/20 border-t-white rounded-full animate-spin"></div>
            SYNCING...
        </span>`;

        const url = activeEditingId 
            ? `${API_BASE}/workers/${activeEditingId}/update` 
            : `${API_BASE}/workers`;

        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                full_name: name,
                club: club,
                daily_rate: parseFloat(rate),
                phone: phone,
                registered_at: regDate // Sent to backend for generate_series backfill
            })
        });

        if (!response.ok) throw new Error("API Error");

        // 2. Haptic Feedback for the "Pro" feel
        if (window.Telegram?.WebApp?.HapticFeedback) {
            Telegram.WebApp.HapticFeedback.notificationOccurred('success');
        }

        showToast(activeEditingId ? "Profile Updated" : "Worker Registered", "success");
        
        closeModal(); 
        await refreshData(); 

    } catch (err) {
        console.error(err);
        showToast("Failed to save worker", "error");
        if (window.Telegram?.WebApp?.HapticFeedback) {
            Telegram.WebApp.HapticFeedback.notificationOccurred('error');
        }
    } finally {
        btn.disabled = false;
        btn.innerText = originalText;
    }
}

// Reset everything when closing
function closeModal() {
    document.getElementById('worker-modal').classList.add('hidden');
    document.getElementById('modal-action-grid').classList.remove('hidden');
    document.getElementById('modal-edit-form').classList.add('hidden');
    activeEditingId = null;
}

function triggerAddFlow() {
    activeEditingId = null; 

    // 1. Safe UI Label Updates (Prevents the 'null' crash)
    const setTitle = (id, text) => {
        const el = document.getElementById(id);
        if (el) el.innerText = text;
    };

    setTitle('modal-title', "New Worker");
    setTitle('modal-worker-subtitle', "Register a new worker");
    setTitle('edit-submit-btn', "Create Worker");

    // 2. Clear All Potential Input IDs (Handling both old and new)
    const inputIds = [
        'new-worker-name', 'edit-name', 
        'new-worker-rate', 'edit-rate', 
        'edit-phone', 'new-worker-reg-date'
    ];
    
    inputIds.forEach(id => {
        const el = document.getElementById(id);
        if (el) {
            el.value = '';
            // Extra polish: quick border glow
            el.classList.add('border-premium-neonBlue/30');
            setTimeout(() => el.classList.remove('border-premium-neonBlue/30'), 1500);
        }
    });

    // 3. Set Defaults
    const clubSelect = document.getElementById('edit-club');
    if (clubSelect) clubSelect.selectedIndex = 0;

    const dateInput = document.getElementById('new-worker-reg-date');
    if (dateInput) {
        dateInput.value = new Date().toISOString().split('T')[0];
    }
    // Inside your existing triggerAddFlow function, add this line:
const dateContainer = document.querySelector('#new-worker-reg-date')?.parentElement;
if (dateContainer) {
    dateContainer.classList.remove('hidden'); // Show it only for NEW workers
}

    // 4. UI State Switching
    const actionGrid = document.getElementById('modal-action-grid');
    const editForm = document.getElementById('modal-edit-form');
    const workerModal = document.getElementById('worker-modal');

    if (actionGrid) actionGrid.classList.add('hidden');
    if (editForm) editForm.classList.remove('hidden');
    if (workerModal) workerModal.classList.remove('hidden');

    // 5. Update the Live Preview and Trigger Haptics
    updateWorkerPreview();

    if (window.Telegram?.WebApp?.HapticFeedback) {
        Telegram.WebApp.HapticFeedback.impactOccurred('heavy');
    }
}
function updateWorkerPreview() {
    const nameInput = document.getElementById('new-worker-name');
    const rateInput = document.getElementById('new-worker-rate');
    const dateInput = document.getElementById('new-worker-reg-date');
    const clubInput = document.getElementById('edit-club');

    const name = nameInput.value || "Full Name";
    const rate = parseFloat(rateInput.value) || 0;
    const regDateVal = dateInput.value;
    const club = clubInput.value;
    const calcContainer = document.getElementById('calc-breakdown');
    const calcText = document.getElementById('calc-text');

    // 1. Text Updates
    document.getElementById('prev-name').innerText = name;
    document.getElementById('prev-club').innerText = club;
    document.getElementById('prev-rate').innerText = `${rate} Br.`;

    // 2. Retroactive Math
    if (regDateVal && rate > 0) {
        const regDate = new Date(regDateVal);
        const today = new Date();
        
        regDate.setHours(0,0,0,0);
        today.setHours(0,0,0,0);

        const diffTime = today - regDate;
        // The "+ 1" ensures today is included in the pay
        const diffDays = Math.max(1, Math.floor(diffTime / (1000 * 60 * 60 * 24)) + 1);
        const totalUnpaid = diffDays * rate;

        // Update main balance
        document.getElementById('prev-unpaid').innerHTML = `${totalUnpaid.toLocaleString()} <span class="text-[10px] text-premium-neonBlue">Br.</span>`;

        // UPDATE THE TOOLTIP
        if (calcContainer && calcText) {
            calcContainer.classList.remove('hidden');
            // Logic explanation for your uncle:
            calcText.innerText = `${diffDays} days × ${rate} Br/day`;
            
            // If it's a long period, make the badge "pop"
            if (diffDays > 7) {
                calcContainer.classList.add('opacity-100', 'translate-x-0');
                calcText.classList.add('text-premium-neonPurple'); // Turn purple for "Big Debt"
            } else {
                calcText.classList.remove('text-premium-neonPurple');
            }
        }
    } else {
        if (calcContainer) calcContainer.classList.add('hidden');
    }
}

function attachWorkerListeners() {
    // 1. Button listeners - Using .onclick to overwrite old ones
    document.querySelectorAll('[data-action]').forEach(btn => {
        btn.onclick = (e) => {
            const action = btn.dataset.action;
            const index = parseInt(btn.dataset.index);
            const worker = state.currentWorkerList[index];
            if (worker) {
                if (action === 'pay') triggerPayFlow(worker);
                else if (action === 'loan') triggerLoanMode(worker);
            }
        };
    });

    // 2. Toggle listener - Using .onchange to overwrite old ones
    document.querySelectorAll('.status-toggle').forEach(toggle => {
        // OVERWRITE any existing listener to prevent doubling
        toggle.onchange = async function(e) {
            // Guard: prevent processing if already in flight
            if (this.dataset.statusSyncing === 'true') return;

            const workerId = this.dataset.workerId;
            const newStatus = this.checked; 
            const card = this.closest('.glass');
            const statusDot = card ? card.querySelector('.status-dot') : null;

            // Lock the toggle
            this.dataset.statusSyncing = 'true';
            
            console.log(`[SYNC START] Worker ${workerId}: ${newStatus}`);

            // 1. Immediate UI Feedback
            updateCardVisuals(card, statusDot, newStatus);
            showToast(newStatus ? "Worker Active" : "Worker Resting", "success");

            try {
                const res = await fetch(`${API_BASE}/workers/${workerId}/toggle`, { 
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' }
                });
                
                if (!res.ok) throw new Error("Server rejected toggle");
                const data = await res.json();
                
                // 2. Reconciliation: If server state differs (e.g., atomic flip failed), sync UI
                if (data.is_active !== undefined && data.is_active !== null) {
    if (data.is_active !== newStatus) {
        this.checked = data.is_active;
        updateCardVisuals(card, statusDot, data.is_active);
    }
}

                // Update the master state so search/filters don't reset it
                const masterWorker = state.workers.find(w => w.id == workerId);
                if (masterWorker) masterWorker.is_active = data.is_active;

            } catch (err) {
                console.error("[SYNC ERROR]", err);
                // Revert UI on failure
                this.checked = !newStatus;
                updateCardVisuals(card, statusDot, !newStatus);
                showToast("Connection Error", "error");
            } finally {
                // Unlock
                delete this.dataset.statusSyncing;
                console.log(`[SYNC END] Worker ${workerId}`);
            }
        };
    });
}

function updateCardVisuals(card, dot, isActive) {
    if (!card || !dot) return;

    const isWorking = !!isActive; 
    console.log('UI UPDATE -> IsWorking:', isWorking);

    // Apply the "Resting" vs "Active" visuals
    if (isWorking) {
        card.classList.remove('border-red-500/50');
        card.classList.add('border-premium-neonGreen');
        dot.classList.remove('bg-red-500');
        dot.classList.add('bg-premium-neonGreen');
        card.style.opacity = "1";
    } else {
        card.classList.remove('border-premium-neonGreen');
        card.classList.add('border-red-500/50');
        dot.classList.remove('bg-premium-neonGreen');
        dot.classList.add('bg-red-500');
        card.style.opacity = "0.6";
    }
}
// Helper to handle the payment trigger safely

// --- PAYFLOW LOGIC ---

let currentPayingWorker = null;

async function triggerPayFlow(worker) {
    if (state.isActionPending) return;
    currentPayingWorker = worker;

    try {
        const response = await fetch(`${API_BASE}/workers/${worker.id}/settlement-summary`);
        const data = await response.json();
        state.currentSettlement = data; 

        // 1. Update Labels
        document.getElementById('modal-days-chip').innerText = `${data.days_on || 0} Days`;
        document.getElementById('modal-gross').innerText = `${(data.gross_owed || 0).toLocaleString()} ETB`;
        
        // REMOVED MINUS SIGN: already_paid is now "Total Value Accounted For"
        document.getElementById('modal-already-paid').innerText = `${(data.already_paid || 0).toLocaleString()} ETB`;
        
        const payoutInput = document.getElementById('input-payout-amount');
        const loanInput = document.getElementById('input-loan-deduction');
        
        // 2. AUTO-FILL PAYOUT: Suggest the remaining Labor Value
        // remaining = (Total Work Done) - (Value already settled via partials/loans)
        const remainingValue = Math.max(0, (data.gross_owed || 0) - (data.already_paid || 0));
        payoutInput.value = remainingValue;

        // 3. AUTO-FILL LOAN: Guard against undefined or null
        const debt = parseFloat(data.total_debt || 0);
        if (debt > 0) {
            loanInput.value = debt;
            loanInput.disabled = false;
            loanInput.classList.remove('opacity-50');
        } else {
            loanInput.value = 0;
            loanInput.disabled = true;
            loanInput.classList.add('opacity-50');
        }

        document.getElementById('pay-modal').classList.remove('hidden');
        recalculateSettlement(); 

    } catch (err) {
        showToast("Error loading worker data", "error");
    }
}

function recalculateSettlement() {
    const data = state.currentSettlement;
    if (!data) return;

    const payoutInput = document.getElementById('input-payout-amount');
    const loanInput = document.getElementById('input-loan-deduction');
    const btn = document.getElementById('confirm-pay-btn');
    
    // 1. Capture Raw Values
    let inputGross = parseFloat(payoutInput.value) || 0;
    let inputLoan = parseFloat(loanInput.value) || 0;

    // 2. OVERPAYMENT GUARDRAIL (New)
    // We calculate the maximum labor value remaining for this worker
    const totalOwed = parseFloat(data.gross_owed || 0);
    const alreadyApplied = parseFloat(data.already_paid || 0);
    const maxPayoutAllowed = Math.max(0, totalOwed - alreadyApplied);

    let isOverpaying = false;
    if (inputGross > maxPayoutAllowed) {
        inputGross = maxPayoutAllowed; // Snap value to max
        payoutInput.value = inputGross;
        isOverpaying = true; // Flag to show a quick warning if you want
    }

    // 3. LOAN GUARDRAILS 
    const maxLoanAllowed = parseFloat(data.total_debt || 0);
    if (inputLoan > maxLoanAllowed) {
        inputLoan = maxLoanAllowed;
        loanInput.value = inputLoan;
    }
    
    // Safety check: Loan deduction cannot exceed the CURRENT payout either
    // (You can't deduct 500 ETB from a 200 ETB payout)
    if (inputLoan > inputGross) {
        inputLoan = inputGross;
        loanInput.value = inputLoan;
    }

    if (inputLoan < 0) {
        inputLoan = 0;
        loanInput.value = 0;
    }

    // 4. CALCULATE CASH 
    const finalCash = Math.max(0, inputGross - inputLoan);
    document.getElementById('modal-net').innerText = `${finalCash.toLocaleString()} ETB`;
    
    // 5. SETTLEMENT LOGIC (Value-Based)
    const totalValueCovered = alreadyApplied + inputGross;
    const isFinal = totalValueCovered >= (totalOwed - 0.01);
    
    // 6. UI UPDATES
    const card = document.getElementById('settlement-status-card');
    const dot = document.getElementById('status-dot');
    const title = document.getElementById('status-title');
    const desc = document.getElementById('status-desc');

    if (isFinal) {
        card.className = "mt-4 p-3 rounded-xl bg-green-500/10 border border-green-500/30 transition-colors";
        dot.className = "w-2 h-2 rounded-full bg-green-500";
        title.innerText = "Final Settlement";
        title.className = "text-[10px] font-black uppercase tracking-widest text-green-400";
        desc.innerText = "This will fully clear all attendance records.";
        btn.className = "w-full bg-green-500 py-4 rounded-2xl text-black font-black uppercase tracking-widest active:scale-95 transition-all shadow-[0_10px_20px_rgba(34,197,94,0.2)]";
        btn.disabled = false;
    } else {
        const remaining = totalOwed - totalValueCovered;
        card.className = "mt-4 p-3 rounded-xl bg-amber-500/10 border border-amber-500/30 transition-colors";
        dot.className = "w-2 h-2 rounded-full bg-amber-500 animate-pulse";
        title.innerText = "Partial Payment";
        title.className = "text-[10px] font-black uppercase tracking-widest text-amber-400";
        desc.innerText = `Attendance stays open. Remaining Value: ${Math.round(remaining).toLocaleString()} ETB.`;
        btn.className = "w-full bg-amber-500 py-4 rounded-2xl text-black font-black uppercase tracking-widest active:scale-95 transition-all shadow-[0_10px_20px_rgba(245,158,11,0.2)]";
        btn.disabled = false;
    }
}
async function confirmFinalPay(event) {
    if (!currentPayingWorker || state.isActionPending) return;

    const savedWorkerName = currentPayingWorker.full_name;
    const finalGross = parseFloat(document.getElementById('input-payout-amount').value) || 0;
    const finalLoanDed = parseFloat(document.getElementById('input-loan-deduction').value) || 0;
    const finalDays = parseInt(document.getElementById('modal-days-chip').innerText) || 0;

    const btn = event ? event.currentTarget : document.querySelector('#confirm-pay-btn');
    const originalText = btn.innerText;

    try {
        state.isActionPending = true;
        btn.disabled = true;
        btn.innerText = "PROCESSING...";

        const response = await fetch(`${API_BASE}/payouts/confirm`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                worker_id: currentPayingWorker.id,
                gross_amount: finalGross, // This is the total Labor Value
                loan_deduction: finalLoanDed,
                days: finalDays,
                idempotency_key: `pay_${currentPayingWorker.id}_${Date.now()}`
            })
        });

        const result = await response.json();
        if (!response.ok) throw new Error(result.error || "Server Error");

        closePayModal();

        // Use the server's calculated net_now for the toast
        // This avoids UI-side math discrepancies
        const actualCashPaid = result.net_now; 

        if (result.type === "final") {
            showToast(`Settlement Cleared: ${savedWorkerName}`, "success");
        } else {
            showToast(`Recorded ${actualCashPaid.toLocaleString()} ETB for ${savedWorkerName}`, "warning");
        }

        // Show Undo option - Pass the real payout ID and cash amount
        if (result.payout_id) {
            showUndoToast(result.payout_id, actualCashPaid, savedWorkerName);
        }

        refreshData();

    } catch (err) {
        console.error(err);
        showToast(err.message, "error");
    } finally {
        state.isActionPending = false;
        btn.disabled = false;
        btn.innerText = originalText;
    }
}

function showUndoToast(payoutId) {
    const toast = document.createElement('div');
    toast.className = "fixed top-12 left-1/2 -translate-x-1/2 glass px-6 py-4 rounded-2xl flex items-center gap-4 z-[300] border-t border-white/20 animate-slide-up shadow-2xl";
    toast.innerHTML = `
        <span class="text-[10px] font-black uppercase tracking-widest text-white">Payment Sent</span>
        <button id="undo-btn" class="text-[10px] font-black uppercase text-premium-neonBlue tracking-widest px-3 py-1 bg-white/5 rounded-lg">Undo</button>
    `;
    document.body.appendChild(toast);

    // Auto-remove after 10s
    state.undoTimeout = setTimeout(() => toast.remove(), 10000);

    document.getElementById('undo-btn').onclick = async () => {
        clearTimeout(state.undoTimeout);
        toast.innerHTML = `<span class="text-[10px] font-black uppercase animate-pulse">Reversing...</span>`;
        
        await fetch(`${API_BASE}/payouts/reverse/${payoutId}`, { method: 'POST' });
        toast.remove();
        refreshData();
        showToast("Payment Reversed", "success");
    };
}



function animateValue(target, start, end, duration) {
    // target can be either an element or an element ID string
    const obj = typeof target === "string" ? document.getElementById(target) : target;
    if (!obj) return;

    let startTimestamp = null;
    const step = (timestamp) => {
        if (!startTimestamp) startTimestamp = timestamp;
        const progress = Math.min((timestamp - startTimestamp) / duration, 1);
        const value = Math.floor(progress * (end - start) + start);
        obj.innerText = value.toLocaleString();
        if (progress < 1) {
            window.requestAnimationFrame(step);
        }
    };
    window.requestAnimationFrame(step);
}

function animateValueToElement(start, end, duration, el) {
    let startTs = null;
    const step = (ts) => {
        if (!startTs) startTs = ts;
        const progress = Math.min((ts - startTs) / duration, 1);
        const value = Math.floor(progress * (end - start) + start);
        el.innerText = value.toLocaleString();
        if (progress < 1) requestAnimationFrame(step);
    };
    requestAnimationFrame(step);
}


function closePayModal() {
    const modal = document.getElementById('pay-modal');
    modal.classList.add('hidden');
    currentPayingWorker = null;
}



// Ensure the modal close works
function closeModal() {
    document.getElementById('worker-modal').classList.add('hidden');
}


let currentCursor = null;
let isLoadingHistory = false;

async function fetchHistory(isLoadMore = false) {
    console.log("Fetching history with cursor:", currentCursor, "Load More:", isLoadMore);
    if (isLoadingHistory) return;
    isLoadingHistory = true;

    const container = document.getElementById('history-list-container');
    const filterType = document.getElementById('filter-type')?.value || '';
    const searchQuery = document.getElementById('history-search')?.value || '';

    if (!isLoadMore) {
        container.innerHTML = `<div class="p-10 text-center animate-pulse text-gray-500 text-[10px] tracking-widest uppercase">Syncing Ledger...</div>`;
        currentCursor = null;
    }

    try {
        // Build URL with parameters
        let url = `${API_BASE}/transactions?limit=20`;
        if (currentCursor) url += `&cursor=${currentCursor}`;
        if (filterType) url += `&type=${filterType}`;
        if (searchQuery) url += `&q=${encodeURIComponent(searchQuery)}`;

        const res = await fetch(url);
        const data = await res.json();
        console.log("Fetched Transactions:", data);

        const html = data.items.map(tx => `
            <div class="glass rounded-3xl p-5 mb-4 border-l-4 ${tx.type === 'payout' ? 'border-premium-neonGreen' : 'border-premium-neonPurple'} animate-fade-in">
                <div class="flex justify-between items-start">
                    <div class="flex flex-col gap-1">
                        <span class="text-xs font-black text-white uppercase tracking-tight">${tx.worker_name || 'System'}</span>
                        <span class="text-[9px] text-gray-500">${new Date(tx.created_at).toLocaleDateString()} • ${tx.club || 'General'}</span>
                    </div>
                    <div class="text-right">
                        <div class="text-sm font-black ${tx.type === 'payout' ? 'text-premium-neonGreen' : 'text-premium-neonPurple'}">
                            ${tx.type === 'payout' ? '-' : '+'}${tx.net_amount.toLocaleString()} ETB
                        </div>
                        <div class="text-[8px] font-bold text-gray-600 uppercase tracking-widest mt-1">${tx.type}</div>
                    </div>
                </div>
            </div>
        `).join('');

        if (isLoadMore) {
            container.innerHTML += html;
        } else {
            container.innerHTML = html || `<div class="p-10 text-center text-gray-600 text-xs uppercase font-bold">No Records Found</div>`;
        }

        currentCursor = data.next_cursor;
        
        // Show "Load More" button if there's a next cursor
        const loadMoreBtn = document.getElementById('load-more-container');
        if (currentCursor) {
            loadMoreBtn.classList.remove('hidden');
        } else {
            loadMoreBtn.classList.add('hidden');
        }

    } catch (err) {
        showToast("History Sync Failed", "error");
    } finally {
        isLoadingHistory = false;
    }
}

window.switchTab = (tabId) => {
    const targetTab = document.getElementById(tabId);
    if (!targetTab) return;

    // --- SAVE THE VIEW ---
    localStorage.setItem('payease_last_view', tabId);
    // ---------------------

    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    targetTab.classList.add('active');
    
    document.querySelectorAll('.nav-btn').forEach(btn => {
        btn.classList.remove('nav-active', 'text-premium-neonBlue');
        btn.classList.add('text-gray-500');
        
        if (btn.getAttribute('onclick')?.includes(tabId)) {
            btn.classList.add('nav-active', 'text-premium-neonBlue');
            btn.classList.remove('text-gray-500');
        }
    });

    if (Telegram?.HapticFeedback) Telegram.HapticFeedback.selectionChanged();
    
    if (tabId === 'dashboard') refreshData();
    if (tabId === 'history') fetchHistory(); 
    
    // This part ensures that if we are on 'staff', we force a render
    if (tabId === 'staff') {
        if (state.workers && state.workers.length > 0) {
            renderWorkerCards(state.workers);
        } else {
            // If data is still missing, trigger a refresh
            refreshData();
        }
    }
};

// --- HELPERS ---
function getInitials(name) {
    return name.split(' ').map(n => n[0]).join('').toUpperCase().substring(0, 2);
}// Main JS logic


let undoTimeout = null;

function showUndoToast(payoutId, amount, name) {
    // Remove existing toast if any
    const existing = document.getElementById('undo-toast');
    if (existing) existing.remove();

    const toast = document.createElement('div');
    toast.id = 'undo-toast';
    toast.className = `fixed top-8 left-4 right-4 p-4 glass rounded-2xl border border-white/10 z-[500] flex justify-between items-center animate-slide-up`;
    
    toast.innerHTML = `
        <div class="flex items-center gap-3">
            <div class="w-8 h-8 rounded-lg bg-premium-neonGreen/10 flex items-center justify-center">
                <i data-lucide="check" class="w-4 h-4 text-premium-neonGreen"></i>
            </div>
            <div>
                <p class="text-[10px] font-bold text-white uppercase">Paid ${name}</p>
                <p class="text-[9px] text-gray-500">${amount} ETB Processed</p>
            </div>
        </div>
        <button onclick="reversePayout(${payoutId})" class="text-[10px] font-black text-red-500 uppercase tracking-widest px-4 py-2 bg-red-500/10 rounded-lg">
            Undo
        </button>
    `;

    document.body.appendChild(toast);
    if (window.lucide) lucide.createIcons();

    // Auto-hide after 8 seconds
    undoTimeout = setTimeout(() => {
        toast.classList.add('animate-fade-out');
        setTimeout(() => toast.remove(), 500);
    }, 8000);
}

async function reversePayout(payoutId) {
    clearTimeout(undoTimeout);
    try {
            const res = await fetch(`${API_BASE}/payouts/reverse/${payoutId}`, { method: 'POST' });        if (res.ok) {
            document.getElementById('undo-toast').remove();
            showToast("Payment Reversed Successfully", "info");
            refreshData();
        }
    } catch (err) {
        showToast("Reversal Failed - Contact Dev", "error");
    }
}

// Add this to your System Utilities
window.showLifecycle = async (productId) => {
    if (Telegram?.HapticFeedback) Telegram.HapticFeedback.impactOccurred('light');
    
    // Simulate a neural fetch
    toast(`Fetching Product Matrix: ${productId}`, 'success');
    
    // You can use Telegram's Popup for a native feel
    if (Telegram) {
        Telegram.showPopup({
            title: 'NODE_INTEL',
            message: `Deep-diving into Product ${productId}. Full lifecycle analysis coming in next sync.`,
            buttons: [{id: 'ok', type: 'destructive', text: 'CLOSE_LINK'}]
        });
    }
};


function setupEventListeners() {
    const payModal = document.getElementById('pay-modal');
    payModal.addEventListener('click', (e) => {
        if (e.target === payModal) closePayModal();
    });

    // Simple Tab Switching
   
}


function initPayoutChart(weeklyData) {
    const ctx = document.getElementById('payoutChart');
    if (!ctx || !weeklyData) return;

    if (window.payoutChart instanceof Chart) {
        window.payoutChart.destroy();
    }

    // 1. Get unique days for X-axis (e.g., ["Mon", "Tue", "Sun"])
    const labels = [...new Set(weeklyData.map(d => d.day))];
    
    // 2. Identify unique clubs (using the new club_name key from Python)
    const clubs = [...new Set(weeklyData.map(d => d.club_name || d.club))].filter(c => c);

    const colors = {
        'Alpha': '#00d2ff', 
        'Beta': '#9d50bb',  
        'General': '#43e97b'
    };

    // 3. Create a dataset for each club
    const datasets = clubs.map(clubName => {
        return {
            label: clubName,
            data: labels.map(day => {
                // Sum all entries for this club on this day
                return weeklyData
                    .filter(d => d.day === day && (d.club_name === clubName || d.club === clubName))
                    .reduce((sum, item) => sum + parseFloat(item.daily_total || item.total || 0), 0);
            }),
            borderColor: colors[clubName] || '#ffffff',
            backgroundColor: (colors[clubName] || '#ffffff') + '15', // 15 is 8% opacity
            borderWidth: 2,
            pointRadius: 3,
            tension: 0.4,
            fill: true
        };
    });

    // Fallback if data is messy
    if (datasets.length === 0 && weeklyData.length > 0) {
        datasets.push({
            label: 'Total Payouts',
            data: labels.map(day => {
                return weeklyData
                    .filter(d => d.day === day)
                    .reduce((sum, item) => sum + parseFloat(item.total || item.daily_total || 0), 0);
            }),
            borderColor: '#00d2ff',
            fill: true,
            backgroundColor: 'rgba(0, 210, 255, 0.1)',
            tension: 0.4
        });
    }

    window.payoutChart = new Chart(ctx, {
        type: 'line',
        data: { labels, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: datasets.length > 1,
                    position: 'top',
                    labels: { color: '#888', font: { size: 10 }, usePointStyle: true }
                },
                tooltip: {
                    backgroundColor: '#111',
                    titleColor: '#fff',
                    bodyColor: '#aaa',
                    borderColor: '#333',
                    borderWidth: 1,
                    padding: 10,
                    displayColors: true
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: { color: 'rgba(255,255,255,0.03)' },
                    ticks: { color: '#666', font: { size: 9 }, callback: (v) => v.toLocaleString() }
                },
                x: {
                    grid: { display: false },
                    ticks: { color: '#888', font: { size: 10, weight: 'bold' } }
                }
            }
        }
    });
}


let touchStart = 0;
const appContainer = document.getElementById('app');

appContainer.addEventListener('touchstart', (e) => {
    touchStart = e.touches[0].pageY;
}, {passive: true});

appContainer.addEventListener('touchend', (e) => {
    const touchEnd = e.changedTouches[0].pageY;
    // If pulled down more than 100px at the top of the scroll
    if (appContainer.scrollTop === 0 && touchEnd - touchStart > 100) {
        refreshData();
    }
}, {passive: true});


let activeHistoryTab = 'payouts';

function openFullHistory() {
    document.getElementById('history-overlay').classList.remove('hidden');
    
    // LOOK HERE: Get the saved tab right when we open the overlay
    const savedTab = localStorage.getItem('payease_active_history_tab') || 'payouts';
    switchTabs(savedTab); 

    if (window.Telegram?.WebApp?.HapticFeedback) {
        Telegram.WebApp.HapticFeedback.impactOccurred('medium');
    }
}
function switchTabs(tab) {
    activeHistoryTab = tab;
    localStorage.setItem('payease_active_history_tab', tab);

    const payoutsBtn = document.getElementById('tab-payouts');
    const loansBtn = document.getElementById('tab-loans');
    const listContainer = document.getElementById('history-content-list');

    if (!listContainer) return;

    // UI Toggle logic
    if (tab === 'payouts') {
        payoutsBtn.className = "flex-1 py-3 rounded-xl text-[10px] font-black uppercase bg-premium-neonBlue text-black transition-all";
        loansBtn.className = "flex-1 py-3 rounded-xl text-[10px] font-black uppercase text-gray-500 transition-all";
        
        // ONLY render if we have a worker selected
        if (currentWorkerDetailData) {
            renderPayouts(listContainer);
        } else {
            listContainer.innerHTML = `<p class="text-[10px] text-gray-500 text-center py-4 uppercase font-black opacity-20">Select a worker to view history</p>`;
        }
    } else {
        loansBtn.className = "flex-1 py-3 rounded-xl text-[10px] font-black uppercase bg-premium-neonPurple text-black transition-all";
        payoutsBtn.className = "flex-1 py-3 rounded-xl text-[10px] font-black uppercase text-gray-500 transition-all";
        
        if (currentWorkerDetailData) {
            renderLoans(listContainer);
        } else {
            listContainer.innerHTML = `<p class="text-[10px] text-gray-500 text-center py-4 uppercase font-black opacity-20">Select a worker to view history</p>`;
        }
    }
    
    if (window.lucide) lucide.createIcons();
}
function closeFullHistory() {
    document.getElementById('history-overlay').classList.add('hidden');
}


function renderPayouts(container) {
    // Assuming 'currentWorkerDetailData' holds the full detail response
    if (!container) return;
    
    // GUARD: If state or payouts is missing, show a placeholder instead of crashing
    if (!state || !state.payouts) {
        container.innerHTML = `<p class="text-[10px] text-gray-500 text-center py-4">Loading history...</p>`;
        return;
    }
    const payouts = currentWorkerDetailData.payouts || [];
    
    if (payouts.length === 0) {
        container.innerHTML = `<div class="text-center py-20 opacity-30 text-[10px] font-black uppercase tracking-widest">No Payout Records</div>`;
        return;
    }

    container.innerHTML = payouts.map(p => {
        const date = new Date(p.created_at).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' });
        return `
            <div class="glass-card p-5 rounded-[2rem] border border-white/5 bg-white/[0.02] flex items-center justify-between">
                <div>
                    <p class="text-[9px] font-black text-premium-neonGreen uppercase tracking-widest mb-1">Salary Payment</p>
                    <h4 class="text-xl font-black text-white">${parseFloat(p.net).toLocaleString()} <span class="text-[10px] text-gray-500">ETB</span></h4>
                    <p class="text-[10px] font-bold text-gray-500 mt-1">${date} • ${p.days} Days Worked</p>
                </div>
                <div class="w-12 h-12 rounded-2xl bg-premium-neonGreen/10 flex items-center justify-center">
                    <i data-lucide="check-circle-2" class="w-6 h-6 text-premium-neonGreen"></i>
                </div>
            </div>
        `;
    }).join('');
}

function renderLoans(container) {
    const loans = currentWorkerDetailData.loans || [];

    if (loans.length === 0) {
        container.innerHTML = `<div class="text-center py-20 opacity-30 text-[10px] font-black uppercase tracking-widest">No Loan Records</div>`;
        return;
    }

    container.innerHTML = loans.map(l => {
        const date = new Date(l.created_at).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' });
        const isDeducted = l.status === 'deducted';
        
        return `
            <div class="glass-card p-5 rounded-[2rem] border border-white/5 bg-white/[0.02] flex items-center justify-between">
                <div>
                    <p class="text-[9px] font-black ${isDeducted ? 'text-gray-500' : 'text-premium-neonPurple'} uppercase tracking-widest mb-1">
                        ${isDeducted ? 'Loan Settled' : 'Active Debt'}
                    </p>
                    <h4 class="text-xl font-black ${isDeducted ? 'text-gray-500 line-through' : 'text-white'}">
                        ${parseFloat(l.amount).toLocaleString()} <span class="text-[10px]">ETB</span>
                    </h4>
                    <p class="text-[10px] font-bold text-gray-500 mt-1">${date}</p>
                </div>
                <div class="w-12 h-12 rounded-2xl ${isDeducted ? 'bg-white/5' : 'bg-premium-neonPurple/10'} flex items-center justify-center">
                    <i data-lucide="${isDeducted ? 'history' : 'hand-coins'}" class="w-6 h-6 ${isDeducted ? 'text-gray-500' : 'text-premium-neonPurple'}"></i>
                </div>
            </div>
        `;
    }).join('');
}


// Add to your state object in app.js
state.report = {
    mode: 'audit',
    club: 'all',
    range: 30,
    workerId: null
};
function setReportMode(mode) {
    if (!state.report) state.report = { mode: 'audit', range: 30 };
    state.report.mode = mode;
    
    // 1. Update Button Visuals
    document.querySelectorAll('.report-mode-btn').forEach(btn => {
        const icon = btn.querySelector('i');
        const span = btn.querySelector('span');
        // Check if this button's onclick matches the new mode
        const isCurrent = btn.getAttribute('onclick').includes(`'${mode}'`);

        if (isCurrent) {
            btn.classList.add('border-premium-neonBlue', 'bg-white/5');
            btn.classList.remove('border-white/10');
            if (icon) icon.className = `w-5 h-5 text-premium-neonBlue`; 
            if (span) span.classList.add('text-white');
        } else {
            btn.classList.remove('border-premium-neonBlue', 'bg-white/5');
            btn.classList.add('border-white/10');
            if (icon) icon.className = `w-5 h-5 text-gray-500`;
            if (span) span.classList.remove('text-white');
        }
    });

    // 2. Contextual UI: Hide "Target Club" if looking at Debt Ledger
    const clubSection = document.getElementById('filter-club-section');
    if (clubSection) {
        clubSection.style.display = (mode === 'debt') ? 'none' : 'block';
    }

    // 3. Haptic Feedback
    if (window.Telegram?.WebApp?.HapticFeedback) {
        window.Telegram.WebApp.HapticFeedback.impactOccurred('medium');
    }
    
    // Refresh icons to ensure colors update correctly
    if (window.lucide) lucide.createIcons();
}

function setReportClub(club, el) {
    state.report.club = club;
    
    // Visual toggle for club buttons
    document.querySelectorAll('.club-opt-btn').forEach(btn => {
        btn.classList.remove('border-premium-neonBlue', 'text-premium-neonBlue');
        btn.classList.add('border-white/5', 'text-gray-500');
    });
    el.classList.add('border-premium-neonBlue', 'text-premium-neonBlue');
    el.classList.remove('border-white/5', 'text-gray-500');
}

function updateReportRange(val) {
    state.report.range = val;
    document.getElementById('range-label').innerText = `Last ${val} Days`;
}


async function generateReport() {
    showSyncing(true); // Reuse your existing sync indicator
    
    try {
        const tgData = window.Telegram?.WebApp?.initDataUnsafe;
        const userId = tgData?.user?.id || 1131741322;
        
    if (!userId) {
        console.error("Critical: Could not find Telegram User ID");
        return;
    }

    const reportData = {
        telegram_id: userId, // Ensure this key matches what the backend expects
        mode: state.report?.mode || 'audit',
        range: 30
    };

    const response = await fetch(`${API_BASE}/reports/generate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(reportData)
    });

        if (response.ok) {
            showToast("Report Sent to Telegram", "success");
            if (Telegram?.HapticFeedback) Telegram.HapticFeedback.notificationOccurred('success');
        } else {
            throw new Error();
        }
    } catch (err) {
        showToast("Generation Failed", "error");
    } finally {
        showSyncing(false);
    }
}