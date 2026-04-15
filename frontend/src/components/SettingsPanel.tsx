"use client";

import { API_BASE } from "@/lib/api";
import React, { useState, useEffect, useCallback } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Settings, ExternalLink, Key, Shield, X, Save, ChevronDown, ChevronUp, Rss, Plus, Trash2, RotateCcw, Database, RefreshCw, CheckCircle, AlertCircle, Clock } from "lucide-react";

interface ApiEntry {
    id: string;
    name: string;
    description: string;
    category: string;
    url: string | null;
    required: boolean;
    has_key: boolean;
    env_key: string | null;
    value_obfuscated: string | null;
    is_set: boolean;
}

interface FeedEntry {
    name: string;
    url: string;
    weight: number;
}

const WEIGHT_LABELS: Record<number, string> = { 1: "LOW", 2: "MED", 3: "STD", 4: "HIGH", 5: "CRIT" };
const WEIGHT_COLORS: Record<number, string> = {
    1: "text-gray-400 border-gray-600",
    2: "text-blue-400 border-blue-600",
    3: "text-cyan-400 border-cyan-600",
    4: "text-orange-400 border-orange-600",
    5: "text-red-400 border-red-600",
};
const MAX_FEEDS = 20;

// Category colors for the tactical UI
const CATEGORY_COLORS: Record<string, string> = {
    Aviation: "text-cyan-400 border-cyan-500/30 bg-cyan-950/20",
    Maritime: "text-blue-400 border-blue-500/30 bg-blue-950/20",
    Geophysical: "text-orange-400 border-orange-500/30 bg-orange-950/20",
    Space: "text-purple-400 border-purple-500/30 bg-purple-950/20",
    Intelligence: "text-red-400 border-red-500/30 bg-red-950/20",
    Geolocation: "text-green-400 border-green-500/30 bg-green-950/20",
    Weather: "text-yellow-400 border-yellow-500/30 bg-yellow-950/20",
    Markets: "text-emerald-400 border-emerald-500/30 bg-emerald-950/20",
    SIGINT: "text-rose-400 border-rose-500/30 bg-rose-950/20",
};

type Tab = "api-keys" | "news-feeds" | "data-sync";

interface SyncScript {
    id: string;
    label: string;
    description: string;
    layer: number;
    fields: string[];
    depends_on: string[];
    status: "idle" | "running" | "success" | "error";
    last_run_iso: string | null;
    duration_s: number | null;
    coverage: Record<string, number>;
    log_tail: string;
    error: string | null;
}

const SettingsPanel = React.memo(function SettingsPanel({ isOpen, onClose }: { isOpen: boolean; onClose: () => void }) {
    const [activeTab, setActiveTab] = useState<Tab>("api-keys");

    // --- Admin Key (for protected endpoints) ---
    const [adminKey, setAdminKey] = useState(() => {
        if (typeof window !== 'undefined') return localStorage.getItem('sb_admin_key') || '';
        return '';
    });
    const adminHeaders = (extra?: Record<string, string>): Record<string, string> => {
        const h: Record<string, string> = { ...extra };
        if (adminKey) h['X-Admin-Key'] = adminKey;
        return h;
    };

    // --- API Keys state ---
    const [apis, setApis] = useState<ApiEntry[]>([]);
    const [editingId, setEditingId] = useState<string | null>(null);
    const [editValue, setEditValue] = useState("");
    const [saving, setSaving] = useState(false);
    const [expandedCategories, setExpandedCategories] = useState<Set<string>>(new Set(["Aviation", "Maritime"]));

    // --- News Feeds state ---
    const [feeds, setFeeds] = useState<FeedEntry[]>([]);
    const [feedsDirty, setFeedsDirty] = useState(false);
    const [feedSaving, setFeedSaving] = useState(false);
    const [feedMsg, setFeedMsg] = useState<{ type: "ok" | "err"; text: string } | null>(null);

    const fetchKeys = useCallback(async () => {
        try {
            const res = await fetch(`${API_BASE}/api/settings/api-keys`, {
                headers: adminHeaders(),
            });
            if (res.ok) setApis(await res.json());
        } catch (e) {
            console.error("Failed to fetch API keys", e);
        }
    }, []);

    // --- Data Sync state ---
    const [syncScripts, setSyncScripts] = useState<SyncScript[]>([]);
    const [syncRunning, setSyncRunning] = useState<Set<string>>(new Set());

    const fetchSyncStatus = useCallback(async () => {
        try {
            const res = await fetch(`${API_BASE}/api/data-sync/status`);
            if (res.ok) {
                const data: SyncScript[] = await res.json();
                setSyncScripts(data);
                setSyncRunning(new Set(data.filter(s => s.status === "running").map(s => s.id)));
            }
        } catch (e) { console.error("Failed to fetch sync status", e); }
    }, []);

    const triggerSync = async (scriptId: string) => {
        setSyncRunning(prev => new Set([...prev, scriptId]));
        try {
            await fetch(`${API_BASE}/api/data-sync/run/${scriptId}`, {
                method: "POST",
                headers: adminHeaders(),
            });
        } catch (e) { console.error("Failed to trigger sync", e); }
    };

    // Poll while any script is running
    useEffect(() => {
        if (!isOpen || activeTab !== "data-sync") return;
        fetchSyncStatus();
        if (syncRunning.size === 0) return;
        const interval = setInterval(fetchSyncStatus, 3000);
        return () => clearInterval(interval);
    }, [isOpen, activeTab, syncRunning.size, fetchSyncStatus]);

    useEffect(() => {
        if (isOpen && activeTab === "data-sync") fetchSyncStatus();
    }, [isOpen, activeTab, fetchSyncStatus]);

    const fetchFeeds = useCallback(async () => {
        try {
            const res = await fetch(`${API_BASE}/api/settings/news-feeds`);
            if (res.ok) {
                setFeeds(await res.json());
                setFeedsDirty(false);
            }
        } catch (e) {
            console.error("Failed to fetch news feeds", e);
        }
    }, []);

    useEffect(() => {
        if (isOpen) {
            fetchKeys();
            fetchFeeds();
        }
    }, [isOpen, fetchKeys, fetchFeeds]);

    // API Keys handlers
    const startEditing = (api: ApiEntry) => { setEditingId(api.id); setEditValue(""); };

    const saveKey = async (api: ApiEntry) => {
        if (!api.env_key) return;
        setSaving(true);
        try {
            const res = await fetch(`${API_BASE}/api/settings/api-keys`, {
                method: "PUT",
                headers: adminHeaders({ "Content-Type": "application/json" }),
                body: JSON.stringify({ env_key: api.env_key, value: editValue }),
            });
            if (res.ok) { setEditingId(null); fetchKeys(); }
        } catch (e) {
            console.error("Failed to save API key", e);
        } finally { setSaving(false); }
    };

    const toggleCategory = (cat: string) => {
        setExpandedCategories(prev => {
            const next = new Set(prev);
            if (next.has(cat)) next.delete(cat); else next.add(cat);
            return next;
        });
    };

    const grouped = apis.reduce<Record<string, ApiEntry[]>>((acc, api) => {
        if (!acc[api.category]) acc[api.category] = [];
        acc[api.category].push(api);
        return acc;
    }, {});

    // News Feeds handlers
    const updateFeed = (idx: number, field: keyof FeedEntry, value: string | number) => {
        setFeeds(prev => prev.map((f, i) => i === idx ? { ...f, [field]: value } : f));
        setFeedsDirty(true);
        setFeedMsg(null);
    };

    const removeFeed = (idx: number) => {
        setFeeds(prev => prev.filter((_, i) => i !== idx));
        setFeedsDirty(true);
        setFeedMsg(null);
    };

    const addFeed = () => {
        if (feeds.length >= MAX_FEEDS) return;
        setFeeds(prev => [...prev, { name: "", url: "", weight: 3 }]);
        setFeedsDirty(true);
        setFeedMsg(null);
    };

    const saveFeeds = async () => {
        setFeedSaving(true);
        setFeedMsg(null);
        try {
            const res = await fetch(`${API_BASE}/api/settings/news-feeds`, {
                method: "PUT",
                headers: adminHeaders({ "Content-Type": "application/json" }),
                body: JSON.stringify(feeds),
            });
            if (res.ok) {
                setFeedsDirty(false);
                setFeedMsg({ type: "ok", text: "Feeds saved. Changes take effect on next news refresh (~30min) or manual /api/refresh." });
            } else {
                const d = await res.json().catch(() => ({}));
                setFeedMsg({ type: "err", text: d.message || "Save failed" });
            }
        } catch (e) {
            setFeedMsg({ type: "err", text: "Network error" });
        } finally { setFeedSaving(false); }
    };

    const resetFeeds = async () => {
        try {
            const res = await fetch(`${API_BASE}/api/settings/news-feeds/reset`, {
                method: "POST",
                headers: adminHeaders(),
            });
            if (res.ok) {
                const d = await res.json();
                setFeeds(d.feeds || []);
                setFeedsDirty(false);
                setFeedMsg({ type: "ok", text: "Reset to defaults" });
            }
        } catch (e) {
            setFeedMsg({ type: "err", text: "Reset failed" });
        }
    };

    return (
        <AnimatePresence>
            {isOpen && (
                <>
                    {/* Backdrop */}
                    <motion.div
                        initial={{ opacity: 0 }}
                        animate={{ opacity: 1 }}
                        exit={{ opacity: 0 }}
                        className="fixed inset-0 bg-black/70 backdrop-blur-sm z-[9998]"
                        onClick={onClose}
                    />

                    {/* Settings Panel */}
                    <motion.div
                        initial={{ opacity: 0, x: -300 }}
                        animate={{ opacity: 1, x: 0 }}
                        exit={{ opacity: 0, x: -300 }}
                        transition={{ type: "spring", damping: 25, stiffness: 300 }}
                        className="fixed left-0 top-0 bottom-0 w-[480px] bg-[var(--bg-secondary)]/95 backdrop-blur-xl border-r border-cyan-900/50 z-[9999] flex flex-col shadow-[4px_0_40px_rgba(0,0,0,0.3)]"
                    >
                        {/* Header */}
                        <div className="flex items-center justify-between p-6 border-b border-[var(--border-primary)]/80">
                            <div className="flex items-center gap-3">
                                <div className="w-8 h-8 rounded-lg bg-cyan-500/10 border border-cyan-500/30 flex items-center justify-center">
                                    <Settings size={16} className="text-cyan-400" />
                                </div>
                                <div>
                                    <h2 className="text-sm font-bold tracking-[0.2em] text-[var(--text-primary)] font-mono">SYSTEM CONFIG</h2>
                                    <span className="text-[9px] text-[var(--text-muted)] font-mono tracking-widest">SETTINGS &amp; DATA SOURCES</span>
                                </div>
                            </div>
                            <button
                                onClick={onClose}
                                className="w-8 h-8 rounded-lg border border-[var(--border-primary)] hover:border-red-500/50 flex items-center justify-center text-[var(--text-muted)] hover:text-red-400 transition-all hover:bg-red-950/20"
                            >
                                <X size={14} />
                            </button>
                        </div>

                        {/* Admin Key Bar */}
                        <div className="flex items-center gap-2 px-4 py-2.5 border-b border-[var(--border-primary)]/40 bg-[var(--bg-primary)]/30">
                            <Shield size={12} className={adminKey ? "text-green-400" : "text-yellow-500"} />
                            <span className="text-[9px] font-mono tracking-widest text-[var(--text-muted)] whitespace-nowrap">ADMIN KEY</span>
                            <input
                                type="password"
                                value={adminKey}
                                onChange={(e) => {
                                    setAdminKey(e.target.value);
                                    localStorage.setItem('sb_admin_key', e.target.value);
                                }}
                                placeholder="Enter admin key for protected operations..."
                                className="flex-1 bg-[var(--bg-primary)]/60 border border-[var(--border-primary)] rounded px-2 py-1 text-[10px] font-mono text-[var(--text-secondary)] outline-none focus:border-cyan-700 placeholder:text-[var(--text-muted)]/50"
                            />
                            {adminKey && <span className="text-[8px] font-mono text-green-400/70 tracking-widest">SET</span>}
                        </div>

                        <div className="flex border-b border-[var(--border-primary)]/60">
                            <button
                                onClick={() => setActiveTab("api-keys")}
                                className={`flex-1 px-4 py-2.5 text-[10px] font-mono tracking-widest font-bold transition-colors flex items-center justify-center gap-1.5 ${activeTab === "api-keys" ? "text-cyan-400 border-b-2 border-cyan-500 bg-cyan-950/10" : "text-[var(--text-muted)] hover:text-[var(--text-secondary)]"}`}
                            >
                                <Key size={10} />
                                API KEYS
                            </button>
                            <button
                                onClick={() => setActiveTab("news-feeds")}
                                className={`flex-1 px-4 py-2.5 text-[10px] font-mono tracking-widest font-bold transition-colors flex items-center justify-center gap-1.5 ${activeTab === "news-feeds" ? "text-orange-400 border-b-2 border-orange-500 bg-orange-950/10" : "text-[var(--text-muted)] hover:text-[var(--text-secondary)]"}`}
                            >
                                <Rss size={10} />
                                NEWS FEEDS
                                {feedsDirty && <span className="w-1.5 h-1.5 rounded-full bg-orange-400 animate-pulse" />}
                            </button>
                            <button
                                onClick={() => setActiveTab("data-sync")}
                                className={`flex-1 px-4 py-2.5 text-[10px] font-mono tracking-widest font-bold transition-colors flex items-center justify-center gap-1.5 ${activeTab === "data-sync" ? "text-emerald-400 border-b-2 border-emerald-500 bg-emerald-950/10" : "text-[var(--text-muted)] hover:text-[var(--text-secondary)]"}`}
                            >
                                <Database size={10} />
                                DATA SYNC
                                {syncRunning.size > 0 && <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />}
                            </button>
                        </div>

                        {/* ==================== API KEYS TAB ==================== */}
                        {activeTab === "api-keys" && (
                            <>
                                {/* Info Banner */}
                                <div className="mx-4 mt-4 p-3 rounded-lg border border-cyan-900/30 bg-cyan-950/10">
                                    <div className="flex items-start gap-2">
                                        <Shield size={12} className="text-cyan-500 mt-0.5 flex-shrink-0" />
                                        <p className="text-[10px] text-[var(--text-secondary)] font-mono leading-relaxed">
                                            API keys are stored locally in the backend <span className="text-cyan-400">.env</span> file. Keys marked with <Key size={8} className="inline text-yellow-500" /> are required for full functionality. Public APIs need no key.
                                        </p>
                                    </div>
                                </div>

                                {/* API List */}
                                <div className="flex-1 overflow-y-auto styled-scrollbar p-4 space-y-3">
                                    {Object.entries(grouped).map(([category, categoryApis]) => {
                                        const colorClass = CATEGORY_COLORS[category] || "text-gray-400 border-gray-700 bg-gray-900/20";
                                        const isExpanded = expandedCategories.has(category);
                                        return (
                                            <div key={category} className="rounded-lg border border-[var(--border-primary)]/60 overflow-hidden">
                                                <button
                                                    onClick={() => toggleCategory(category)}
                                                    className="w-full flex items-center justify-between px-4 py-2.5 bg-[var(--bg-secondary)]/50 hover:bg-[var(--bg-secondary)]/80 transition-colors"
                                                >
                                                    <div className="flex items-center gap-2">
                                                        <span className={`text-[9px] font-mono tracking-widest font-bold px-2 py-0.5 rounded border ${colorClass}`}>
                                                            {category.toUpperCase()}
                                                        </span>
                                                        <span className="text-[10px] text-[var(--text-muted)] font-mono">
                                                            {categoryApis.length} {categoryApis.length === 1 ? 'service' : 'services'}
                                                        </span>
                                                    </div>
                                                    {isExpanded ? <ChevronUp size={12} className="text-[var(--text-muted)]" /> : <ChevronDown size={12} className="text-[var(--text-muted)]" />}
                                                </button>
                                                <AnimatePresence>
                                                    {isExpanded && (
                                                        <motion.div
                                                            initial={{ height: 0, opacity: 0 }}
                                                            animate={{ height: "auto", opacity: 1 }}
                                                            exit={{ height: 0, opacity: 0 }}
                                                            transition={{ duration: 0.2 }}
                                                        >
                                                            {categoryApis.map((api) => (
                                                                <div key={api.id} className="border-t border-[var(--border-primary)]/40 px-4 py-3 hover:bg-[var(--bg-secondary)]/30 transition-colors">
                                                                    <div className="flex items-center justify-between mb-1">
                                                                        <div className="flex items-center gap-2">
                                                                            {api.required && <Key size={10} className="text-yellow-500" />}
                                                                            <span className="text-xs font-mono text-[var(--text-primary)] font-medium">{api.name}</span>
                                                                        </div>
                                                                        <div className="flex items-center gap-1.5">
                                                                            {api.has_key ? (
                                                                                api.is_set ? (
                                                                                    <span className="text-[8px] font-mono px-1.5 py-0.5 rounded border border-green-500/30 text-green-400 bg-green-950/20">KEY SET</span>
                                                                                ) : (
                                                                                    <span className="text-[8px] font-mono px-1.5 py-0.5 rounded border border-yellow-500/30 text-yellow-400 bg-yellow-950/20">MISSING</span>
                                                                                )
                                                                            ) : (
                                                                                <span className="text-[8px] font-mono px-1.5 py-0.5 rounded border border-[var(--border-primary)] text-[var(--text-muted)]">PUBLIC</span>
                                                                            )}
                                                                            {api.url && (
                                                                                <a href={api.url} target="_blank" rel="noopener noreferrer" className="text-[var(--text-muted)] hover:text-cyan-400 transition-colors" onClick={(e) => e.stopPropagation()}>
                                                                                    <ExternalLink size={10} />
                                                                                </a>
                                                                            )}
                                                                        </div>
                                                                    </div>
                                                                    <p className="text-[10px] text-[var(--text-muted)] font-mono leading-relaxed mb-2">{api.description}</p>
                                                                    {api.has_key && (
                                                                        <div className="mt-2">
                                                                            {editingId === api.id ? (
                                                                                <div className="flex gap-2">
                                                                                    <input type="text" value={editValue} onChange={(e) => setEditValue(e.target.value)} className="flex-1 bg-black/60 border border-cyan-900/50 rounded px-2 py-1.5 text-[11px] font-mono text-cyan-300 outline-none focus:border-cyan-500/70 transition-colors" placeholder="Enter API key..." autoFocus />
                                                                                    <button onClick={() => saveKey(api)} disabled={saving} className="px-3 py-1.5 rounded bg-cyan-500/20 border border-cyan-500/40 text-cyan-400 hover:bg-cyan-500/30 transition-colors text-[10px] font-mono flex items-center gap-1">
                                                                                        <Save size={10} />{saving ? "..." : "SAVE"}
                                                                                    </button>
                                                                                    <button onClick={() => setEditingId(null)} className="px-2 py-1.5 rounded border border-[var(--border-primary)] text-[var(--text-muted)] hover:text-[var(--text-primary)] hover:border-[var(--border-secondary)] transition-colors text-[10px] font-mono">ESC</button>
                                                                                </div>
                                                                            ) : (
                                                                                <div className="flex items-center gap-1.5">
                                                                                    <div className="flex-1 bg-[var(--bg-primary)]/40 border border-[var(--border-primary)] rounded px-2.5 py-1.5 font-mono text-[11px] cursor-pointer hover:border-[var(--border-secondary)] transition-colors select-none" onClick={() => startEditing(api)}>
                                                                                        <span className="text-[var(--text-muted)] tracking-wider">{api.is_set ? api.value_obfuscated : "Click to set key..."}</span>
                                                                                    </div>
                                                                                </div>
                                                                            )}
                                                                        </div>
                                                                    )}
                                                                </div>
                                                            ))}
                                                        </motion.div>
                                                    )}
                                                </AnimatePresence>
                                            </div>
                                        );
                                    })}
                                </div>

                                {/* Footer */}
                                <div className="p-4 border-t border-[var(--border-primary)]/80">
                                    <div className="flex items-center justify-between text-[9px] text-[var(--text-muted)] font-mono">
                                        <span>{apis.length} REGISTERED APIs</span>
                                        <span>{apis.filter(a => a.has_key).length} KEYS CONFIGURED</span>
                                    </div>
                                </div>
                            </>
                        )}

                        {/* ==================== NEWS FEEDS TAB ==================== */}
                        {activeTab === "news-feeds" && (
                            <>
                                {/* Info Banner */}
                                <div className="mx-4 mt-4 p-3 rounded-lg border border-orange-900/30 bg-orange-950/10">
                                    <div className="flex items-start gap-2">
                                        <Rss size={12} className="text-orange-500 mt-0.5 flex-shrink-0" />
                                        <p className="text-[10px] text-[var(--text-secondary)] font-mono leading-relaxed">
                                            Configure RSS/Atom feeds for the Threat Intel news panel. Each feed is scored by keyword heuristics and weighted by the priority you set. Up to <span className="text-orange-400">{MAX_FEEDS}</span> sources.
                                        </p>
                                    </div>
                                </div>

                                {/* Feed List */}
                                <div className="flex-1 overflow-y-auto styled-scrollbar p-4 space-y-2">
                                    {feeds.map((feed, idx) => (
                                        <div key={idx} className="rounded-lg border border-[var(--border-primary)]/60 p-3 hover:border-[var(--border-secondary)]/60 transition-colors group">
                                            {/* Row 1: Name + Weight + Delete */}
                                            <div className="flex items-center gap-2 mb-2">
                                                <input
                                                    type="text"
                                                    value={feed.name}
                                                    onChange={(e) => updateFeed(idx, "name", e.target.value)}
                                                    className="flex-1 bg-transparent border-b border-[var(--border-primary)] text-xs font-mono text-[var(--text-primary)] outline-none focus:border-cyan-500/70 transition-colors px-1 py-0.5"
                                                    placeholder="Source name..."
                                                />
                                                {/* Weight selector */}
                                                <div className="flex items-center gap-1">
                                                    {[1, 2, 3, 4, 5].map(w => (
                                                        <button
                                                            key={w}
                                                            onClick={() => updateFeed(idx, "weight", w)}
                                                            className={`w-5 h-5 rounded text-[8px] font-mono font-bold border transition-all ${feed.weight === w ? WEIGHT_COLORS[w] + " bg-black/40" : "border-[var(--border-primary)]/40 text-[var(--text-muted)]/50 hover:border-[var(--border-secondary)]"}`}
                                                            title={WEIGHT_LABELS[w]}
                                                        >
                                                            {w}
                                                        </button>
                                                    ))}
                                                    <span className={`text-[8px] font-mono ml-1 w-7 ${WEIGHT_COLORS[feed.weight]?.split(" ")[0] || "text-gray-400"}`}>
                                                        {WEIGHT_LABELS[feed.weight] || "STD"}
                                                    </span>
                                                </div>
                                                <button
                                                    onClick={() => removeFeed(idx)}
                                                    className="w-6 h-6 rounded flex items-center justify-center text-[var(--text-muted)] hover:text-red-400 hover:bg-red-950/20 transition-all opacity-0 group-hover:opacity-100"
                                                    title="Remove feed"
                                                >
                                                    <Trash2 size={11} />
                                                </button>
                                            </div>
                                            {/* Row 2: URL */}
                                            <input
                                                type="text"
                                                value={feed.url}
                                                onChange={(e) => updateFeed(idx, "url", e.target.value)}
                                                className="w-full bg-black/30 border border-[var(--border-primary)]/40 rounded px-2 py-1 text-[10px] font-mono text-[var(--text-muted)] outline-none focus:border-cyan-500/50 focus:text-cyan-300 transition-colors"
                                                placeholder="https://example.com/rss.xml"
                                            />
                                        </div>
                                    ))}

                                    {/* Add Feed Button */}
                                    <button
                                        onClick={addFeed}
                                        disabled={feeds.length >= MAX_FEEDS}
                                        className="w-full py-2.5 rounded-lg border border-dashed border-[var(--border-primary)]/60 text-[var(--text-muted)] hover:border-orange-500/50 hover:text-orange-400 hover:bg-orange-950/10 transition-all text-[10px] font-mono flex items-center justify-center gap-1.5 disabled:opacity-30 disabled:cursor-not-allowed"
                                    >
                                        <Plus size={10} />
                                        ADD FEED ({feeds.length}/{MAX_FEEDS})
                                    </button>
                                </div>

                                {/* Status message */}
                                {feedMsg && (
                                    <div className={`mx-4 mb-2 px-3 py-2 rounded text-[10px] font-mono ${feedMsg.type === "ok" ? "text-green-400 bg-green-950/20 border border-green-900/30" : "text-red-400 bg-red-950/20 border border-red-900/30"}`}>
                                        {feedMsg.text}
                                    </div>
                                )}

                                {/* Footer */}
                                <div className="p-4 border-t border-[var(--border-primary)]/80">
                                    <div className="flex items-center gap-2">
                                        <button
                                            onClick={saveFeeds}
                                            disabled={!feedsDirty || feedSaving}
                                            className="flex-1 px-4 py-2 rounded bg-orange-500/20 border border-orange-500/40 text-orange-400 hover:bg-orange-500/30 transition-colors text-[10px] font-mono flex items-center justify-center gap-1.5 disabled:opacity-30 disabled:cursor-not-allowed"
                                        >
                                            <Save size={10} />
                                            {feedSaving ? "SAVING..." : "SAVE FEEDS"}
                                        </button>
                                        <button
                                            onClick={resetFeeds}
                                            className="px-3 py-2 rounded border border-[var(--border-primary)] text-[var(--text-muted)] hover:text-[var(--text-primary)] hover:border-[var(--border-secondary)] transition-all text-[10px] font-mono flex items-center gap-1.5"
                                            title="Reset to defaults"
                                        >
                                            <RotateCcw size={10} />
                                            RESET
                                        </button>
                                    </div>
                                    <div className="flex items-center justify-between text-[9px] text-[var(--text-muted)] font-mono mt-2">
                                        <span>{feeds.length}/{MAX_FEEDS} SOURCES</span>
                                        <span>WEIGHT: 1=LOW  5=CRITICAL</span>
                                    </div>
                                </div>
                            </>
                        )}
                        {/* ==================== DATA SYNC TAB ==================== */}
                        {activeTab === "data-sync" && (
                            <>
                                <div className="mx-4 mt-4 p-3 rounded-lg border border-emerald-900/30 bg-emerald-950/10">
                                    <div className="flex items-start gap-2">
                                        <Database size={12} className="text-emerald-500 mt-0.5 flex-shrink-0" />
                                        <p className="text-[10px] text-[var(--text-secondary)] font-mono leading-relaxed">
                                            Enrichment pipeline for <span className="text-emerald-400">datacenters_geocoded.json</span>. Each layer adds risk fields. Run them in order L1 → L5. Requires admin key.
                                        </p>
                                    </div>
                                </div>
                                <div className="flex-1 overflow-y-auto styled-scrollbar p-4 space-y-3">
                                    {syncScripts.map((script) => {
                                        const isRunning = script.status === "running" || syncRunning.has(script.id);
                                        const coverageEntries = Object.entries(script.coverage || {});
                                        const relTime = script.last_run_iso
                                            ? new Date(script.last_run_iso).toLocaleString()
                                            : null;
                                        return (
                                            <div key={script.id} className="rounded-lg border border-[var(--border-primary)]/60 p-3 hover:border-[var(--border-secondary)]/60 transition-colors">
                                                {/* Header row */}
                                                <div className="flex items-start justify-between gap-2 mb-2">
                                                    <div className="flex items-center gap-2 min-w-0">
                                                        <span className="text-[9px] font-mono px-1.5 py-0.5 rounded border border-emerald-500/30 text-emerald-400 bg-emerald-950/20 shrink-0">L{script.layer}</span>
                                                        <span className="text-xs font-mono font-bold text-[var(--text-primary)] truncate">{script.label}</span>
                                                    </div>
                                                    <div className="flex items-center gap-2 shrink-0">
                                                        {/* Status badge */}
                                                        {isRunning && <span className="flex items-center gap-1 text-[9px] font-mono text-yellow-400"><RefreshCw size={9} className="animate-spin" />RUNNING</span>}
                                                        {!isRunning && script.status === "success" && <span className="flex items-center gap-1 text-[9px] font-mono text-green-400"><CheckCircle size={9} />OK</span>}
                                                        {!isRunning && script.status === "error" && <span className="flex items-center gap-1 text-[9px] font-mono text-red-400"><AlertCircle size={9} />ERROR</span>}
                                                        {!isRunning && script.status === "idle" && <span className="text-[9px] font-mono text-[var(--text-muted)]">IDLE</span>}
                                                        {/* Run button */}
                                                        <button
                                                            onClick={() => triggerSync(script.id)}
                                                            disabled={isRunning || !adminKey}
                                                            title={!adminKey ? "Admin key required" : `Run ${script.id}`}
                                                            className="px-2 py-1 rounded border border-emerald-500/40 text-emerald-400 bg-emerald-950/20 hover:bg-emerald-500/20 transition-colors text-[9px] font-mono flex items-center gap-1 disabled:opacity-30 disabled:cursor-not-allowed"
                                                        >
                                                            <RefreshCw size={9} className={isRunning ? "animate-spin" : ""} />
                                                            RUN
                                                        </button>
                                                    </div>
                                                </div>
                                                {/* Description */}
                                                <p className="text-[10px] text-[var(--text-muted)] font-mono leading-relaxed mb-2">{script.description}</p>
                                                {/* Last run */}
                                                {relTime && (
                                                    <div className="flex items-center gap-1.5 mb-2 text-[9px] font-mono text-[var(--text-muted)]">
                                                        <Clock size={9} />
                                                        <span>{relTime}</span>
                                                        {script.duration_s != null && <span className="text-[var(--text-muted)]/60">· {script.duration_s}s</span>}
                                                    </div>
                                                )}
                                                {/* Coverage bars */}
                                                {coverageEntries.length > 0 && (
                                                    <div className="space-y-1">
                                                        {coverageEntries.map(([field, pct]) => (
                                                            <div key={field} className="flex items-center gap-2">
                                                                <span className="text-[9px] font-mono text-[var(--text-muted)] w-36 shrink-0 truncate">{field}</span>
                                                                <div className="flex-1 h-1 rounded-full bg-[var(--bg-secondary)]">
                                                                    <div
                                                                        className="h-1 rounded-full transition-all duration-500"
                                                                        style={{
                                                                            width: `${pct}%`,
                                                                            backgroundColor: pct > 50 ? "#34d399" : pct > 5 ? "#fbbf24" : "#6b7280",
                                                                        }}
                                                                    />
                                                                </div>
                                                                <span className="text-[9px] font-mono text-[var(--text-muted)] w-10 text-right shrink-0">{pct}%</span>
                                                            </div>
                                                        ))}
                                                    </div>
                                                )}
                                                {/* Error message */}
                                                {script.status === "error" && script.error && (
                                                    <p className="mt-2 text-[9px] font-mono text-red-400/80 bg-red-950/20 rounded px-2 py-1 border border-red-900/30">{script.error}</p>
                                                )}
                                            </div>
                                        );
                                    })}
                                    {syncScripts.length === 0 && (
                                        <div className="text-center py-8 text-[10px] font-mono text-[var(--text-muted)]">Loading pipeline status...</div>
                                    )}
                                </div>
                                <div className="p-4 border-t border-[var(--border-primary)]/80">
                                    <div className="flex items-center justify-between text-[9px] text-[var(--text-muted)] font-mono">
                                        <span>{syncScripts.length} PIPELINE STAGES</span>
                                        <span>{syncScripts.filter(s => s.status === "success").length} COMPLETED</span>
                                    </div>
                                </div>
                            </>
                        )}

                    </motion.div>
                </>
            )}
        </AnimatePresence>
    );
});

export default SettingsPanel;
