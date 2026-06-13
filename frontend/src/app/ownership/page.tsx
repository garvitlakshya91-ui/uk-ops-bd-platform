'use client';

import React, { useState, useEffect, useMemo } from 'react';
import Link from 'next/link';
import {
  ShareIcon,
  BuildingLibraryIcon,
  CubeTransparentIcon,
  BanknotesIcon,
  ChevronDownIcon,
  ChevronUpIcon,
} from '@heroicons/react/24/outline';
import api from '@/lib/api';
import { cn } from '@/lib/utils';
import Card from '@/components/ui/Card';
import Select from '@/components/ui/Select';
import {
  OwnershipStats,
  OwnershipTarget,
  OwnershipTargetsResponse,
  ownerTypeTone,
  OWNER_TYPE_BADGE,
  OWNER_TYPE_BAR,
} from './types';

// ---------------------------------------------------------------------------
// Inline scheme list for an expanded target row
// ---------------------------------------------------------------------------

interface TargetScheme {
  id: number;
  name: string;
  council: string | null;
  scheme_type: string | null;
  units: number | null;
  arrears: number | null;
  vehicle: string | null;
  postcode: string | null;
}

function TargetSchemes({ target }: { target: string }) {
  const [schemes, setSchemes] = useState<TargetScheme[] | null>(null);
  const [error, setError] = useState(false);
  const [showAll, setShowAll] = useState(false);

  useEffect(() => {
    let cancelled = false;
    api
      .get('/v2/ownership/targets/schemes', { params: { target } })
      .then((r) => !cancelled && setSchemes(r.data.schemes ?? []))
      .catch(() => !cancelled && setError(true));
    return () => {
      cancelled = true;
    };
  }, [target]);

  if (error)
    return <div className="text-xs text-red-400">Failed to load schemes.</div>;
  if (schemes === null)
    return <div className="text-xs text-slate-500">Loading schemes…</div>;
  if (!schemes.length)
    return <div className="text-xs text-slate-500">No schemes linked.</div>;

  const visible = showAll ? schemes : schemes.slice(0, 8);
  return (
    <div>
      <div className="text-xs text-slate-500 uppercase tracking-wide mb-2">
        Schemes ({schemes.length})
      </div>
      <table className="w-full text-xs">
        <tbody>
          {visible.map((s) => (
            <tr key={s.id} className="border-t border-slate-700/50">
              <td className="py-1.5 pr-3">
                <Link
                  href={`/schemes?search=${encodeURIComponent(s.name)}`}
                  className="text-violet-400 hover:text-violet-300"
                  onClick={(e) => e.stopPropagation()}
                >
                  {s.name}
                </Link>
              </td>
              <td className="py-1.5 pr-3 text-slate-400">{s.council ?? '—'}</td>
              <td className="py-1.5 pr-3 text-slate-400">{s.scheme_type ?? '—'}</td>
              <td className="py-1.5 pr-3 text-slate-300 text-right">
                {s.units ? `${s.units.toLocaleString()} units` : '—'}
              </td>
              <td className="py-1.5 pr-3 text-slate-500">{s.vehicle ?? ''}</td>
              <td className="py-1.5 text-right">
                {s.arrears != null && s.arrears >= 35 ? (
                  <span
                    className={cn(
                      'inline-flex px-1.5 py-0.5 rounded text-[10px] font-semibold',
                      s.arrears >= 60
                        ? 'bg-red-500/15 text-red-400'
                        : 'bg-amber-500/15 text-amber-400'
                    )}
                  >
                    {Math.round(s.arrears)}
                  </span>
                ) : null}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {schemes.length > 8 && (
        <button
          className="mt-2 text-xs text-violet-400 hover:text-violet-300"
          onClick={(e) => {
            e.stopPropagation();
            setShowAll(!showAll);
          }}
        >
          {showAll ? 'Show fewer' : `Show all ${schemes.length} →`}
        </button>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function ownerTypeBadge(type?: string | null) {
  if (!type)
    return (
      <span className="inline-flex items-center px-2 py-0.5 rounded-md text-[11px] font-semibold border bg-slate-700/50 text-slate-400 border-slate-600">
        Unknown
      </span>
    );
  return (
    <span
      className={cn(
        'inline-flex items-center px-2 py-0.5 rounded-md text-[11px] font-semibold border',
        OWNER_TYPE_BADGE[ownerTypeTone(type)],
      )}
    >
      {type}
    </span>
  );
}

function arrearsBadge(score: number | null) {
  if (score === null || score === undefined) return <span className="text-slate-500">—</span>;
  const cls =
    score >= 60
      ? 'bg-red-500/20 text-red-300 border-red-500/40'
      : score >= 35
      ? 'bg-amber-500/15 text-amber-300 border-amber-500/30'
      : 'bg-slate-700/50 text-slate-400 border-slate-600';
  return (
    <span
      className={cn(
        'inline-flex items-center justify-center min-w-[2.25rem] px-1.5 py-0.5 rounded-md text-xs font-semibold border tabular-nums',
        cls,
      )}
      title="Max operator arrears risk score across this owner's schemes"
    >
      {score.toFixed(0)}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function OwnershipPage() {
  const [stats, setStats] = useState<OwnershipStats | null>(null);
  const [targets, setTargets] = useState<OwnershipTarget[]>([]);
  const [statsLoading, setStatsLoading] = useState(true);
  const [targetsLoading, setTargetsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [ownerType, setOwnerType] = useState('');
  const [minSchemes, setMinSchemes] = useState('1');
  const [expandedTarget, setExpandedTarget] = useState<string | null>(null);

  useEffect(() => {
    setStatsLoading(true);
    api.get('/v2/ownership/stats')
      .then((res) => setStats(res.data))
      .catch((err) => setError(err?.response?.data?.detail || 'Failed to load ownership stats'))
      .finally(() => setStatsLoading(false));
  }, []);

  useEffect(() => {
    setTargetsLoading(true);
    const params: Record<string, string | number> = {
      min_schemes: Math.max(1, parseInt(minSchemes, 10) || 1),
      limit: 200,
    };
    if (ownerType) params.owner_type = ownerType;
    api.get<OwnershipTargetsResponse>('/v2/ownership/targets', { params })
      .then((res) => setTargets(res.data.targets))
      .catch((err) => setError(err?.response?.data?.detail || 'Failed to load ownership targets'))
      .finally(() => setTargetsLoading(false));
  }, [ownerType, minSchemes]);

  const peFundCompanies = useMemo(() => {
    if (!stats) return 0;
    return stats.by_type
      .filter((t) => ownerTypeTone(t.type) === 'red')
      .reduce((sum, t) => sum + t.companies, 0);
  }, [stats]);

  const typeTotal = useMemo(() => {
    if (!stats) return 0;
    return stats.by_type.reduce((sum, t) => sum + t.companies, 0);
  }, [stats]);

  const ownerTypeOptions = useMemo(() => {
    if (!stats) return [];
    return stats.by_type.map((t) => ({ value: t.type, label: t.type }));
  }, [stats]);

  if (statsLoading)
    return (
      <div className="p-8">
        <div className="animate-pulse space-y-4">
          <div className="h-8 w-64 bg-slate-800 rounded" />
          <div className="grid grid-cols-4 gap-3">
            {[1, 2, 3, 4].map((i) => (
              <div key={i} className="h-24 bg-slate-800 rounded-lg" />
            ))}
          </div>
        </div>
      </div>
    );

  if (error && !stats)
    return (
      <div className="p-8">
        <div className="bg-red-500/10 border border-red-500/30 rounded-lg p-6 text-red-300">{error}</div>
      </div>
    );

  if (!stats) return null;

  return (
    <div className="p-6 space-y-6 max-w-[1600px] mx-auto">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-semibold text-white flex items-center gap-2">
            <ShareIcon className="w-7 h-7 text-violet-400" />
            Ownership Intelligence
          </h1>
          <p className="mt-1 text-sm text-slate-400">
            Walked Companies House PSC chains: scheme → owner SPV → asset-management platform →
            ultimate owner / PE fund. Pitch the platform, not the SPV.
          </p>
        </div>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-4 gap-3">
        <KpiCard
          label="Companies Walked"
          value={stats.companies_walked}
          tone="blue"
          icon={<BuildingLibraryIcon className="w-5 h-5" />}
          sub="Owner companies with PSC chains resolved"
        />
        <KpiCard
          label="SPV Candidates"
          value={stats.spv_candidates}
          tone="amber"
          icon={<CubeTransparentIcon className="w-5 h-5" />}
          sub="Single-asset vehicles masking the real owner"
        />
        <KpiCard
          label="Platform Clusters"
          value={stats.platform_clusters}
          tone="violet"
          icon={<ShareIcon className="w-5 h-5" />}
          sub="Registered-office clusters of ≥3 companies"
        />
        <KpiCard
          label="PE / Fund Owners"
          value={peFundCompanies}
          tone="red"
          icon={<BanknotesIcon className="w-5 h-5" />}
          sub="Companies ultimately held by PE / institutional capital"
        />
      </div>

      {/* Ultimate-owner type distribution */}
      <Card className="p-5">
        <h2 className="text-sm font-semibold text-white">Ultimate Owner Types</h2>
        <p className="text-xs text-slate-500 mt-0.5 mb-4">
          {typeTotal.toLocaleString()} classified owner companies. Click a chip to filter the target list.
        </p>
        {/* Stacked bar */}
        <div className="h-3 flex rounded-full overflow-hidden bg-slate-800">
          {stats.by_type.map((t) => (
            <div
              key={t.type}
              className={cn('h-full', OWNER_TYPE_BAR[ownerTypeTone(t.type)])}
              style={{ width: `${typeTotal ? (100 * t.companies) / typeTotal : 0}%` }}
              title={`${t.type}: ${t.companies} companies / ${t.schemes} schemes`}
            />
          ))}
        </div>
        {/* Chip row */}
        <div className="mt-3 flex flex-wrap gap-2">
          {stats.by_type.map((t) => {
            const tone = ownerTypeTone(t.type);
            const active = ownerType === t.type;
            return (
              <button
                key={t.type}
                onClick={() => setOwnerType(active ? '' : t.type)}
                className={cn(
                  'inline-flex items-center gap-2 px-3 py-1.5 rounded-md text-xs font-medium border transition-colors',
                  active
                    ? OWNER_TYPE_BADGE[tone]
                    : 'bg-slate-800 text-slate-400 border-slate-700 hover:text-white',
                )}
              >
                <span className={cn('w-2 h-2 rounded-full', OWNER_TYPE_BAR[tone])} />
                {t.type}
                <span className="tabular-nums text-slate-500">
                  {t.companies} co · {t.schemes} sch
                </span>
              </button>
            );
          })}
        </div>
      </Card>

      {/* Target list */}
      <Card className="overflow-hidden">
        <div className="px-5 py-4 border-b border-slate-700 flex items-center justify-between flex-wrap gap-3">
          <div>
            <h2 className="text-sm font-semibold text-white">Target List — by ultimate owner</h2>
            <p className="text-xs text-slate-500 mt-0.5">
              {targetsLoading
                ? 'Loading targets...'
                : `${targets.length} target${targets.length === 1 ? '' : 's'}, sorted by portfolio size. Click a row to see vehicles.`}
            </p>
          </div>
          <div className="flex items-center gap-2">
            <Select
              options={ownerTypeOptions}
              value={ownerType}
              onChange={setOwnerType}
              placeholder="All owner types"
              className="w-56"
            />
            <div className="flex items-center gap-1.5">
              <label className="text-xs text-slate-500 whitespace-nowrap">Min schemes</label>
              <input
                type="number"
                min={1}
                value={minSchemes}
                onChange={(e) => setMinSchemes(e.target.value)}
                className="w-16 px-2 py-2 bg-slate-700 border border-slate-600 rounded-lg text-sm text-slate-200 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent tabular-nums"
              />
            </div>
          </div>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead>
              <tr className="text-xs text-slate-500 uppercase tracking-wide bg-slate-800/40">
                <th className="w-8 px-4 py-2" />
                <th className="text-left px-3 py-2 font-medium">Target</th>
                <th className="text-left px-3 py-2 font-medium">Type</th>
                <th className="text-right px-3 py-2 font-medium">Vehicles</th>
                <th className="text-right px-3 py-2 font-medium">SPVs</th>
                <th className="text-right px-3 py-2 font-medium">Schemes</th>
                <th className="text-right px-3 py-2 font-medium">Units</th>
                <th className="text-left px-3 py-2 font-medium">Councils</th>
                <th className="text-right px-3 py-2 font-medium">Max arrears</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800">
              {!targetsLoading && targets.length === 0 && (
                <tr>
                  <td colSpan={9} className="text-center py-6 text-slate-500">
                    No targets match this filter.
                  </td>
                </tr>
              )}
              {targets.map((t) => {
                const expanded = expandedTarget === t.target;
                return (
                  <React.Fragment key={t.target}>
                    <tr
                      className="hover:bg-slate-800/40 transition-colors cursor-pointer"
                      onClick={() => setExpandedTarget(expanded ? null : t.target)}
                      title={t.vehicle_names.join(' · ')}
                    >
                      <td className="px-4 py-2.5">
                        {expanded ? (
                          <ChevronUpIcon className="w-4 h-4 text-slate-400" />
                        ) : (
                          <ChevronDownIcon className="w-4 h-4 text-slate-400" />
                        )}
                      </td>
                      <td className="px-3 py-2.5">
                        <span className="text-white font-medium">{t.target}</span>
                      </td>
                      <td className="px-3 py-2.5">{ownerTypeBadge(t.owner_type)}</td>
                      <td className="text-right px-3 py-2.5 tabular-nums text-slate-300">{t.vehicles}</td>
                      <td className="text-right px-3 py-2.5 tabular-nums">
                        {t.spv_count > 0 ? (
                          <span className="text-amber-300 font-semibold">{t.spv_count}</span>
                        ) : (
                          <span className="text-slate-500">—</span>
                        )}
                      </td>
                      <td className="text-right px-3 py-2.5 tabular-nums text-white font-semibold">{t.schemes}</td>
                      <td className="text-right px-3 py-2.5 tabular-nums text-slate-300">
                        {t.units > 0 ? t.units.toLocaleString() : '—'}
                      </td>
                      <td className="px-3 py-2.5 text-xs text-slate-400">
                        {t.councils.slice(0, 3).join(', ')}
                        {t.councils.length > 3 && (
                          <span className="ml-1 text-slate-500">+{t.councils.length - 3}</span>
                        )}
                      </td>
                      <td className="text-right px-3 py-2.5">{arrearsBadge(t.max_arrears)}</td>
                    </tr>
                    {expanded && (
                      <tr>
                        <td colSpan={9} className="px-8 py-4 bg-slate-800/60">
                          <div className="text-xs text-slate-500 uppercase tracking-wide mb-2">
                            Vehicles ({t.vehicles})
                          </div>
                          <div className="flex flex-wrap gap-2">
                            {t.vehicle_names.map((name) => (
                              <span
                                key={name}
                                className="inline-flex items-center px-2.5 py-1 rounded-md text-xs bg-slate-700/50 text-slate-300 border border-slate-700"
                              >
                                {name}
                              </span>
                            ))}
                            {t.vehicles > t.vehicle_names.length && (
                              <span className="inline-flex items-center px-2.5 py-1 rounded-md text-xs text-slate-500">
                                +{t.vehicles - t.vehicle_names.length} more
                              </span>
                            )}
                          </div>
                          {t.councils.length > 3 && (
                            <div className="mt-3">
                              <div className="text-xs text-slate-500 uppercase tracking-wide mb-1">Councils</div>
                              <div className="text-xs text-slate-400">{t.councils.join(', ')}</div>
                            </div>
                          )}
                          <div className="mt-4">
                            <TargetSchemes target={t.target} />
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function KpiCard({
  label,
  value,
  sub,
  tone,
  icon,
}: {
  label: string;
  value: number;
  sub: string;
  tone: 'blue' | 'amber' | 'violet' | 'red';
  icon: React.ReactNode;
}) {
  const toneClass = {
    blue: 'from-blue-500/10 to-blue-500/[0.02] border-blue-500/30 text-blue-300',
    amber: 'from-amber-500/10 to-amber-500/[0.02] border-amber-500/30 text-amber-300',
    violet: 'from-violet-500/10 to-violet-500/[0.02] border-violet-500/30 text-violet-300',
    red: 'from-red-500/10 to-red-500/[0.02] border-red-500/30 text-red-300',
  }[tone];

  return (
    <div className={cn('rounded-lg p-4 bg-gradient-to-br border', toneClass)}>
      <div className="flex items-center justify-between">
        <span className="text-xs font-medium uppercase tracking-wide text-slate-400">{label}</span>
        <span className="opacity-70">{icon}</span>
      </div>
      <div className="mt-2 text-3xl font-semibold text-white tabular-nums">{value.toLocaleString()}</div>
      <div className="mt-1 text-[11px] text-slate-500">{sub}</div>
    </div>
  );
}
